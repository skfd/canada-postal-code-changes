# How Updated Files Are Found and Processed

This document explains how the system discovers new Statistics Canada NAR (National Address Register) snapshots and processes them through the change detection pipeline.

## Overview

The system tracks Canadian postal codes across time by downloading periodic snapshots from Statistics Canada, parsing the raw address data into unique postal code records, and comparing consecutive snapshots to detect additions, removals, and attribute changes.

The pipeline has four stages: **Download** -> **Process** -> **Diff** -> **Summary**.

## Data Sources

### Primary: Statistics Canada NAR

The NAR is a national address database published by Statistics Canada under an Open Licence. It is released as periodic ZIP archives, each containing CSV files with millions of address records (~15M rows per snapshot, yielding ~893K unique postal codes).

Six snapshots are currently configured in `src/config.py`:

| Period    | Reference Date | Archive Name |
|-----------|---------------|--------------|
| 2022      | 2022-01-01    | 2022.zip     |
| 2023      | 2023-01-01    | 2023.zip     |
| 2024-06   | 2024-06-01    | 2024.zip     |
| 2024-12   | 2024-12-01    | 202412.zip   |
| 2025-07   | 2025-07-01    | 202507.zip   |
| 2025-12   | 2025-12-01    | 202512.zip   |

All ZIPs are hosted at `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/`.

### Secondary: Geocoder.ca and GeoNames

- **Geocoder.ca** provides crowdsourced current postal code data with coordinates. It requires manual download from https://geocoder.ca/?freedata=1.
- **GeoNames** provides a global postal code extract. The Canadian file (`CA_full.csv.zip`) is auto-downloaded from https://download.geonames.org/export/zip/.

## Stage 1: Finding and Downloading Updated Files

### How new snapshots are discovered

The system uses a **configured catalogue** approach rather than scraping. All known NAR snapshot URLs are listed in the `NAR_SNAPSHOTS` dictionary in `src/config.py`. When Statistics Canada publishes a new NAR release, a developer adds its URL, period label, and reference date to this dictionary and to the `NAR_SNAPSHOT_ORDER` list.

The catalogue page where new releases are announced is:
https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm

### How downloads work

When `download` or `refresh` is run, the downloader (`src/downloader.py`) checks each configured snapshot:

1. **Check if ZIP already exists** at `data/raw/nar/{period}.zip`. If it does, skip the download.
2. **Stream-download** the ZIP from Statistics Canada with a progress bar (256KB chunks).
3. **Extract the ZIP** to `data/raw/nar/{period}/` if not already extracted.
4. **Record the download** in the `data_sources` table (source type, reference date, URL, file path, timestamp).

This means running `download` or `refresh` multiple times is safe — it only downloads files that are missing locally.

### CLI commands

```bash
# Download all configured sources
python -m src.cli download

# Download only NAR snapshots
python -m src.cli download --source nar

# Download a specific NAR period
python -m src.cli download --source nar --period 2024-12

# Full pipeline: download + process + diff + summary
python -m src.cli refresh
```

## Stage 2: Parsing Raw Data

Each NAR ZIP contains address-level CSV files under an `Addresses/` directory. These CSVs have columns like `MAIL_POSTAL_CODE`, `PROV_CODE`, `MAIL_MUN_NAME`, and `CSD_ENG_NAME`.

The parser (`src/parser_nar.py`) processes these files:

1. **Parallel parsing**: Each CSV file is parsed in a separate process using `ProcessPoolExecutor`.
2. **Chunked reading**: CSVs are read in 500,000-row chunks to manage memory.
3. **Cleaning**: Postal codes are uppercased, spaces removed, and validated (must be 6 characters).
4. **Province normalization**: A 4-stage process resolves province from raw abbreviation, numeric code, FSA first letter, or Nunavut-specific rules.
5. **Aggregation**: ~15M address rows are deduplicated down to ~893K unique postal codes, with address counts preserved.
6. **Storage**: Results are saved to both a Parquet file (`data/processed/nar_{period}_unique.parquet`) and the `postal_code_snapshots` database table.

The `--force` flag reprocesses snapshots even if they were already loaded.

## Stage 3: Change Detection (Diffing)

The differ (`src/differ.py`) compares consecutive snapshot pairs and detects five types of changes:

| Change Type       | Meaning                                       |
|-------------------|-----------------------------------------------|
| `added`           | Postal code exists in the later snapshot only  |
| `removed`         | Postal code exists in the earlier snapshot only|
| `city_changed`    | Same postal code, different city name          |
| `csd_changed`     | Same postal code, different Census Subdivision |
| `location_shifted`| Centroid moved more than ~1 km                 |

### City change classification

City name changes are further classified into subtypes by `src/classifier.py`:

- **encoding** — Mojibake or character encoding corruption
- **accent_normalization** — Same text after stripping accents (e.g., Montreal vs Montréal)
- **punctuation** — Trailing period added or removed
- **spacing** — Hyphen vs space normalization
- **abbreviation** — St/Saint, Ste/Sainte, Mt/Mount patterns
- **boundary** — Known municipal boundary changes (from rules file)
- **rename** — Known official city renames (from rules file)
- **substantive** — Actual city rename (default)

### Merged snapshots

When multiple data sources are available, the system builds **merged snapshots** that combine postal codes from all sources at each time point. Source priority for field resolution: Geocoder.ca/GeoNames (lat/lon) > NAR (address count, CSD code).

## Stage 4: Summary

After diffing, the `postal_code_summary` table is rebuilt. It contains one row per unique postal code with:

- First and last seen dates
- Whether currently active
- Province, city, coordinates
- Total number of changes across all snapshots
- Which data sources contributed

## Adding a New Snapshot

When Statistics Canada publishes a new NAR release:

1. **Find the new URL** on the [NAR catalogue page](https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm).
2. **Add the snapshot** to `NAR_SNAPSHOTS` in `src/config.py`:
   ```python
   "2026-06": {
       "url": "https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/202606.zip",
       "reference_date": "2026-06-01",
   },
   ```
3. **Add the period** to `NAR_SNAPSHOT_ORDER`.
4. **Run the pipeline**:
   ```bash
   python -m src.cli refresh --source nar
   ```
   This downloads the new ZIP, parses it, diffs it against the previous snapshot, rebuilds merged views, and updates the summary.

## File Layout

```
data/
  raw/
    nar/
      2022.zip          # Downloaded archive
      2022/             # Extracted directory
        Addresses/
          address_*.csv  # Raw address CSVs (~15M rows)
      2023.zip
      2023/
      ...
    geocoder/
      2026-02-11.csv    # Named by download date
    geonames/
      CA_full.csv.zip
      CA_full.txt
  processed/
    nar_2022_unique.parquet   # Deduplicated postal codes
    nar_2023_unique.parquet
    ...
  postal_codes.db             # SQLite database
```
