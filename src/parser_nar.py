"""Parse NAR CSV files into unique postal code records per snapshot.

The NAR data is split into per-province Address CSV files inside each snapshot ZIP.
Column names use MAIL_POSTAL_CODE, PROV_CODE, MAIL_MUN_NAME, etc. (not the names
in the data-spec.md which were approximated).

BG_X/BG_Y are in Statistics Canada Lambert Conformal Conic projection, NOT lat/lon.
We skip these for now — lat/lon can be backfilled from Geocoder.ca or GeoNames.
"""

import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from src import db
from src.config import (
    FSA_FIRST_LETTER_TO_PROVINCE,
    NAR_CHUNK_SIZE,
    NAR_SNAPSHOTS,
    NUNAVUT_FSAS,
    PROCESSED_DIR,
    PROVINCE_CODE_TO_ABBR,
    RAW_NAR_DIR,
)

logger = logging.getLogger(__name__)

# Actual NAR CSV column names → our internal names
NAR_COL_MAP = {
    "MAIL_POSTAL_CODE": "postal_code",
    "PROV_CODE": "province_code",
    "MAIL_MUN_NAME": "city_name",
    "MAIL_PROV_ABVN": "province_abbr_raw",
    "CSD_ENG_NAME": "csd_name",
}

NAR_USECOLS = list(NAR_COL_MAP.keys())


def _find_address_csvs(extract_dir: Path) -> list[Path]:
    """Find all Address CSV files inside an extracted NAR directory.

    NAR splits data into per-province files like:
      Addresses/address_35_part_1.csv  (or Address_35_part_1.csv)
    """
    csvs = []
    for subdir in extract_dir.iterdir():
        if subdir.is_dir() and subdir.name.lower() == "addresses":
            csvs = sorted(subdir.glob("*.csv"))
            break
    if not csvs:
        # Fallback: look for any CSV with "address" in the name
        csvs = sorted(extract_dir.rglob("*ddress*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No Address CSVs found in {extract_dir}")
    return csvs


def _normalize_province(province_code: str, province_abbr: str, postal_code: str) -> str:
    """Derive the best 2-letter province abbreviation (scalar version)."""
    abbr = str(province_abbr).strip().upper() if pd.notna(province_abbr) else ""
    if len(abbr) != 2:
        abbr = PROVINCE_CODE_TO_ABBR.get(str(province_code).strip(), "")
    if not abbr and pd.notna(postal_code) and len(str(postal_code)) >= 1:
        abbr = FSA_FIRST_LETTER_TO_PROVINCE.get(str(postal_code)[0].upper(), "")
    if abbr == "NT" and pd.notna(postal_code) and len(str(postal_code)) >= 3:
        if str(postal_code)[:3].upper() in NUNAVUT_FSAS:
            abbr = "NU"
    return abbr


def _normalize_province_vectorized(df: pd.DataFrame) -> pd.Series:
    """Vectorized province normalization. Operates on the full DataFrame at once."""
    # Stage 1: Try province_abbr_raw (strip + uppercase, keep only 2-char values)
    raw = df["province_abbr_raw"].fillna("").str.strip().str.upper()
    abbr = raw.where(raw.str.len() == 2, other="")

    # Stage 2: Where abbr is empty, fall back to numeric province_code mapping
    code_mapped = df["province_code"].fillna("").str.strip().map(PROVINCE_CODE_TO_ABBR).fillna("")
    mask_empty = abbr == ""
    abbr = abbr.where(~mask_empty, other=code_mapped)

    # Stage 3: Where still empty, fall back to first letter of postal_code
    first_letter = df["postal_code"].str[0].str.upper()
    letter_mapped = first_letter.map(FSA_FIRST_LETTER_TO_PROVINCE).fillna("")
    mask_still_empty = abbr == ""
    abbr = abbr.where(~mask_still_empty, other=letter_mapped)

    # Stage 4: Nunavut disambiguation -- where abbr=="NT" and FSA is in NUNAVUT_FSAS
    fsa = df["postal_code"].str[:3].str.upper()
    is_nunavut = (abbr == "NT") & fsa.isin(NUNAVUT_FSAS)
    abbr = abbr.where(~is_nunavut, other="NU")

    return abbr


def _parse_single_csv(csv_path: Path, chunk_size: int) -> tuple[list[pd.DataFrame], int]:
    """Parse a single NAR CSV file into aggregated chunks.

    Must be a module-level function (picklable for ProcessPoolExecutor on Windows).
    Returns (list_of_aggregated_dataframes, total_row_count).
    """
    aggregated = []
    row_count = 0

    for chunk in pd.read_csv(
        csv_path,
        usecols=NAR_USECOLS,
        dtype=str,
        chunksize=chunk_size,
        encoding="latin-1",
        encoding_errors="replace",
        on_bad_lines="skip",
    ):
        row_count += len(chunk)
        chunk = chunk.rename(columns=NAR_COL_MAP)

        # Clean postal code: strip spaces, uppercase
        chunk["postal_code"] = (
            chunk["postal_code"]
            .str.replace(" ", "", regex=False)
            .str.upper()
            .str.strip()
        )
        chunk = chunk.dropna(subset=["postal_code"])
        chunk = chunk[chunk["postal_code"].str.len() == 6]

        if chunk.empty:
            continue

        grouped = (
            chunk.groupby("postal_code", sort=False)
            .agg(
                province_code=("province_code", "first"),
                province_abbr_raw=("province_abbr_raw", "first"),
                city_name=("city_name", "first"),
                csd_name=("csd_name", "first"),
                address_count=("postal_code", "size"),
            )
            .reset_index()
        )
        aggregated.append(grouped)

    return aggregated, row_count


def parse_nar_snapshot(period: str) -> pd.DataFrame:
    """Parse all NAR Address CSVs for a snapshot into unique postal codes.

    Uses ProcessPoolExecutor to parse CSV files in parallel.
    Returns a DataFrame with one row per unique postal code.
    """
    info = NAR_SNAPSHOTS.get(period)
    if not info:
        raise ValueError(f"Unknown NAR period: {period!r}")

    extract_dir = RAW_NAR_DIR / period
    if not extract_dir.exists():
        raise FileNotFoundError(
            f"NAR data not downloaded for {period}. Run 'download' first."
        )

    csv_files = _find_address_csvs(extract_dir)
    logger.info("Parsing NAR %s: %d CSV files", period, len(csv_files))

    all_aggregated = []
    total_rows = 0

    # Parse CSV files in parallel across CPU cores
    max_workers = min(len(csv_files), os.cpu_count() or 4)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_parse_single_csv, csv_path, NAR_CHUNK_SIZE): csv_path
            for csv_path in csv_files
        }
        for future in as_completed(futures):
            csv_path = futures[future]
            aggregated, row_count = future.result()
            logger.info("  Completed %s (%d rows)", csv_path.name, row_count)
            all_aggregated.extend(aggregated)
            total_rows += row_count

    if not all_aggregated:
        raise ValueError(f"No data found in NAR CSVs for {period}")

    # Combine all chunks across all files and re-aggregate
    combined = pd.concat(all_aggregated, ignore_index=True)

    final = (
        combined.groupby("postal_code", sort=False)
        .agg(
            province_code=("province_code", "first"),
            province_abbr_raw=("province_abbr_raw", "first"),
            city_name=("city_name", "first"),
            csd_name=("csd_name", "first"),
            address_count=("address_count", "sum"),
        )
        .reset_index()
    )

    # Normalize province to 2-letter abbreviation (vectorized)
    final["province_abbr"] = _normalize_province_vectorized(final)

    # Normalize city names to title case
    final["city_name"] = final["city_name"].str.strip().str.title()

    # Use CSD name as csd_code (the NAR doesn't have numeric CSD codes)
    final = final.rename(columns={"csd_name": "csd_code"})

    # No lat/lon available from NAR (BG_X/BG_Y are in Lambert projection)
    final["latitude"] = None
    final["longitude"] = None

    # Drop intermediate columns
    final = final.drop(columns=["province_code", "province_abbr_raw"])

    logger.info(
        "NAR %s: %d total rows -> %d unique postal codes",
        period,
        total_rows,
        len(final),
    )

    return final


def _store_nar_snapshot(period: str, df: pd.DataFrame) -> int:
    """Store a pre-parsed NAR DataFrame into the database. Returns unique PC count."""
    info = NAR_SNAPSHOTS[period]
    snapshot_date = info["reference_date"]

    # Save processed parquet
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = PROCESSED_DIR / f"nar_{period}_unique.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info("Saved %s", parquet_path)

    # Insert into database
    db.init_db()
    conn = db.get_connection()

    # Delete existing snapshot data for this period (in case of reprocess)
    conn.execute(
        "DELETE FROM postal_code_snapshots WHERE snapshot_date = ? AND source_type = 'nar'",
        (snapshot_date,),
    )

    # Prepare for insert
    df = df.copy()
    df["snapshot_date"] = snapshot_date
    df["source_type"] = "nar"

    insert_cols = [
        "postal_code",
        "snapshot_date",
        "source_type",
        "province_abbr",
        "city_name",
        "latitude",
        "longitude",
        "csd_code",
        "address_count",
    ]
    df[insert_cols].to_sql(
        "postal_code_snapshots",
        conn,
        if_exists="append",
        index=False,
        chunksize=5000,
    )
    conn.commit()
    conn.close()

    # Record processing
    total_rows = int(df["address_count"].sum())
    db.mark_processed("nar", snapshot_date, total_rows, len(df))

    logger.info("Loaded %d postal codes for NAR %s into database", len(df), period)
    return len(df)


def process_nar_snapshot(period: str, force: bool = False) -> int:
    """Parse, save to parquet, and load into database. Returns unique PC count."""
    info = NAR_SNAPSHOTS[period]
    snapshot_date = info["reference_date"]

    # Check if already processed
    if not force:
        conn = db.get_connection()
        row = conn.execute(
            """
            SELECT processed_at FROM data_sources
            WHERE source_type = 'nar' AND reference_date = ?
            """,
            (snapshot_date,),
        ).fetchone()
        conn.close()
        if row and row["processed_at"]:
            logger.info("NAR %s already processed, skipping (use --force to reprocess)", period)
            return 0

    df = parse_nar_snapshot(period)
    return _store_nar_snapshot(period, df)


def process_all_nar(force: bool = False) -> dict[str, int]:
    """Process all downloaded NAR snapshots in parallel. Returns {period: count}."""
    # Phase 1: Determine which periods need processing
    periods_to_process = []
    for period in NAR_SNAPSHOTS:
        extract_dir = RAW_NAR_DIR / period
        if not extract_dir.exists():
            logger.warning("NAR %s not downloaded, skipping", period)
            continue

        if not force:
            info = NAR_SNAPSHOTS[period]
            snapshot_date = info["reference_date"]
            conn = db.get_connection()
            row = conn.execute(
                "SELECT processed_at FROM data_sources WHERE source_type = 'nar' AND reference_date = ?",
                (snapshot_date,),
            ).fetchone()
            conn.close()
            if row and row["processed_at"]:
                logger.info("NAR %s already processed, skipping", period)
                continue

        periods_to_process.append(period)

    if not periods_to_process:
        return {}

    # Phase 2: Parse all snapshots in parallel (CPU-bound, no DB access)
    parsed_results = {}
    max_workers = min(len(periods_to_process), os.cpu_count() or 4)
    logger.info("Parsing %d snapshots with %d workers", len(periods_to_process), max_workers)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(parse_nar_snapshot, period): period
            for period in periods_to_process
        }
        for future in as_completed(futures):
            period = futures[future]
            try:
                parsed_results[period] = future.result()
                logger.info("Parsed NAR %s: %d unique postal codes", period, len(parsed_results[period]))
            except Exception:
                logger.exception("Failed to parse NAR %s", period)

    # Phase 3: Write to DB sequentially (SQLite single-writer constraint)
    results = {}
    for period in periods_to_process:
        if period not in parsed_results:
            continue
        count = _store_nar_snapshot(period, parsed_results[period])
        results[period] = count

    return results
