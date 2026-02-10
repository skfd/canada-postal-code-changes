"""Parse NAR CSV files into unique postal code records per snapshot.

The NAR data is split into per-province Address CSV files inside each snapshot ZIP.
Column names use MAIL_POSTAL_CODE, PROV_CODE, MAIL_MUN_NAME, etc. (not the names
in the data-spec.md which were approximated).

BG_X/BG_Y are in Statistics Canada Lambert Conformal Conic projection, NOT lat/lon.
We skip these for now — lat/lon can be backfilled from Geocoder.ca or GeoNames.
"""

import logging
from pathlib import Path

import pandas as pd

from src import db
from src.config import (
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
    """Derive the best 2-letter province abbreviation.

    Uses MAIL_PROV_ABVN first, falls back to PROV_CODE numeric mapping.
    Handles Nunavut/NWT disambiguation for postal codes starting with X.
    """
    # Prefer the direct abbreviation from the CSV
    abbr = str(province_abbr).strip().upper() if pd.notna(province_abbr) else ""
    if len(abbr) != 2:
        # Fall back to numeric code
        abbr = PROVINCE_CODE_TO_ABBR.get(str(province_code).strip(), "")
    if not abbr and pd.notna(postal_code) and len(str(postal_code)) >= 1:
        from src.config import FSA_FIRST_LETTER_TO_PROVINCE
        abbr = FSA_FIRST_LETTER_TO_PROVINCE.get(str(postal_code)[0].upper(), "")
    # Disambiguate X → NU vs NT
    if abbr == "NT" and pd.notna(postal_code) and len(str(postal_code)) >= 3:
        if str(postal_code)[:3].upper() in NUNAVUT_FSAS:
            abbr = "NU"
    return abbr


def parse_nar_snapshot(period: str) -> pd.DataFrame:
    """Parse all NAR Address CSVs for a snapshot into unique postal codes.

    Reads each per-province CSV in chunks, aggregates, then combines.
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

    for csv_path in csv_files:
        logger.info("  Reading %s ...", csv_path.name)

        for chunk in pd.read_csv(
            csv_path,
            usecols=NAR_USECOLS,
            dtype=str,
            chunksize=NAR_CHUNK_SIZE,
            encoding="latin-1",
            encoding_errors="replace",
            on_bad_lines="skip",
        ):
            total_rows += len(chunk)

            # Rename columns
            chunk = chunk.rename(columns=NAR_COL_MAP)

            # Clean postal code: strip spaces, uppercase
            chunk["postal_code"] = (
                chunk["postal_code"]
                .str.replace(" ", "", regex=False)
                .str.upper()
                .str.strip()
            )
            # Drop rows with missing/invalid postal codes
            chunk = chunk.dropna(subset=["postal_code"])
            chunk = chunk[chunk["postal_code"].str.len() == 6]

            if chunk.empty:
                continue

            # Group by postal code within this chunk
            grouped = (
                chunk.groupby("postal_code", sort=False)
                .agg(
                    province_code=("province_code", "first"),
                    province_abbr_raw=("province_abbr_raw", "first"),
                    city_name=("city_name", lambda x: x.mode().iloc[0] if len(x) > 0 else None),
                    csd_name=("csd_name", "first"),
                    address_count=("postal_code", "size"),
                )
                .reset_index()
            )
            all_aggregated.append(grouped)

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

    # Normalize province to 2-letter abbreviation
    final["province_abbr"] = final.apply(
        lambda row: _normalize_province(
            row["province_code"], row["province_abbr_raw"], row["postal_code"]
        ),
        axis=1,
    )

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
        "NAR %s: %d total rows → %d unique postal codes",
        period,
        total_rows,
        len(final),
    )

    return final


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

    # Parse
    df = parse_nar_snapshot(period)

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
    df["snapshot_date"] = snapshot_date
    df["source_type"] = "nar"

    # Use pandas to_sql for bulk insert
    # Note: method="multi" can exceed SQLite's variable limit, so we use
    # the default row-by-row method with a reasonable chunksize
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


def process_all_nar(force: bool = False) -> dict[str, int]:
    """Process all downloaded NAR snapshots. Returns {period: count}."""
    results = {}
    for period in NAR_SNAPSHOTS:
        extract_dir = RAW_NAR_DIR / period
        if extract_dir.exists():
            count = process_nar_snapshot(period, force=force)
            results[period] = count
        else:
            logger.warning("NAR %s not downloaded, skipping", period)
    return results
