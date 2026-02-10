"""Parse Geocoder.ca CSV files."""

import logging
from pathlib import Path

import pandas as pd

from src import db
from src.config import NUNAVUT_FSAS, RAW_GEOCODER_DIR

logger = logging.getLogger(__name__)


def find_latest_geocoder_csv() -> Path | None:
    """Find the most recent Geocoder.ca CSV in the raw directory."""
    RAW_GEOCODER_DIR.mkdir(parents=True, exist_ok=True)
    csvs = sorted(RAW_GEOCODER_DIR.glob("*.csv"))
    return csvs[-1] if csvs else None


def parse_geocoder_csv(csv_path: Path) -> pd.DataFrame:
    """Parse a Geocoder.ca CSV into a postal code DataFrame.

    Expected columns: PostCode, Latitude, Longitude, City, Province, ...
    """
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8", on_bad_lines="skip")

    # Normalize column names (Geocoder.ca may vary)
    col_map = {}
    for col in df.columns:
        lower = col.strip().lower().replace(" ", "")
        if lower in ("postcode", "postalcode", "postal_code"):
            col_map[col] = "postal_code"
        elif lower == "latitude":
            col_map[col] = "latitude"
        elif lower == "longitude":
            col_map[col] = "longitude"
        elif lower == "city":
            col_map[col] = "city_name"
        elif lower == "province":
            col_map[col] = "province_abbr"
    df = df.rename(columns=col_map)

    required = {"postal_code", "latitude", "longitude"}
    if not required.issubset(df.columns):
        raise ValueError(
            f"Missing columns in Geocoder.ca CSV. Found: {list(df.columns)}"
        )

    # Clean postal code
    df["postal_code"] = (
        df["postal_code"].str.replace(" ", "", regex=False).str.upper()
    )
    df = df.dropna(subset=["postal_code"])

    # Convert lat/lon to float
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Province — Geocoder uses 2-letter codes already
    if "province_abbr" in df.columns:
        df["province_abbr"] = df["province_abbr"].str.strip().str.upper()
    else:
        df["province_abbr"] = None

    # Disambiguate NU vs NT for X codes
    mask_x = df["postal_code"].str[0] == "X"
    mask_nu = df["postal_code"].str[:3].isin(NUNAVUT_FSAS)
    df.loc[mask_x & mask_nu, "province_abbr"] = "NU"
    df.loc[mask_x & ~mask_nu, "province_abbr"] = "NT"

    if "city_name" in df.columns:
        df["city_name"] = df["city_name"].str.strip().str.title()
    else:
        df["city_name"] = None

    # Keep only one row per postal code
    df = df.drop_duplicates(subset=["postal_code"], keep="first")

    # Set standard columns
    df["csd_code"] = None
    df["address_count"] = 1

    output_cols = [
        "postal_code", "province_abbr", "city_name",
        "latitude", "longitude", "csd_code", "address_count",
    ]
    return df[output_cols].reset_index(drop=True)


def process_geocoder(csv_path: Path | None = None, force: bool = False) -> int:
    """Parse and load Geocoder.ca data into the database."""
    if csv_path is None:
        csv_path = find_latest_geocoder_csv()
    if csv_path is None or not csv_path.exists():
        logger.warning("No Geocoder.ca CSV found in %s", RAW_GEOCODER_DIR)
        return 0

    # Derive snapshot date from filename (YYYY-MM-DD.csv)
    snapshot_date = csv_path.stem  # e.g., "2026-02-01"

    df = parse_geocoder_csv(csv_path)
    logger.info("Geocoder.ca: %d unique postal codes from %s", len(df), csv_path.name)

    # Store in database
    db.init_db()
    conn = db.get_connection()
    conn.execute(
        "DELETE FROM postal_code_snapshots WHERE snapshot_date = ? AND source_type = 'geocoder'",
        (snapshot_date,),
    )

    df["snapshot_date"] = snapshot_date
    df["source_type"] = "geocoder"

    insert_cols = [
        "postal_code", "snapshot_date", "source_type", "province_abbr",
        "city_name", "latitude", "longitude", "csd_code", "address_count",
    ]
    df[insert_cols].to_sql(
        "postal_code_snapshots", conn, if_exists="append",
        index=False, chunksize=5000,
    )
    conn.commit()
    conn.close()

    db.mark_processed("geocoder", snapshot_date, len(df), len(df))
    return len(df)
