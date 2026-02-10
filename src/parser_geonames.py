"""Parse GeoNames CA_full postal code data."""

import logging
from pathlib import Path

import pandas as pd

from src import db
from src.config import NUNAVUT_FSAS, RAW_GEONAMES_DIR

logger = logging.getLogger(__name__)

# GeoNames columns (tab-delimited, no header)
GEONAMES_COLUMNS = [
    "country_code",
    "postal_code",
    "place_name",
    "admin_name1",
    "admin_code1",
    "admin_name2",
    "admin_code2",
    "admin_name3",
    "admin_code3",
    "latitude",
    "longitude",
    "accuracy",
]


def find_geonames_file() -> Path | None:
    """Find the GeoNames CA file in the raw directory."""
    RAW_GEONAMES_DIR.mkdir(parents=True, exist_ok=True)
    for pattern in ["CA_full.txt", "CA_full.csv", "CA.txt"]:
        files = list(RAW_GEONAMES_DIR.glob(pattern))
        if files:
            return files[0]
    return None


def parse_geonames(file_path: Path) -> pd.DataFrame:
    """Parse GeoNames tab-delimited file into postal code DataFrame."""
    df = pd.read_csv(
        file_path,
        sep="\t",
        header=None,
        names=GEONAMES_COLUMNS,
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip",
    )

    # Clean postal code: GeoNames includes a space (e.g., "M5V 1J2")
    df["postal_code"] = (
        df["postal_code"].str.replace(" ", "", regex=False).str.upper()
    )
    df = df.dropna(subset=["postal_code"])

    # Convert lat/lon
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Province: admin_code1 is the 2-letter abbreviation
    df["province_abbr"] = df["admin_code1"].str.strip().str.upper()

    # Disambiguate NU vs NT
    mask_x = df["postal_code"].str[0] == "X"
    mask_nu = df["postal_code"].str[:3].isin(NUNAVUT_FSAS)
    df.loc[mask_x & mask_nu, "province_abbr"] = "NU"
    df.loc[mask_x & ~mask_nu, "province_abbr"] = "NT"

    # City name
    df["city_name"] = df["place_name"].str.strip().str.title()

    # Deduplicate
    df = df.drop_duplicates(subset=["postal_code"], keep="first")

    df["csd_code"] = None
    df["address_count"] = 1

    output_cols = [
        "postal_code", "province_abbr", "city_name",
        "latitude", "longitude", "csd_code", "address_count",
    ]
    return df[output_cols].reset_index(drop=True)


def process_geonames(file_path: Path | None = None, force: bool = False) -> int:
    """Parse and load GeoNames data into the database."""
    if file_path is None:
        file_path = find_geonames_file()
    if file_path is None or not file_path.exists():
        logger.warning("No GeoNames file found in %s", RAW_GEONAMES_DIR)
        return 0

    from datetime import datetime

    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    df = parse_geonames(file_path)
    logger.info("GeoNames: %d unique postal codes from %s", len(df), file_path.name)

    db.init_db()
    conn = db.get_connection()
    conn.execute(
        "DELETE FROM postal_code_snapshots WHERE source_type = 'geonames'",
    )

    df["snapshot_date"] = snapshot_date
    df["source_type"] = "geonames"

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

    db.mark_processed("geonames", snapshot_date, len(df), len(df))
    return len(df)
