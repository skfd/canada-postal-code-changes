"""Paths, URLs, province mappings, and other constants."""

from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DB_PATH = DATA_DIR / "postal_codes.db"

RAW_NAR_DIR = RAW_DIR / "nar"
RAW_GEOCODER_DIR = RAW_DIR / "geocoder"
RAW_GEONAMES_DIR = RAW_DIR / "geonames"

# ── NAR download URLs (Statistics Canada Open Licence) ───────────────────────

NAR_CATALOGUE_URL = (
    "https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm"
)

NAR_SNAPSHOTS: dict[str, dict] = {
    "2022": {
        "url": "https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/2022.zip",
        "reference_date": "2022-01-01",
    },
    "2023": {
        "url": "https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/2023.zip",
        "reference_date": "2023-01-01",
    },
    "2024-06": {
        "url": "https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/2024.zip",
        "reference_date": "2024-06-01",
    },
    "2024-12": {
        "url": "https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/202412.zip",
        "reference_date": "2024-12-01",
    },
    "2025-07": {
        "url": "https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/202507.zip",
        "reference_date": "2025-07-01",
    },
    "2025-12": {
        "url": "https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/202512.zip",
        "reference_date": "2025-12-01",
    },
}

# Ordered list of snapshot periods for diffing consecutive pairs
NAR_SNAPSHOT_ORDER = ["2022", "2023", "2024-06", "2024-12", "2025-07", "2025-12"]

# ── Geocoder.ca ──────────────────────────────────────────────────────────────

GEOCODER_DATA_URL = "https://geocoder.ca/?freedata=1"

# ── GeoNames ─────────────────────────────────────────────────────────────────

GEONAMES_URL = "https://download.geonames.org/export/zip/CA_full.csv.zip"

# ── Province mappings ────────────────────────────────────────────────────────

# NAR uses numeric province codes
PROVINCE_CODE_TO_ABBR: dict[str, str] = {
    "10": "NL",
    "11": "PE",
    "12": "NS",
    "13": "NB",
    "24": "QC",
    "35": "ON",
    "46": "MB",
    "47": "SK",
    "48": "AB",
    "59": "BC",
    "60": "YT",
    "61": "NT",
    "62": "NU",
}

# First letter of postal code → province (for validation / fallback)
FSA_FIRST_LETTER_TO_PROVINCE: dict[str, str] = {
    "A": "NL",
    "B": "NS",
    "C": "PE",
    "E": "NB",
    "G": "QC",
    "H": "QC",
    "J": "QC",
    "K": "ON",
    "L": "ON",
    "M": "ON",
    "N": "ON",
    "P": "ON",
    "R": "MB",
    "S": "SK",
    "T": "AB",
    "V": "BC",
    "X": "NT",  # default; disambiguate NU below
    "Y": "YT",
}

# FSAs that belong to Nunavut (rest of X = NWT)
NUNAVUT_FSAS = frozenset({"X0A", "X0B", "X0C"})

PROVINCE_ABBR_TO_NAME: dict[str, str] = {
    "NL": "Newfoundland and Labrador",
    "PE": "Prince Edward Island",
    "NS": "Nova Scotia",
    "NB": "New Brunswick",
    "QC": "Quebec",
    "ON": "Ontario",
    "MB": "Manitoba",
    "SK": "Saskatchewan",
    "AB": "Alberta",
    "BC": "British Columbia",
    "YT": "Yukon",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
}

# ── NAR processing constants ────────────────────────────────────────────────

# Actual NAR CSV column mapping is in parser_nar.py (MAIL_POSTAL_CODE, etc.)
NAR_CHUNK_SIZE = 500_000

# ── Web server defaults ─────────────────────────────────────────────────────

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080
