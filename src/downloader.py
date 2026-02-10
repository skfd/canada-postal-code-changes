"""Download NAR ZIPs, Geocoder.ca, and GeoNames data files."""

import zipfile
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

from src import db
from src.config import (
    GEONAMES_URL,
    NAR_SNAPSHOTS,
    RAW_GEOCODER_DIR,
    RAW_GEONAMES_DIR,
    RAW_NAR_DIR,
)


def _download_file(url: str, dest: Path, description: str = "") -> Path:
    """Stream-download a file with a progress bar. Returns the dest path."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    label = description or dest.name
    with (
        open(dest, "wb") as f,
        tqdm(total=total, unit="B", unit_scale=True, desc=label) as bar,
    ):
        for chunk in resp.iter_content(chunk_size=1024 * 256):
            f.write(chunk)
            bar.update(len(chunk))
    return dest


def _extract_zip(zip_path: Path, extract_dir: Path) -> list[Path]:
    """Extract a ZIP and return list of extracted file paths."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
        return [extract_dir / name for name in zf.namelist()]


def download_nar(period: str) -> Path | None:
    """Download a single NAR snapshot ZIP. Returns path to extracted dir or None."""
    info = NAR_SNAPSHOTS.get(period)
    if not info:
        raise ValueError(
            f"Unknown NAR period: {period!r}. "
            f"Available: {', '.join(NAR_SNAPSHOTS)}"
        )

    zip_path = RAW_NAR_DIR / f"{period}.zip"
    extract_dir = RAW_NAR_DIR / period

    if zip_path.exists():
        print(f"  Already downloaded: {zip_path}")
    else:
        print(f"  Downloading NAR {period} ...")
        _download_file(info["url"], zip_path, description=f"NAR {period}")

    # Extract if not already done
    if not extract_dir.exists() or not any(extract_dir.iterdir()):
        print(f"  Extracting to {extract_dir} ...")
        _extract_zip(zip_path, extract_dir)
    else:
        print(f"  Already extracted: {extract_dir}")

    # Record in database
    db.init_db()
    db.record_download(
        source_type="nar",
        reference_date=info["reference_date"],
        download_url=info["url"],
        file_path=str(extract_dir),
    )

    return extract_dir


def download_all_nar() -> list[Path]:
    """Download all known NAR snapshots."""
    results = []
    for period in NAR_SNAPSHOTS:
        path = download_nar(period)
        if path:
            results.append(path)
    return results


def download_geocoder() -> Path | None:
    """Download current Geocoder.ca CSV.

    Note: Geocoder.ca may require browser interaction to get the actual
    download link. This function stores the data page URL for reference.
    The actual CSV download may need manual steps — see data-spec.md Section 5.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    dest = RAW_GEOCODER_DIR / f"{today}.csv"

    if dest.exists():
        print(f"  Already downloaded: {dest}")
        return dest

    # The free data page URL — actual CSV download link may differ
    print(
        "  NOTE: Geocoder.ca may require manual download from:\n"
        "    https://geocoder.ca/?freedata=1\n"
        f"  Save the CSV to: {dest}"
    )

    db.init_db()
    db.record_download(
        source_type="geocoder",
        reference_date=today,
        download_url="https://geocoder.ca/?freedata=1",
        file_path=str(dest),
    )
    return None


def download_geonames() -> Path | None:
    """Download GeoNames CA_full.csv.zip and extract."""
    zip_path = RAW_GEONAMES_DIR / "CA_full.csv.zip"
    extract_dir = RAW_GEONAMES_DIR

    if zip_path.exists():
        print(f"  Already downloaded: {zip_path}")
    else:
        print("  Downloading GeoNames Canada data ...")
        _download_file(GEONAMES_URL, zip_path, description="GeoNames CA")

    # Extract
    extracted = list(extract_dir.glob("CA_full.txt")) + list(
        extract_dir.glob("CA_full.csv")
    )
    if not extracted:
        print(f"  Extracting to {extract_dir} ...")
        files = _extract_zip(zip_path, extract_dir)
        extracted = [f for f in files if "CA" in f.name]

    today = datetime.now().strftime("%Y-%m-%d")
    file_path = str(extracted[0]) if extracted else str(extract_dir)

    db.init_db()
    db.record_download(
        source_type="geonames",
        reference_date=today,
        download_url=GEONAMES_URL,
        file_path=file_path,
    )

    return extract_dir
