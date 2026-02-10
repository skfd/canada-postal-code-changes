"""Compare consecutive postal code snapshots to detect changes."""

import logging

import pandas as pd

from src import db

logger = logging.getLogger(__name__)


def diff_snapshots(
    source_type: str,
    date_before: str,
    date_after: str,
) -> list[dict]:
    """Compare two snapshots and return a list of change events.

    Change types detected:
    - added: postal code in later snapshot but not earlier
    - removed: postal code in earlier snapshot but not later
    - city_changed: same postal code, different city name
    - csd_changed: same postal code, different CSD code
    - location_shifted: centroid moved >1 km
    """
    conn = db.get_connection()

    before = pd.read_sql(
        """
        SELECT postal_code, city_name, province_abbr, csd_code, latitude, longitude
        FROM postal_code_snapshots
        WHERE snapshot_date = ? AND source_type = ?
        """,
        conn,
        params=(date_before, source_type),
    ).set_index("postal_code")

    after = pd.read_sql(
        """
        SELECT postal_code, city_name, province_abbr, csd_code, latitude, longitude
        FROM postal_code_snapshots
        WHERE snapshot_date = ? AND source_type = ?
        """,
        conn,
        params=(date_after, source_type),
    ).set_index("postal_code")

    conn.close()

    if before.empty:
        logger.warning("No data for %s snapshot %s", source_type, date_before)
        return []
    if after.empty:
        logger.warning("No data for %s snapshot %s", source_type, date_after)
        return []

    changes: list[dict] = []

    # 1. ADDED: in after but not in before
    added_codes = after.index.difference(before.index)
    for pc in added_codes:
        row = after.loc[pc]
        changes.append(
            {
                "postal_code": pc,
                "change_type": "added",
                "source_type": source_type,
                "snapshot_before": date_before,
                "snapshot_after": date_after,
                "old_value": None,
                "new_value": None,
                "province_abbr": row["province_abbr"],
                "fsa": pc[:3],
            }
        )

    # 2. REMOVED: in before but not in after
    removed_codes = before.index.difference(after.index)
    for pc in removed_codes:
        row = before.loc[pc]
        changes.append(
            {
                "postal_code": pc,
                "change_type": "removed",
                "source_type": source_type,
                "snapshot_before": date_before,
                "snapshot_after": date_after,
                "old_value": None,
                "new_value": None,
                "province_abbr": row["province_abbr"],
                "fsa": pc[:3],
            }
        )

    # 3. MODIFICATIONS for codes present in both snapshots
    common = before.index.intersection(after.index)
    if common.empty:
        return changes

    b = before.loc[common]
    a = after.loc[common]

    # City name changes (case-insensitive comparison)
    b_city = b["city_name"].fillna("").str.lower()
    a_city = a["city_name"].fillna("").str.lower()
    city_changed = b_city != a_city
    for pc in city_changed[city_changed].index:
        changes.append(
            {
                "postal_code": pc,
                "change_type": "city_changed",
                "source_type": source_type,
                "snapshot_before": date_before,
                "snapshot_after": date_after,
                "old_value": b.loc[pc, "city_name"],
                "new_value": a.loc[pc, "city_name"],
                "province_abbr": a.loc[pc, "province_abbr"],
                "fsa": pc[:3],
            }
        )

    # CSD code changes
    b_csd = b["csd_code"].fillna("")
    a_csd = a["csd_code"].fillna("")
    csd_changed = b_csd != a_csd
    for pc in csd_changed[csd_changed].index:
        changes.append(
            {
                "postal_code": pc,
                "change_type": "csd_changed",
                "source_type": source_type,
                "snapshot_before": date_before,
                "snapshot_after": date_after,
                "old_value": b.loc[pc, "csd_code"],
                "new_value": a.loc[pc, "csd_code"],
                "province_abbr": a.loc[pc, "province_abbr"],
                "fsa": pc[:3],
            }
        )

    # Location shift > ~1 km (0.009° lat or 0.012° lon)
    lat_diff = (b["latitude"] - a["latitude"]).abs()
    lon_diff = (b["longitude"] - a["longitude"]).abs()
    shifted = (lat_diff > 0.009) | (lon_diff > 0.012)
    for pc in shifted[shifted].index:
        old_loc = f"{b.loc[pc, 'latitude']:.6f},{b.loc[pc, 'longitude']:.6f}"
        new_loc = f"{a.loc[pc, 'latitude']:.6f},{a.loc[pc, 'longitude']:.6f}"
        changes.append(
            {
                "postal_code": pc,
                "change_type": "location_shifted",
                "source_type": source_type,
                "snapshot_before": date_before,
                "snapshot_after": date_after,
                "old_value": old_loc,
                "new_value": new_loc,
                "province_abbr": a.loc[pc, "province_abbr"],
                "fsa": pc[:3],
            }
        )

    logger.info(
        "Diff %s %s→%s: %d added, %d removed, %d city, %d csd, %d location",
        source_type,
        date_before,
        date_after,
        len(added_codes),
        len(removed_codes),
        city_changed.sum(),
        csd_changed.sum(),
        shifted.sum(),
    )

    return changes


def store_changes(changes: list[dict]) -> int:
    """Insert change events into the database. Returns count inserted."""
    if not changes:
        return 0

    conn = db.get_connection()
    conn.executemany(
        """
        INSERT INTO postal_code_changes
            (postal_code, change_type, source_type,
             snapshot_before, snapshot_after,
             old_value, new_value, province_abbr, fsa)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                c["postal_code"],
                c["change_type"],
                c["source_type"],
                c["snapshot_before"],
                c["snapshot_after"],
                c["old_value"],
                c["new_value"],
                c["province_abbr"],
                c["fsa"],
            )
            for c in changes
        ],
    )
    conn.commit()
    conn.close()
    return len(changes)


def diff_all_pairs(source_type: str = "nar") -> dict[str, int]:
    """Run diffs for all consecutive snapshot pairs.

    Returns {pair_label: change_count}.
    """
    dates = db.get_snapshot_dates(source_type)
    if len(dates) < 2:
        logger.warning("Need at least 2 snapshots to diff, found %d", len(dates))
        return {}

    # Clear existing changes for this source before recomputing
    db.clear_changes(source_type)

    results = {}
    for i in range(len(dates) - 1):
        d_before = dates[i]
        d_after = dates[i + 1]
        label = f"{d_before}→{d_after}"
        logger.info("Diffing %s %s", source_type, label)

        changes = diff_snapshots(source_type, d_before, d_after)
        count = store_changes(changes)
        results[label] = count
        logger.info("  %s: %d changes", label, count)

    return results
