"""Compare consecutive postal code snapshots to detect changes."""

import logging

import pandas as pd

from src import db

logger = logging.getLogger(__name__)


def diff_snapshots(
    source_type: str,
    date_before: str,
    date_after: str,
) -> pd.DataFrame:
    """Compare two snapshots and return a DataFrame of change events.

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

    empty = pd.DataFrame(columns=[
        "postal_code", "change_type", "source_type",
        "snapshot_before", "snapshot_after",
        "old_value", "new_value", "province_abbr", "fsa",
    ])

    if before.empty:
        logger.warning("No data for %s snapshot %s", source_type, date_before)
        return empty
    if after.empty:
        logger.warning("No data for %s snapshot %s", source_type, date_after)
        return empty

    changes_dfs = []

    # 1. ADDED: in after but not in before
    added_codes = after.index.difference(before.index)
    if len(added_codes) > 0:
        added_df = pd.DataFrame({
            "postal_code": added_codes,
            "change_type": "added",
            "source_type": source_type,
            "snapshot_before": date_before,
            "snapshot_after": date_after,
            "old_value": None,
            "new_value": None,
            "province_abbr": after.loc[added_codes, "province_abbr"].values,
            "fsa": pd.Series(added_codes).str[:3].values,
        })
        changes_dfs.append(added_df)

    # 2. REMOVED: in before but not in after
    removed_codes = before.index.difference(after.index)
    if len(removed_codes) > 0:
        removed_df = pd.DataFrame({
            "postal_code": removed_codes,
            "change_type": "removed",
            "source_type": source_type,
            "snapshot_before": date_before,
            "snapshot_after": date_after,
            "old_value": None,
            "new_value": None,
            "province_abbr": before.loc[removed_codes, "province_abbr"].values,
            "fsa": pd.Series(removed_codes).str[:3].values,
        })
        changes_dfs.append(removed_df)

    # 3. MODIFICATIONS for codes present in both snapshots
    common = before.index.intersection(after.index)
    if not common.empty:
        b = before.loc[common]
        a = after.loc[common]

        # City name changes (case-insensitive comparison)
        b_city = b["city_name"].fillna("").str.lower()
        a_city = a["city_name"].fillna("").str.lower()
        city_mask = b_city != a_city
        if city_mask.any():
            pcs = city_mask[city_mask].index
            city_df = pd.DataFrame({
                "postal_code": pcs,
                "change_type": "city_changed",
                "source_type": source_type,
                "snapshot_before": date_before,
                "snapshot_after": date_after,
                "old_value": b.loc[pcs, "city_name"].values,
                "new_value": a.loc[pcs, "city_name"].values,
                "province_abbr": a.loc[pcs, "province_abbr"].values,
                "fsa": pd.Series(pcs).str[:3].values,
            })
            changes_dfs.append(city_df)

        # CSD code changes
        b_csd = b["csd_code"].fillna("")
        a_csd = a["csd_code"].fillna("")
        csd_mask = b_csd != a_csd
        if csd_mask.any():
            pcs = csd_mask[csd_mask].index
            csd_df = pd.DataFrame({
                "postal_code": pcs,
                "change_type": "csd_changed",
                "source_type": source_type,
                "snapshot_before": date_before,
                "snapshot_after": date_after,
                "old_value": b.loc[pcs, "csd_code"].values,
                "new_value": a.loc[pcs, "csd_code"].values,
                "province_abbr": a.loc[pcs, "province_abbr"].values,
                "fsa": pd.Series(pcs).str[:3].values,
            })
            changes_dfs.append(csd_df)

        # Location shift > ~1 km (0.009 deg lat or 0.012 deg lon)
        lat_diff = (b["latitude"] - a["latitude"]).abs()
        lon_diff = (b["longitude"] - a["longitude"]).abs()
        shifted = (lat_diff > 0.009) | (lon_diff > 0.012)
        if shifted.any():
            pcs = shifted[shifted].index
            old_loc = b.loc[pcs, "latitude"].astype(str) + "," + b.loc[pcs, "longitude"].astype(str)
            new_loc = a.loc[pcs, "latitude"].astype(str) + "," + a.loc[pcs, "longitude"].astype(str)
            loc_df = pd.DataFrame({
                "postal_code": pcs,
                "change_type": "location_shifted",
                "source_type": source_type,
                "snapshot_before": date_before,
                "snapshot_after": date_after,
                "old_value": old_loc.values,
                "new_value": new_loc.values,
                "province_abbr": a.loc[pcs, "province_abbr"].values,
                "fsa": pd.Series(pcs).str[:3].values,
            })
            changes_dfs.append(loc_df)

    if changes_dfs:
        all_changes = pd.concat(changes_dfs, ignore_index=True)
    else:
        all_changes = empty

    logger.info(
        "Diff %s %s->%s: %d added, %d removed, %d other modifications",
        source_type,
        date_before,
        date_after,
        len(added_codes),
        len(removed_codes),
        len(all_changes) - len(added_codes) - len(removed_codes),
    )

    return all_changes


def store_changes(changes: pd.DataFrame) -> int:
    """Insert change events DataFrame into the database. Returns count inserted."""
    if changes.empty:
        return 0

    conn = db.get_connection()
    insert_cols = [
        "postal_code", "change_type", "source_type",
        "snapshot_before", "snapshot_after",
        "old_value", "new_value", "province_abbr", "fsa",
    ]
    changes[insert_cols].to_sql(
        "postal_code_changes",
        conn,
        if_exists="append",
        index=False,
        chunksize=5000,
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
        label = f"{d_before}->{d_after}"
        logger.info("Diffing %s %s", source_type, label)

        changes = diff_snapshots(source_type, d_before, d_after)
        count = store_changes(changes)
        results[label] = count
        logger.info("  %s: %d changes", label, count)

    return results
