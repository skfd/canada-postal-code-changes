"""Generate static JSON data files for the changes-only static site."""

import json
import logging
from pathlib import Path

from src import db

logger = logging.getLogger(__name__)

STATIC_DATA_DIR = Path("static-site/data")


def generate_all() -> None:
    """Generate all JSON data files for the static site."""
    STATIC_DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = db.get_connection()

    _generate_summary(conn)
    _generate_timeline(conn)
    _generate_by_province(conn)
    _generate_added(conn)
    _generate_removed(conn)
    _generate_city_changed(conn)

    conn.close()
    logger.info("Static data generated in %s", STATIC_DATA_DIR)


def _write_json(filename: str, data: object) -> None:
    path = STATIC_DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = path.stat().st_size / 1024
    logger.info("  Wrote %s (%.1f KB)", filename, size_kb)


def _generate_summary(conn) -> None:
    """Overall stats."""
    row = conn.execute(
        "SELECT COUNT(*) as total, SUM(is_active) as active FROM postal_code_summary"
    ).fetchone()

    snapshots = conn.execute(
        "SELECT COUNT(DISTINCT snapshot_date) as n FROM postal_code_snapshots "
        "WHERE source_type = 'merged'"
    ).fetchone()

    changes = conn.execute(
        "SELECT change_type, COUNT(*) as cnt FROM postal_code_changes "
        "WHERE source_type = 'merged' AND change_type IN ('added', 'removed', 'city_changed') "
        "GROUP BY change_type"
    ).fetchall()

    change_counts = {r["change_type"]: r["cnt"] for r in changes}

    _write_json("summary.json", {
        "total_codes": row["total"],
        "active_codes": row["active"],
        "snapshots": snapshots["n"],
        "added": change_counts.get("added", 0),
        "removed": change_counts.get("removed", 0),
        "city_changed": change_counts.get("city_changed", 0),
        "total_changes": sum(change_counts.values()),
    })


def _generate_timeline(conn) -> None:
    """Per-period change counts."""
    rows = conn.execute(
        "SELECT snapshot_before || ' to ' || snapshot_after as period, "
        "  change_type, COUNT(*) as cnt "
        "FROM postal_code_changes "
        "WHERE source_type = 'merged' AND change_type IN ('added', 'removed', 'city_changed') "
        "GROUP BY period, change_type "
        "ORDER BY period, change_type"
    ).fetchall()

    # Pivot: {period -> {added: N, removed: N, city_changed: N}}
    periods = {}
    for r in rows:
        p = r["period"]
        if p not in periods:
            periods[p] = {"period": p, "added": 0, "removed": 0, "city_changed": 0}
        periods[p][r["change_type"]] = r["cnt"]

    _write_json("timeline.json", list(periods.values()))


def _generate_by_province(conn) -> None:
    """Per-province change counts."""
    rows = conn.execute(
        "SELECT province_abbr, change_type, COUNT(*) as cnt "
        "FROM postal_code_changes "
        "WHERE source_type = 'merged' AND change_type IN ('added', 'removed', 'city_changed') "
        "  AND province_abbr IS NOT NULL AND province_abbr != '' "
        "GROUP BY province_abbr, change_type "
        "ORDER BY province_abbr, change_type"
    ).fetchall()

    provinces = {}
    for r in rows:
        prov = r["province_abbr"]
        if prov not in provinces:
            provinces[prov] = {"added": 0, "removed": 0, "city_changed": 0}
        provinces[prov][r["change_type"]] = r["cnt"]

    _write_json("by_province.json", provinces)


def _generate_added(conn) -> None:
    """All postal codes added after initial snapshot."""
    rows = conn.execute(
        "SELECT postal_code as pc, snapshot_after as date, "
        "  province_abbr as prov, fsa "
        "FROM postal_code_changes "
        "WHERE source_type = 'merged' AND change_type = 'added' "
        "ORDER BY snapshot_after, postal_code"
    ).fetchall()

    # Add city from summary table for each postal code
    cities = {}
    city_rows = conn.execute(
        "SELECT postal_code, city_name FROM postal_code_summary"
    ).fetchall()
    for r in city_rows:
        cities[r["postal_code"]] = r["city_name"]

    data = []
    for r in rows:
        data.append({
            "pc": r["pc"],
            "date": r["date"],
            "prov": r["prov"],
            "city": cities.get(r["pc"], ""),
            "fsa": r["fsa"],
        })

    _write_json("added.json", data)


def _generate_removed(conn) -> None:
    """All removed postal codes."""
    rows = conn.execute(
        "SELECT c.postal_code as pc, c.snapshot_after as date, "
        "  c.province_abbr as prov, c.fsa, "
        "  s.city_name as city "
        "FROM postal_code_changes c "
        "LEFT JOIN postal_code_summary s ON s.postal_code = c.postal_code "
        "WHERE c.source_type = 'merged' AND c.change_type = 'removed' "
        "ORDER BY c.snapshot_after, c.postal_code"
    ).fetchall()

    data = [{"pc": r["pc"], "date": r["date"], "prov": r["prov"],
             "city": r["city"] or "", "fsa": r["fsa"]} for r in rows]

    _write_json("removed.json", data)


def _generate_city_changed(conn) -> None:
    """All city name changes."""
    rows = conn.execute(
        "SELECT postal_code as pc, snapshot_after as date, "
        "  province_abbr as prov, fsa, old_value as old_city, new_value as new_city "
        "FROM postal_code_changes "
        "WHERE source_type = 'merged' AND change_type = 'city_changed' "
        "ORDER BY snapshot_after, postal_code"
    ).fetchall()

    data = [{"pc": r["pc"], "date": r["date"], "prov": r["prov"],
             "fsa": r["fsa"], "old": r["old_city"] or "", "new": r["new_city"] or ""}
            for r in rows]

    _write_json("city_changed.json", data)
