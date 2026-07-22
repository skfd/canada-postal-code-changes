"""Generate static JSON data files for the changes-only static site.

Copyright (c) 2026 Canadian Postal Code Change Tracking System Contributors
Licensed under the MIT License - see LICENSE file for details.

Data sources used by this software have their own licenses:
- Statistics Canada NAR: Statistics Canada Open Licence
- Geocoder.ca: CC BY 2.5 Canada
- GeoNames: CC BY 4.0
"""

import json
import logging
from pathlib import Path

from src import db
from src.classifier import classify_city_change
from src.config import PROVINCE_ABBR_TO_NAME

logger = logging.getLogger(__name__)

STATIC_DATA_DIR = Path("docs/data")

# A "city change" is only real if it falls in one of these subtypes; every other
# subtype (encoding, accent_normalization, punctuation, spacing, abbreviation) is
# technical noise from the feed and is reported separately. This is the single
# shared definition — mirror it anywhere the distinction is made.
REAL_SUBTYPES = ("substantive", "boundary", "rename")
_REAL_SQL = "change_subtype IN ('substantive', 'boundary', 'rename')"

_MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _label(d: str) -> str:
    """'2026-02-10' -> 'Feb 2026'."""
    y, m = d[:4], int(d[5:7])
    return f"{_MONTHS[m]} {y}"


def generate_all() -> None:
    """Generate all JSON data files for the static site."""
    STATIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    (STATIC_DATA_DIR / "snapshot").mkdir(parents=True, exist_ok=True)

    conn = db.get_connection()

    removed = _generate_removed(conn)
    snapshots = _snapshot_transitions(conn, removed)

    _generate_summary(conn, removed)
    _generate_snapshots(conn, snapshots)
    _generate_by_province(conn)
    _generate_by_fsa(conn)
    _generate_added(conn)
    _generate_city_changed(conn)
    _generate_map_points(conn, snapshots)
    _generate_snapshot_details(conn, snapshots)

    conn.close()
    logger.info("Static data generated in %s", STATIC_DATA_DIR)


def _write_json(filename: str, data: object) -> None:
    path = STATIC_DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = path.stat().st_size / 1024
    logger.info("  Wrote %s (%.1f KB)", filename, size_kb)


def _active_by_snapshot(conn) -> dict[str, int]:
    rows = conn.execute(
        "SELECT snapshot_date, COUNT(*) AS n FROM postal_code_snapshots "
        "WHERE source_type = 'merged' GROUP BY snapshot_date ORDER BY snapshot_date"
    ).fetchall()
    return {r["snapshot_date"]: r["n"] for r in rows}


def _snapshot_transitions(conn, removed: list[dict]) -> list[dict]:
    """One record per snapshot transition (skips the baseline snapshot)."""
    active = _active_by_snapshot(conn)
    dates = list(active.keys())

    returned: dict[str, int] = {}
    for r in removed:
        if "back" in r:
            returned[r["date"]] = returned.get(r["date"], 0) + 1

    rows = conn.execute(
        f"""
        SELECT snapshot_before, snapshot_after,
               SUM(change_type = 'added') AS added,
               SUM(change_type = 'removed') AS removed,
               SUM(change_type = 'csd_changed') AS csd_changed,
               SUM(change_type = 'city_changed' AND {_REAL_SQL}) AS city_real,
               SUM(change_type = 'city_changed' AND NOT {_REAL_SQL}) AS city_noise
        FROM postal_code_changes
        WHERE source_type = 'merged'
        GROUP BY snapshot_before, snapshot_after
        ORDER BY snapshot_after
        """
    ).fetchall()

    out = []
    for r in rows:
        before, after = r["snapshot_before"], r["snapshot_after"]
        out.append({
            "date": after,
            "before": before,
            "label": _label(after),
            "range": f"{_label(before)} – {_label(after)}",
            "active_before": active.get(before, 0),
            "active_after": active.get(after, 0),
            "added": r["added"] or 0,
            "removed": r["removed"] or 0,
            "removed_returned": returned.get(after, 0),
            "csd_changed": r["csd_changed"] or 0,
            "city_real": r["city_real"] or 0,
            "city_noise": r["city_noise"] or 0,
        })

    # Baseline (first snapshot) — carried for the cumulative line chart only.
    if dates:
        out.insert(0, {
            "date": dates[0], "before": None, "label": _label(dates[0]),
            "range": _label(dates[0]), "active_before": None,
            "active_after": active[dates[0]], "baseline": True,
            "added": 0, "removed": 0, "removed_returned": 0, "csd_changed": 0,
            "city_real": 0, "city_noise": 0,
        })
    return out


def _generate_summary(conn, removed: list[dict]) -> None:
    """Overall stats — real numbers lead, raw total kept as a secondary fact."""
    row = conn.execute(
        "SELECT COUNT(*) AS total, SUM(is_active) AS active FROM postal_code_summary"
    ).fetchone()

    active = _active_by_snapshot(conn)
    dates = list(active.keys())

    counts = conn.execute(
        f"""
        SELECT
          SUM(change_type = 'added') AS added,
          SUM(change_type = 'removed') AS removed,
          SUM(change_type = 'csd_changed') AS csd_changed,
          SUM(change_type = 'city_changed') AS city_total,
          SUM(change_type = 'city_changed' AND {_REAL_SQL}) AS city_real,
          SUM(change_type = 'city_changed' AND NOT {_REAL_SQL}) AS city_noise
        FROM postal_code_changes WHERE source_type = 'merged'
        """
    ).fetchone()

    net = (active[dates[-1]] - active[dates[0]]) if dates else 0

    subtypes = conn.execute(
        "SELECT change_subtype AS s, COUNT(*) AS n FROM postal_code_changes "
        "WHERE source_type = 'merged' AND change_type = 'city_changed' "
        "GROUP BY change_subtype ORDER BY n DESC"
    ).fetchall()

    _write_json("summary.json", {
        "total_codes": row["total"],
        "active_codes": row["active"],
        "snapshots": len(dates),
        "first_active": active[dates[0]] if dates else 0,
        "last_active": active[dates[-1]] if dates else 0,
        "net_growth": net,
        "added": counts["added"] or 0,
        "removed": counts["removed"] or 0,
        "removed_returned": sum(1 for r in removed if "back" in r),
        "csd_changed": counts["csd_changed"] or 0,
        "city_total": counts["city_total"] or 0,
        "city_real": counts["city_real"] or 0,
        "city_noise": counts["city_noise"] or 0,
        "city_subtypes": {r["s"]: r["n"] for r in subtypes},
    })


def _generate_snapshots(conn, snapshots: list[dict]) -> None:
    """Snapshot index — powers the cumulative line, diverging bars and batch strip."""
    _write_json("snapshots.json", snapshots)


def _generate_by_province(conn) -> None:
    """Per-province change counts, split real vs noise."""
    rows = conn.execute(
        f"""
        SELECT province_abbr,
          SUM(change_type = 'added') AS added,
          SUM(change_type = 'removed') AS removed,
          SUM(change_type = 'csd_changed') AS csd_changed,
          SUM(change_type = 'city_changed' AND {_REAL_SQL}) AS city_real,
          SUM(change_type = 'city_changed' AND NOT {_REAL_SQL}) AS city_noise
        FROM postal_code_changes
        WHERE source_type = 'merged' AND province_abbr IS NOT NULL AND province_abbr != ''
        GROUP BY province_abbr
        """
    ).fetchall()

    provinces = {}
    for r in rows:
        provinces[r["province_abbr"]] = {
            "name": PROVINCE_ABBR_TO_NAME.get(r["province_abbr"], r["province_abbr"]),
            "added": r["added"] or 0,
            "removed": r["removed"] or 0,
            "csd_changed": r["csd_changed"] or 0,
            "city_real": r["city_real"] or 0,
            "city_noise": r["city_noise"] or 0,
        }
    _write_json("by_province.json", provinces)


def _generate_by_fsa(conn) -> None:
    """Per-FSA change counts for the choropleth, split real vs noise."""
    rows = conn.execute(
        f"""
        SELECT fsa,
          SUM(change_type = 'added') AS added,
          SUM(change_type = 'removed') AS removed,
          SUM(change_type = 'csd_changed') AS csd,
          SUM(change_type = 'city_changed' AND {_REAL_SQL}) AS creal,
          SUM(change_type = 'city_changed' AND NOT {_REAL_SQL}) AS cnoise
        FROM postal_code_changes
        WHERE source_type = 'merged' AND fsa != ''
        GROUP BY fsa
        """
    ).fetchall()

    out = {}
    for r in rows:
        a, rm = r["added"] or 0, r["removed"] or 0
        cs, cr, cn = r["csd"] or 0, r["creal"] or 0, r["cnoise"] or 0
        out[r["fsa"]] = {"added": a, "removed": rm, "csd": cs, "creal": cr,
                         "cnoise": cn, "total": a + rm + cs + cr + cn}
    _write_json("by_fsa.json", out)


def _generate_added(conn) -> None:
    """All postal codes added after initial snapshot."""
    rows = conn.execute(
        "SELECT postal_code AS pc, snapshot_after AS date, province_abbr AS prov, fsa "
        "FROM postal_code_changes "
        "WHERE source_type = 'merged' AND change_type = 'added' "
        "ORDER BY snapshot_after, postal_code"
    ).fetchall()

    cities = {
        r["postal_code"]: r["city_name"]
        for r in conn.execute("SELECT postal_code, city_name FROM postal_code_summary")
    }

    data = [{"pc": r["pc"], "date": r["date"], "prov": r["prov"],
             "city": cities.get(r["pc"], ""), "fsa": r["fsa"]} for r in rows]
    _write_json("added.json", data)


def _return_dates(conn) -> dict[str, list[str]]:
    """postal_code -> NAR snapshots where the code came back after a removal.

    Measured on the NAR series only. The merged series unions in other sources,
    so a code missing from NAR but present in GeoNames looks like a return when
    that source first joins — a coverage artifact, not a Canada Post reissue.
    """
    returns: dict[str, list[str]] = {}
    for r in conn.execute(
        "SELECT postal_code, snapshot_after FROM postal_code_changes "
        "WHERE source_type = 'nar' AND change_subtype = 'reintroduced' "
        "ORDER BY snapshot_after"
    ):
        returns.setdefault(r["postal_code"], []).append(r["snapshot_after"])
    return returns


def _generate_removed(conn) -> list[dict]:
    """All removed postal codes, flagged with the date any of them came back."""
    rows = conn.execute(
        "SELECT c.postal_code AS pc, c.snapshot_after AS date, "
        "  c.province_abbr AS prov, c.fsa, s.city_name AS city "
        "FROM postal_code_changes c "
        "LEFT JOIN postal_code_summary s ON s.postal_code = c.postal_code "
        "WHERE c.source_type = 'merged' AND c.change_type = 'removed' "
        "ORDER BY c.snapshot_after, c.postal_code"
    ).fetchall()

    returns = _return_dates(conn)

    data = []
    for r in rows:
        rec = {"pc": r["pc"], "date": r["date"], "prov": r["prov"],
               "city": r["city"] or "", "fsa": r["fsa"]}
        back = next((d for d in returns.get(r["pc"], []) if d > r["date"]), None)
        if back:
            rec["back"] = back
        data.append(rec)
    _write_json("removed.json", data)
    return data


def _generate_city_changed(conn) -> None:
    """All city name changes with subtype classification."""
    rows = conn.execute(
        "SELECT postal_code AS pc, snapshot_after AS date, province_abbr AS prov, fsa, "
        "  old_value AS old_city, new_value AS new_city, change_subtype AS sub "
        "FROM postal_code_changes "
        "WHERE source_type = 'merged' AND change_type = 'city_changed' "
        "ORDER BY snapshot_after, postal_code"
    ).fetchall()

    data = [{"pc": r["pc"], "date": r["date"], "prov": r["prov"], "fsa": r["fsa"],
             "old": r["old_city"] or "", "new": r["new_city"] or "", "sub": r["sub"] or ""}
            for r in rows]
    _write_json("city_changed.json", data)


def _generate_map_points(conn, snapshots: list[dict]) -> None:
    """Per-code points for the map's zoom-in pin layer, grouped by FSA.

    Each real change (added/removed/real-rename/csd-move) gets one point at its
    true lat/lon so users can zoom in and see exactly which codes changed.
    Encoding noise is excluded — including for csd (municipality) moves, where
    only genuine A->B reassignments are kept (mojibake/spacing fixes and
    null<->name transitions are dropped).

    Each point carries the province plus the change's old/new place names so the
    map can show a meaningful hover. For added/removed the place name is looked
    up from the after/before snapshot (the change row itself has none).

    Writes an all-time aggregate (map_points.json, used by the overview map) plus
    one file per transition (map_points/<date>.json, used by that snapshot's map).
    Output shape in every file: {fsa: {a|r|n|m: [[lon, lat, code, prov, old, new], ...]}}.
    """
    (STATIC_DATA_DIR / "map_points").mkdir(parents=True, exist_ok=True)

    # Coordinate lookup per postal code — geonames is precise per code; fill gaps from merged.
    coords: dict[str, tuple[float, float]] = {}
    for src in ("geonames", "merged"):
        for r in conn.execute(
            "SELECT postal_code AS pc, latitude AS lat, longitude AS lon "
            "FROM postal_code_snapshots WHERE source_type = ? AND latitude IS NOT NULL",
            (src,),
        ):
            coords.setdefault(r["pc"], (r["lon"], r["lat"]))

    # City name per (postal code) for a given merged snapshot date, cached — used
    # to name added (after date) and removed (before date) codes.
    city_cache: dict[str, dict[str, str]] = {}

    def _cities(date: str) -> dict[str, str]:
        if date not in city_cache:
            city_cache[date] = {
                r["pc"]: r["c"]
                for r in conn.execute(
                    "SELECT postal_code AS pc, city_name AS c "
                    "FROM postal_code_snapshots "
                    "WHERE source_type = 'merged' AND snapshot_date = ? "
                    "AND city_name IS NOT NULL AND city_name != ''",
                    (date,),
                )
            }
        return city_cache[date]

    bucket = {"added": "a", "removed": "r", "csd_changed": "m"}

    def _values(row) -> tuple[str, str] | None:
        """Resolve (old, new) place names for a change row, or None to skip it.

        added   -> (None, city@after);  removed -> (city@before, None)
        rename  -> (old_city, new_city) straight from the row
        csd move-> (old_muni, new_muni) but only genuine moves survive
        """
        ct, pc = row["ct"], row["pc"]
        if ct == "added":
            return None, _cities(row["sa"]).get(pc)
        if ct == "removed":
            return _cities(row["sb"]).get(pc), None
        if ct == "city_changed":
            return row["ov"], row["nv"]
        # csd_changed: drop noise (encoding/spacing) and null<->name populate/clear
        ov, nv = row["ov"], row["nv"]
        if not ov or not nv:
            return None
        if classify_city_change(ov, nv) not in REAL_SUBTYPES:
            return None
        return ov, nv

    def _place(out: dict, row) -> bool:
        """Append row's point to `out` under its FSA/bucket; False if skipped/no coords."""
        c = coords.get(row["pc"])
        if not c:
            return False
        vals = _values(row)
        if vals is None:  # a filtered-out csd change
            return False
        old, new = vals
        b = bucket.get(row["ct"], "n")  # city_changed real -> 'n'
        out.setdefault(row["pc"][:3], {}).setdefault(b, []).append(
            [round(c[0], 4), round(c[1], 4), row["pc"], row["prov"], old, new]
        )
        return True

    select = f"""
        SELECT postal_code AS pc, change_type AS ct, province_abbr AS prov,
               old_value AS ov, new_value AS nv,
               snapshot_before AS sb, snapshot_after AS sa
        FROM postal_code_changes
        WHERE source_type = 'merged' AND {{extra}} (
              change_type IN ('added', 'removed', 'csd_changed')
              OR (change_type = 'city_changed' AND {_REAL_SQL}))
        """

    # All-time aggregate for the overview map. Ordered by date so that, for a code
    # that changed the same way more than once, the most recent instance wins.
    agg: dict[str, dict[str, list]] = {}
    latest: dict[tuple[str, str], object] = {}
    for r in conn.execute(select.format(extra="") + " ORDER BY snapshot_after ASC"):
        latest[(r["pc"], r["ct"])] = r
    placed = missing = 0
    for r in latest.values():
        if _place(agg, r):
            placed += 1
        else:
            missing += 1
    _write_json("map_points.json", agg)

    # Per-snapshot points, filtered to that transition's changes.
    for s in snapshots:
        if s.get("baseline"):
            continue
        after = s["date"]
        out: dict[str, dict[str, list]] = {}
        for r in conn.execute(
            select.format(extra="snapshot_after = ? AND"), (after,)
        ):
            _place(out, r)
        _write_json(f"map_points/{after}.json", out)

    logger.info("map_points: %d points placed, %d skipped/without coordinates", placed, missing)


# --- per-snapshot deep-dive data -------------------------------------------------

def _group_provinces(conn, after: str, extra: str) -> list[list]:
    """Top-6 provinces (+Other) for a change slice, as [abbr, count] pairs."""
    rows = conn.execute(
        f"SELECT province_abbr AS p, COUNT(*) AS n FROM postal_code_changes "
        f"WHERE source_type = 'merged' AND snapshot_after = ? AND province_abbr != '' {extra} "
        f"GROUP BY province_abbr ORDER BY n DESC",
        (after,),
    ).fetchall()
    top = [[r["p"], r["n"]] for r in rows[:6]]
    other = sum(r["n"] for r in rows[6:])
    if other:
        top.append(["Other", other])
    return top


def _city_examples(conn, after: str, subtype: str) -> list[list]:
    """Most frequent distinct place-name changes of a subtype, with a sample code."""
    rows = conn.execute(
        "SELECT MIN(postal_code) AS pc, old_value AS o, new_value AS nv, "
        "  MIN(province_abbr) AS p, COUNT(*) AS n "
        "FROM postal_code_changes "
        "WHERE source_type = 'merged' AND snapshot_after = ? AND change_type = 'city_changed' "
        "  AND change_subtype = ? AND old_value != '' AND new_value != '' "
        "GROUP BY old_value, new_value ORDER BY n DESC LIMIT 12",
        (after, subtype),
    ).fetchall()
    return [[r["pc"], r["o"], r["nv"], r["p"], r["n"]] for r in rows]


def _added_fsa(conn, after: str) -> list[list]:
    rows = conn.execute(
        "SELECT fsa, province_abbr AS p, COUNT(*) AS n FROM postal_code_changes "
        "WHERE source_type = 'merged' AND snapshot_after = ? AND change_type = 'added' AND fsa != '' "
        "GROUP BY fsa ORDER BY n DESC LIMIT 12",
        (after,),
    ).fetchall()
    return [[r["fsa"], r["p"], r["n"]] for r in rows]


def _generate_snapshot_details(conn, snapshots: list[dict]) -> None:
    """Write one deep-dive JSON per transition to docs/data/snapshot/<date>.json."""
    for s in snapshots:
        if s.get("baseline"):
            continue
        after = s["date"]

        provinces = conn.execute(
            f"""
            SELECT province_abbr AS p,
              SUM(change_type = 'added') AS added,
              SUM(change_type = 'removed') AS removed,
              SUM(change_type = 'csd_changed') AS csd,
              SUM(change_type = 'city_changed' AND {_REAL_SQL}) AS creal,
              SUM(change_type = 'city_changed' AND NOT {_REAL_SQL}) AS cnoise
            FROM postal_code_changes
            WHERE source_type = 'merged' AND snapshot_after = ? AND province_abbr != ''
            GROUP BY province_abbr ORDER BY added DESC
            """,
            (after,),
        ).fetchall()

        top_fsa = conn.execute(
            "SELECT fsa, province_abbr AS p, COUNT(*) AS n FROM postal_code_changes "
            "WHERE source_type = 'merged' AND snapshot_after = ? AND fsa != '' "
            "GROUP BY fsa ORDER BY n DESC LIMIT 12",
            (after,),
        ).fetchall()

        fsa_rows = conn.execute(
            f"""
            SELECT fsa,
              SUM(change_type = 'added') AS added,
              SUM(change_type = 'removed') AS removed,
              SUM(change_type = 'csd_changed') AS csd,
              SUM(change_type = 'city_changed' AND {_REAL_SQL}) AS creal,
              SUM(change_type = 'city_changed' AND NOT {_REAL_SQL}) AS cnoise
            FROM postal_code_changes
            WHERE source_type = 'merged' AND snapshot_after = ? AND fsa != ''
            GROUP BY fsa
            """,
            (after,),
        ).fetchall()
        fsa_counts = {}
        for r in fsa_rows:
            a, rm = r["added"] or 0, r["removed"] or 0
            cs, cr, cn = r["csd"] or 0, r["creal"] or 0, r["cnoise"] or 0
            fsa_counts[r["fsa"]] = {"added": a, "removed": rm, "csd": cs, "creal": cr,
                                    "cnoise": cn, "total": a + rm + cs + cr + cn}

        groups = [
            {"key": "added", "name": "Additions", "count": s["added"],
             "provinces": _group_provinces(conn, after, "AND change_type = 'added'"),
             "top_fsa": _added_fsa(conn, after)},
            {"key": "boundary", "name": "Boundary reassignments",
             "count": _count(conn, after, "change_subtype = 'boundary'"),
             "provinces": _group_provinces(conn, after, "AND change_subtype = 'boundary'"),
             "examples": _city_examples(conn, after, "boundary")},
            {"key": "substantive", "name": "Substantive renames",
             "count": _count(conn, after, "change_subtype IN ('substantive', 'rename')"),
             "provinces": _group_provinces(conn, after,
                                           "AND change_subtype IN ('substantive', 'rename')"),
             "examples": _city_examples(conn, after, "substantive")},
            {"key": "csd_changed", "name": "Municipality (CSD) reassignments",
             "count": s["csd_changed"],
             "provinces": _group_provinces(conn, after, "AND change_type = 'csd_changed'"),
             "examples": []},
            {"key": "encoding", "name": "Encoding & normalization (noise)",
             "count": _count(conn, after, "change_type = 'city_changed' AND NOT " + _REAL_SQL),
             "provinces": _group_provinces(
                 conn, after, "AND change_type = 'city_changed' AND NOT " + _REAL_SQL),
             "examples": _city_examples(conn, after, "encoding")},
            {"key": "removed", "name": "Removals", "count": s["removed"],
             "provinces": _group_provinces(conn, after, "AND change_type = 'removed'"),
             "examples": []},
        ]

        detail = {
            "date": after,
            "before": s["before"],
            "label": s["label"],
            "range": s["range"],
            "active_before": s["active_before"],
            "active_after": s["active_after"],
            "metrics": {
                "added": s["added"], "removed": s["removed"],
                "csd_changed": s["csd_changed"],
                "city_real": s["city_real"], "city_noise": s["city_noise"],
            },
            "provinces": [
                {"p": r["p"], "name": PROVINCE_ABBR_TO_NAME.get(r["p"], r["p"]),
                 "added": r["added"] or 0, "removed": r["removed"] or 0,
                 "csd": r["csd"] or 0, "creal": r["creal"] or 0, "cnoise": r["cnoise"] or 0}
                for r in provinces
            ],
            "top_fsa": [{"fsa": r["fsa"], "p": r["p"], "n": r["n"]} for r in top_fsa],
            "fsa": fsa_counts,
            "groups": groups,
        }
        _write_json(f"snapshot/{after}.json", detail)


def _count(conn, after: str, cond: str) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) AS n FROM postal_code_changes "
        f"WHERE source_type = 'merged' AND snapshot_after = ? AND {cond}",
        (after,),
    ).fetchone()
    return row["n"]
