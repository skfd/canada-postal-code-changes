"""API routes for the postal code change tracker."""

from fastapi import APIRouter, Query

from src import db

router = APIRouter()


@router.get("/snapshots")
def list_snapshots():
    """List all snapshots with metadata."""
    conn = db.get_connection()
    rows = conn.execute(
        """
        SELECT source_type, snapshot_date, COUNT(*) AS postal_code_count
        FROM postal_code_snapshots
        GROUP BY source_type, snapshot_date
        ORDER BY source_type, snapshot_date
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/stats")
def get_stats(
    source: str = Query("nar", description="Source type"),
    province: str | None = Query(None, description="Province abbreviation"),
):
    """Aggregate statistics."""
    conn = db.get_connection()

    # Total postal codes and snapshots
    params: list = [source]
    where = "WHERE source_type = ?"
    if province:
        where += " AND province_abbr = ?"
        params.append(province)

    overview = conn.execute(
        f"""
        SELECT COUNT(DISTINCT postal_code) AS total_codes,
               COUNT(DISTINCT snapshot_date) AS total_snapshots
        FROM postal_code_snapshots {where}
        """,
        params,
    ).fetchone()

    # Change counts by type
    change_params: list = [source]
    change_where = "WHERE source_type = ?"
    if province:
        change_where += " AND province_abbr = ?"
        change_params.append(province)

    change_rows = conn.execute(
        f"""
        SELECT change_type, COUNT(*) AS count
        FROM postal_code_changes {change_where}
        GROUP BY change_type ORDER BY count DESC
        """,
        change_params,
    ).fetchall()

    # Per-province counts (latest snapshot)
    latest = conn.execute(
        "SELECT MAX(snapshot_date) FROM postal_code_snapshots WHERE source_type = ?",
        (source,),
    ).fetchone()[0]

    province_rows = []
    if latest:
        prov_params: list = [source, latest]
        prov_where = "WHERE source_type = ? AND snapshot_date = ?"
        if province:
            prov_where += " AND province_abbr = ?"
            prov_params.append(province)

        province_rows = conn.execute(
            f"""
            SELECT province_abbr, COUNT(*) AS count
            FROM postal_code_snapshots {prov_where}
            GROUP BY province_abbr ORDER BY count DESC
            """,
            prov_params,
        ).fetchall()

    conn.close()

    return {
        "total_codes": overview["total_codes"],
        "total_snapshots": overview["total_snapshots"],
        "changes_by_type": {r["change_type"]: r["count"] for r in change_rows},
        "codes_by_province": {r["province_abbr"]: r["count"] for r in province_rows},
        "latest_snapshot": latest,
    }


@router.get("/changes")
def list_changes(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    change_type: str | None = None,
    province: str | None = None,
    fsa: str | None = None,
    source: str = "nar",
    from_date: str | None = None,
    to_date: str | None = None,
    search: str | None = None,
):
    """Paginated, filtered list of change events."""
    conn = db.get_connection()

    conditions = ["source_type = ?"]
    params: list = [source]

    if change_type:
        conditions.append("change_type = ?")
        params.append(change_type)
    if province:
        conditions.append("province_abbr = ?")
        params.append(province)
    if fsa:
        conditions.append("fsa = ?")
        params.append(fsa.upper())
    if from_date:
        conditions.append("snapshot_after >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("snapshot_after <= ?")
        params.append(to_date)
    if search:
        conditions.append("postal_code LIKE ?")
        params.append(f"%{search.upper().replace(' ', '')}%")

    where = "WHERE " + " AND ".join(conditions)

    # Count total
    count_row = conn.execute(
        f"SELECT COUNT(*) AS total FROM postal_code_changes {where}", params
    ).fetchone()
    total = count_row["total"]

    # Fetch page
    offset = (page - 1) * per_page
    rows = conn.execute(
        f"""
        SELECT postal_code, change_type, source_type,
               snapshot_before, snapshot_after,
               old_value, new_value, province_abbr, fsa
        FROM postal_code_changes {where}
        ORDER BY snapshot_after DESC, change_type, postal_code
        LIMIT ? OFFSET ?
        """,
        [*params, per_page, offset],
    ).fetchall()
    conn.close()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "items": [dict(r) for r in rows],
    }


@router.get("/changes/summary")
def changes_summary(
    change_type: str | None = None,
    source: str = "nar",
    from_date: str | None = None,
    to_date: str | None = None,
):
    """Change counts grouped by FSA (for choropleth map)."""
    conn = db.get_connection()

    conditions = ["source_type = ?"]
    params: list = [source]

    if change_type:
        conditions.append("change_type = ?")
        params.append(change_type)
    if from_date:
        conditions.append("snapshot_after >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("snapshot_after <= ?")
        params.append(to_date)

    where = "WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"""
        SELECT fsa, province_abbr, COUNT(*) AS change_count
        FROM postal_code_changes {where}
        GROUP BY fsa ORDER BY change_count DESC
        """,
        params,
    ).fetchall()
    conn.close()

    return [dict(r) for r in rows]


@router.get("/changes/timeline")
def changes_timeline(
    source: str = "nar",
    province: str | None = None,
):
    """Change counts grouped by snapshot pair and optionally province."""
    conn = db.get_connection()

    conditions = ["source_type = ?"]
    params: list = [source]
    if province:
        conditions.append("province_abbr = ?")
        params.append(province)

    where = "WHERE " + " AND ".join(conditions)

    # Changes per pair per type
    rows = conn.execute(
        f"""
        SELECT snapshot_before, snapshot_after, change_type,
               COUNT(*) AS count
        FROM postal_code_changes {where}
        GROUP BY snapshot_before, snapshot_after, change_type
        ORDER BY snapshot_after, change_type
        """,
        params,
    ).fetchall()

    # Active code count per snapshot
    snap_conditions = ["source_type = ?"]
    snap_params: list = [source]
    if province:
        snap_conditions.append("province_abbr = ?")
        snap_params.append(province)
    snap_where = "WHERE " + " AND ".join(snap_conditions)

    snap_rows = conn.execute(
        f"""
        SELECT snapshot_date, COUNT(*) AS active_count
        FROM postal_code_snapshots {snap_where}
        GROUP BY snapshot_date ORDER BY snapshot_date
        """,
        snap_params,
    ).fetchall()
    conn.close()

    return {
        "changes": [dict(r) for r in rows],
        "snapshots": [dict(r) for r in snap_rows],
    }


@router.get("/postal-code/{code}")
def postal_code_detail(code: str):
    """Full history for a single postal code."""
    code = code.upper().replace(" ", "")
    conn = db.get_connection()

    # All snapshots
    snapshots = conn.execute(
        """
        SELECT snapshot_date, source_type, province_abbr, city_name,
               latitude, longitude, csd_code, address_count
        FROM postal_code_snapshots
        WHERE postal_code = ?
        ORDER BY snapshot_date
        """,
        (code,),
    ).fetchall()

    # All changes
    changes = conn.execute(
        """
        SELECT change_type, source_type, snapshot_before, snapshot_after,
               old_value, new_value
        FROM postal_code_changes
        WHERE postal_code = ?
        ORDER BY snapshot_after
        """,
        (code,),
    ).fetchall()

    # Summary
    summary = conn.execute(
        "SELECT * FROM postal_code_summary WHERE postal_code = ?", (code,)
    ).fetchone()

    conn.close()

    return {
        "postal_code": code,
        "summary": dict(summary) if summary else None,
        "snapshots": [dict(r) for r in snapshots],
        "changes": [dict(r) for r in changes],
    }


@router.get("/fsa/{fsa}")
def fsa_detail(
    fsa: str,
    snapshot_date: str | None = None,
    source: str = "nar",
):
    """All postal codes and changes within an FSA."""
    fsa = fsa.upper()
    conn = db.get_connection()

    # Get latest snapshot if not specified
    if not snapshot_date:
        row = conn.execute(
            "SELECT MAX(snapshot_date) FROM postal_code_snapshots WHERE source_type = ?",
            (source,),
        ).fetchone()
        snapshot_date = row[0] if row else None

    codes = []
    if snapshot_date:
        codes = conn.execute(
            """
            SELECT postal_code, province_abbr, city_name, latitude, longitude,
                   address_count
            FROM postal_code_snapshots
            WHERE fsa = ? AND snapshot_date = ? AND source_type = ?
            ORDER BY postal_code
            """,
            (fsa, snapshot_date, source),
        ).fetchall()

    changes = conn.execute(
        """
        SELECT postal_code, change_type, snapshot_before, snapshot_after,
               old_value, new_value
        FROM postal_code_changes
        WHERE fsa = ? AND source_type = ?
        ORDER BY snapshot_after DESC, postal_code
        """,
        (fsa, source),
    ).fetchall()

    conn.close()

    return {
        "fsa": fsa,
        "snapshot_date": snapshot_date,
        "postal_codes": [dict(r) for r in codes],
        "changes": [dict(r) for r in changes],
        "total_codes": len(codes),
        "total_changes": len(changes),
    }


@router.get("/provinces")
def list_provinces(source: str = "nar"):
    """Province list with postal code counts from latest snapshot."""
    conn = db.get_connection()

    latest = conn.execute(
        "SELECT MAX(snapshot_date) FROM postal_code_snapshots WHERE source_type = ?",
        (source,),
    ).fetchone()[0]

    rows = []
    if latest:
        rows = conn.execute(
            """
            SELECT province_abbr, COUNT(*) AS code_count,
                   SUM(CASE WHEN is_rural = 1 THEN 1 ELSE 0 END) AS rural_count
            FROM postal_code_snapshots
            WHERE source_type = ? AND snapshot_date = ?
            GROUP BY province_abbr ORDER BY code_count DESC
            """,
            (source, latest),
        ).fetchall()

    conn.close()

    from src.config import PROVINCE_ABBR_TO_NAME

    return [
        {
            **dict(r),
            "province_name": PROVINCE_ABBR_TO_NAME.get(r["province_abbr"], ""),
        }
        for r in rows
    ]
