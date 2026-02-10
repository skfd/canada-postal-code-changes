"""SQLite database schema, connection helpers, and common queries."""

import sqlite3
from pathlib import Path

from src.config import DB_PATH

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS data_sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type     TEXT NOT NULL,
    reference_date  TEXT NOT NULL,
    download_url    TEXT,
    downloaded_at   TEXT,
    file_path       TEXT,
    processed_at    TEXT,
    row_count       INTEGER,
    unique_pc_count INTEGER,
    UNIQUE(source_type, reference_date)
);

CREATE TABLE IF NOT EXISTS postal_code_snapshots (
    postal_code     TEXT NOT NULL,
    snapshot_date   TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    province_abbr   TEXT,
    city_name       TEXT,
    latitude        REAL,
    longitude       REAL,
    csd_code        TEXT,
    address_count   INTEGER,
    fsa             TEXT GENERATED ALWAYS AS (SUBSTR(postal_code, 1, 3)) STORED,
    is_rural        INTEGER GENERATED ALWAYS AS (
        CASE WHEN SUBSTR(postal_code, 2, 1) = '0' THEN 1 ELSE 0 END
    ) STORED,
    PRIMARY KEY (postal_code, snapshot_date, source_type)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_fsa
    ON postal_code_snapshots(fsa);
CREATE INDEX IF NOT EXISTS idx_snapshots_province
    ON postal_code_snapshots(province_abbr);
CREATE INDEX IF NOT EXISTS idx_snapshots_date
    ON postal_code_snapshots(snapshot_date);

CREATE TABLE IF NOT EXISTS postal_code_changes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    postal_code     TEXT NOT NULL,
    change_type     TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    snapshot_before TEXT NOT NULL,
    snapshot_after  TEXT NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    province_abbr   TEXT,
    fsa             TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_changes_type
    ON postal_code_changes(change_type);
CREATE INDEX IF NOT EXISTS idx_changes_fsa
    ON postal_code_changes(fsa);
CREATE INDEX IF NOT EXISTS idx_changes_province
    ON postal_code_changes(province_abbr);
CREATE INDEX IF NOT EXISTS idx_changes_dates
    ON postal_code_changes(snapshot_before, snapshot_after);

CREATE TABLE IF NOT EXISTS postal_code_summary (
    postal_code     TEXT PRIMARY KEY,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    is_active       INTEGER,
    province_abbr   TEXT,
    city_name       TEXT,
    latitude        REAL,
    longitude       REAL,
    fsa             TEXT,
    is_rural        INTEGER,
    total_changes   INTEGER DEFAULT 0,
    sources         TEXT
);
"""


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Return a connection with WAL mode and foreign keys enabled."""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path | None = None) -> None:
    """Create all tables and indexes if they don't exist."""
    conn = get_connection(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.close()


def drop_and_recreate(db_path: Path | None = None) -> None:
    """Drop all tables and recreate the schema."""
    conn = get_connection(db_path)
    conn.executescript("""
        DROP TABLE IF EXISTS postal_code_summary;
        DROP TABLE IF EXISTS postal_code_changes;
        DROP TABLE IF EXISTS postal_code_snapshots;
        DROP TABLE IF EXISTS data_sources;
    """)
    conn.executescript(SCHEMA_SQL)
    conn.close()


def record_download(
    source_type: str,
    reference_date: str,
    download_url: str,
    file_path: str,
    db_path: Path | None = None,
) -> None:
    """Record that a file was downloaded."""
    conn = get_connection(db_path)
    conn.execute(
        """
        INSERT OR REPLACE INTO data_sources
            (source_type, reference_date, download_url, downloaded_at, file_path)
        VALUES (?, ?, ?, datetime('now'), ?)
        """,
        (source_type, reference_date, download_url, file_path),
    )
    conn.commit()
    conn.close()


def mark_processed(
    source_type: str,
    reference_date: str,
    row_count: int,
    unique_pc_count: int,
    db_path: Path | None = None,
) -> None:
    """Mark a data source as processed with counts."""
    conn = get_connection(db_path)
    conn.execute(
        """
        UPDATE data_sources
        SET processed_at = datetime('now'),
            row_count = ?,
            unique_pc_count = ?
        WHERE source_type = ? AND reference_date = ?
        """,
        (row_count, unique_pc_count, source_type, reference_date),
    )
    conn.commit()
    conn.close()


def get_unprocessed_sources(
    source_type: str | None = None,
    db_path: Path | None = None,
) -> list[dict]:
    """Return data sources that have been downloaded but not processed."""
    conn = get_connection(db_path)
    sql = """
        SELECT source_type, reference_date, file_path
        FROM data_sources
        WHERE downloaded_at IS NOT NULL AND processed_at IS NULL
    """
    params: list = []
    if source_type:
        sql += " AND source_type = ?"
        params.append(source_type)
    sql += " ORDER BY reference_date"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_snapshot_dates(
    source_type: str = "nar",
    db_path: Path | None = None,
) -> list[str]:
    """Return sorted list of snapshot dates for a source type."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """
        SELECT DISTINCT snapshot_date
        FROM postal_code_snapshots
        WHERE source_type = ?
        ORDER BY snapshot_date
        """,
        (source_type,),
    ).fetchall()
    conn.close()
    return [r["snapshot_date"] for r in rows]


def clear_changes(
    source_type: str | None = None,
    db_path: Path | None = None,
) -> None:
    """Delete change records, optionally filtered by source."""
    conn = get_connection(db_path)
    if source_type:
        conn.execute(
            "DELETE FROM postal_code_changes WHERE source_type = ?",
            (source_type,),
        )
    else:
        conn.execute("DELETE FROM postal_code_changes")
    conn.commit()
    conn.close()


def clear_snapshots(
    source_type: str | None = None,
    db_path: Path | None = None,
) -> None:
    """Delete snapshot records, optionally filtered by source."""
    conn = get_connection(db_path)
    if source_type:
        conn.execute(
            "DELETE FROM postal_code_snapshots WHERE source_type = ?",
            (source_type,),
        )
    else:
        conn.execute("DELETE FROM postal_code_snapshots")
    conn.commit()
    conn.close()


def clear_merged_data(db_path: Path | None = None) -> None:
    """Delete all merged snapshots and changes (rebuilt from real sources)."""
    conn = get_connection(db_path)
    conn.execute("DELETE FROM postal_code_snapshots WHERE source_type = 'merged'")
    conn.execute("DELETE FROM postal_code_changes WHERE source_type = 'merged'")
    conn.commit()
    conn.close()


def rebuild_summary(db_path: Path | None = None) -> int:
    """Rebuild the postal_code_summary table from snapshots and changes.

    Uses merged snapshots/changes if available, otherwise falls back to
    all source types. Returns the number of rows inserted.
    """
    conn = get_connection(db_path)

    # Check if merged data exists
    has_merged = conn.execute(
        "SELECT 1 FROM postal_code_snapshots WHERE source_type = 'merged' LIMIT 1"
    ).fetchone()

    # Use merged for aggregation and changes if available
    snapshot_filter = "source_type = 'merged'" if has_merged else "1=1"
    change_filter = "source_type = 'merged'" if has_merged else "1=1"

    # Get the latest snapshot date (across all sources for is_active)
    row = conn.execute(
        "SELECT MAX(snapshot_date) AS max_date FROM postal_code_snapshots"
    ).fetchone()
    max_date = row["max_date"] if row else None

    if not max_date:
        conn.close()
        return 0

    conn.execute("DELETE FROM postal_code_summary")
    conn.execute(
        f"""
        WITH latest AS (
            SELECT
                postal_code,
                province_abbr,
                city_name,
                latitude,
                longitude,
                ROW_NUMBER() OVER (
                    PARTITION BY postal_code
                    ORDER BY snapshot_date DESC
                ) AS rn
            FROM postal_code_snapshots
            WHERE {snapshot_filter}
        ),
        agg AS (
            SELECT
                postal_code,
                MIN(snapshot_date) AS first_seen,
                MAX(snapshot_date) AS last_seen,
                CASE WHEN MAX(snapshot_date) = ? THEN 1 ELSE 0 END AS is_active,
                GROUP_CONCAT(DISTINCT source_type) AS sources
            FROM postal_code_snapshots
            WHERE {snapshot_filter}
            GROUP BY postal_code
        ),
        real_sources AS (
            SELECT postal_code, GROUP_CONCAT(DISTINCT source_type) AS sources
            FROM postal_code_snapshots
            WHERE source_type != 'merged'
            GROUP BY postal_code
        ),
        change_counts AS (
            SELECT postal_code, COUNT(*) AS change_count
            FROM postal_code_changes
            WHERE {change_filter}
            GROUP BY postal_code
        )
        INSERT INTO postal_code_summary
            (postal_code, first_seen, last_seen, is_active,
             province_abbr, city_name, latitude, longitude,
             fsa, is_rural, total_changes, sources)
        SELECT
            a.postal_code,
            a.first_seen,
            a.last_seen,
            a.is_active,
            l.province_abbr,
            l.city_name,
            l.latitude,
            l.longitude,
            SUBSTR(a.postal_code, 1, 3),
            CASE WHEN SUBSTR(a.postal_code, 2, 1) = '0' THEN 1 ELSE 0 END,
            COALESCE(c.change_count, 0),
            COALESCE(rs.sources, a.sources)
        FROM agg a
        JOIN latest l ON l.postal_code = a.postal_code AND l.rn = 1
        LEFT JOIN change_counts c ON c.postal_code = a.postal_code
        LEFT JOIN real_sources rs ON rs.postal_code = a.postal_code
        """,
        (max_date,),
    )
    count = conn.execute("SELECT COUNT(*) AS n FROM postal_code_summary").fetchone()[
        "n"
    ]
    conn.commit()
    conn.close()
    return count
