"""CLI entry point for the postal code change tracking system."""

import json
import logging
import sys

import click

from src import db
from src.config import NAR_SNAPSHOT_ORDER, NAR_SNAPSHOTS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
def cli(verbose: bool) -> None:
    """Canadian Postal Code Change Tracking System."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


# ── download ─────────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["nar", "geocoder", "geonames", "all"]),
    default="all",
    help="Data source to download",
)
@click.option("--period", help="Specific NAR period (e.g., 2022, 2024-06)")
def download(source: str, period: str | None) -> None:
    """Download data files from source websites."""
    from src.downloader import download_all_nar, download_geocoder, download_geonames, download_nar

    db.init_db()

    if source in ("nar", "all"):
        if period:
            click.echo(f"Downloading NAR {period} ...")
            download_nar(period)
        else:
            click.echo("Downloading all NAR snapshots ...")
            download_all_nar()

    if source in ("geocoder", "all"):
        click.echo("Downloading Geocoder.ca ...")
        download_geocoder()

    if source in ("geonames", "all"):
        click.echo("Downloading GeoNames ...")
        download_geonames()

    click.echo("Download complete.")


# ── process ──────────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["nar", "geocoder", "geonames", "all"]),
    default="all",
    help="Data source to process",
)
@click.option("--period", help="Specific NAR period to process")
@click.option("--force", is_flag=True, help="Reprocess even if already done")
def process(source: str, period: str | None, force: bool) -> None:
    """Parse downloaded files and load into database."""
    from src.parser_geocoder import process_geocoder
    from src.parser_geonames import process_geonames
    from src.parser_nar import process_all_nar, process_nar_snapshot

    db.init_db()

    if source in ("nar", "all"):
        if period:
            click.echo(f"Processing NAR {period} ...")
            count = process_nar_snapshot(period, force=force)
            click.echo(f"  → {count} unique postal codes")
        else:
            click.echo("Processing all NAR snapshots ...")
            results = process_all_nar(force=force)
            for p, c in results.items():
                click.echo(f"  {p}: {c} postal codes")

    if source in ("geocoder", "all"):
        click.echo("Processing Geocoder.ca ...")
        count = process_geocoder(force=force)
        click.echo(f"  → {count} postal codes")

    if source in ("geonames", "all"):
        click.echo("Processing GeoNames ...")
        count = process_geonames(force=force)
        click.echo(f"  → {count} postal codes")

    click.echo("Processing complete.")


# ── diff ─────────────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["nar", "geocoder", "all"]),
    default="nar",
    help="Source to diff",
)
@click.option("--from", "from_date", help="Earlier snapshot date")
@click.option("--to", "to_date", help="Later snapshot date")
def diff(source: str, from_date: str | None, to_date: str | None) -> None:
    """Run change detection between consecutive snapshots."""
    from src.differ import diff_all_pairs, diff_snapshots, store_changes

    sources = [source] if source != "all" else ["nar", "geocoder"]

    for src_type in sources:
        if from_date and to_date:
            click.echo(f"Diffing {src_type} {from_date} → {to_date} ...")
            changes = diff_snapshots(src_type, from_date, to_date)
            count = store_changes(changes)
            click.echo(f"  → {count} changes detected")
        else:
            click.echo(f"Diffing all {src_type} snapshot pairs ...")
            results = diff_all_pairs(src_type)
            for label, count in results.items():
                click.echo(f"  {label}: {count} changes")

    # Rebuild summary
    click.echo("Rebuilding summary table ...")
    summary_count = db.rebuild_summary()
    click.echo(f"  → {summary_count} postal codes in summary")


# ── reprocess ────────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["nar", "geocoder", "geonames", "all"]),
    default="all",
)
@click.option("--rebuild-db", is_flag=True, help="Drop and recreate all tables")
def reprocess(source: str, rebuild_db: bool) -> None:
    """Delete processed data and re-run from raw files."""
    if rebuild_db:
        click.echo("Dropping and recreating database ...")
        db.drop_and_recreate()
    else:
        if source in ("nar", "all"):
            db.clear_snapshots("nar")
            db.clear_changes("nar")
        if source in ("geocoder", "all"):
            db.clear_snapshots("geocoder")
            db.clear_changes("geocoder")
        if source in ("geonames", "all"):
            db.clear_snapshots("geonames")

    # Re-process
    from src.parser_geocoder import process_geocoder
    from src.parser_geonames import process_geonames
    from src.parser_nar import process_all_nar

    if source in ("nar", "all"):
        click.echo("Reprocessing NAR ...")
        process_all_nar(force=True)

    if source in ("geocoder", "all"):
        click.echo("Reprocessing Geocoder.ca ...")
        process_geocoder(force=True)

    if source in ("geonames", "all"):
        click.echo("Reprocessing GeoNames ...")
        process_geonames(force=True)

    # Re-diff
    from src.differ import diff_all_pairs

    if source in ("nar", "all"):
        diff_all_pairs("nar")
    if source in ("geocoder", "all"):
        diff_all_pairs("geocoder")

    # Rebuild summary
    count = db.rebuild_summary()
    click.echo(f"Reprocess complete. Summary: {count} postal codes.")


# ── refresh ──────────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["nar", "geocoder", "all"]),
    default="all",
)
def refresh(source: str) -> None:
    """Check for new data, download, process, and diff."""
    from src.differ import diff_all_pairs
    from src.downloader import download_all_nar, download_geocoder
    from src.parser_geocoder import process_geocoder
    from src.parser_nar import process_all_nar

    db.init_db()

    if source in ("nar", "all"):
        click.echo("Checking for NAR updates ...")
        download_all_nar()
        click.echo("Processing new NAR data ...")
        process_all_nar(force=False)
        click.echo("Running NAR diffs ...")
        diff_all_pairs("nar")

    if source in ("geocoder", "all"):
        click.echo("Checking Geocoder.ca ...")
        download_geocoder()
        click.echo("Processing Geocoder.ca ...")
        process_geocoder(force=False)
        click.echo("Running Geocoder.ca diffs ...")
        diff_all_pairs("geocoder")

    count = db.rebuild_summary()
    click.echo(f"Refresh complete. Summary: {count} postal codes.")


# ── serve ────────────────────────────────────────────────────────────────────


@cli.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", default=8080, type=int, help="Port to listen on")
def serve(host: str, port: int) -> None:
    """Start the web visualization server."""
    import uvicorn

    db.init_db()
    click.echo(f"Starting server at http://{host}:{port}")
    uvicorn.run("src.web.app:app", host=host, port=port, reload=True)


# ── stats ────────────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["nar", "geocoder", "geonames", "all"]),
    default="all",
)
def stats(source: str) -> None:
    """Print summary statistics."""
    db.init_db()
    conn = db.get_connection()

    sources = [source] if source != "all" else ["nar", "geocoder", "geonames"]

    for src_type in sources:
        # Snapshot counts
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT snapshot_date) AS n_snapshots,
                   COUNT(DISTINCT postal_code) AS n_codes
            FROM postal_code_snapshots WHERE source_type = ?
            """,
            (src_type,),
        ).fetchone()

        if row["n_snapshots"] == 0:
            continue

        click.echo(f"\n=== {src_type.upper()} ===")
        click.echo(f"  Snapshots: {row['n_snapshots']}")
        click.echo(f"  Unique postal codes (across all snapshots): {row['n_codes']}")

        # Per-snapshot counts
        rows = conn.execute(
            """
            SELECT snapshot_date, COUNT(*) AS n
            FROM postal_code_snapshots
            WHERE source_type = ?
            GROUP BY snapshot_date ORDER BY snapshot_date
            """,
            (src_type,),
        ).fetchall()
        for r in rows:
            click.echo(f"    {r['snapshot_date']}: {r['n']:,} postal codes")

        # Change counts
        rows = conn.execute(
            """
            SELECT change_type, COUNT(*) AS n
            FROM postal_code_changes
            WHERE source_type = ?
            GROUP BY change_type ORDER BY n DESC
            """,
            (src_type,),
        ).fetchall()
        if rows:
            click.echo("  Changes:")
            for r in rows:
                click.echo(f"    {r['change_type']}: {r['n']:,}")

    # Summary table
    row = conn.execute(
        "SELECT COUNT(*) AS n, SUM(is_active) AS active FROM postal_code_summary"
    ).fetchone()
    if row["n"]:
        click.echo(f"\n=== SUMMARY ===")
        click.echo(f"  Total unique postal codes: {row['n']:,}")
        click.echo(f"  Currently active: {row['active']:,}")

    conn.close()


# ── export ───────────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["csv", "json"]),
    default="csv",
    help="Output format",
)
@click.option("--output", "-o", type=click.Path(), help="Output file path")
@click.option("--source", default="nar", help="Source type to export")
def export(fmt: str, output: str | None, source: str) -> None:
    """Export change data to CSV or JSON."""
    import pandas as pd

    conn = db.get_connection()
    df = pd.read_sql(
        """
        SELECT postal_code, change_type, source_type,
               snapshot_before, snapshot_after,
               old_value, new_value, province_abbr, fsa
        FROM postal_code_changes
        WHERE source_type = ?
        ORDER BY snapshot_after, change_type, postal_code
        """,
        conn,
        params=(source,),
    )
    conn.close()

    if df.empty:
        click.echo("No changes to export.")
        return

    if output is None:
        output = f"postal_code_changes_{source}.{fmt}"

    if fmt == "csv":
        df.to_csv(output, index=False)
    else:
        df.to_json(output, orient="records", indent=2)

    click.echo(f"Exported {len(df)} changes to {output}")


if __name__ == "__main__":
    cli()
