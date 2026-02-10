"""CLI entry point for the postal code change tracking system."""

import json
import logging
import sys
import time

import click

from src import db
from src.config import NAR_SNAPSHOT_ORDER, NAR_SNAPSHOTS
from src.progress_tracker import tracker

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

    # Update progress tracker
    tracker.update_overall_status("Download", "In Progress")
    
    start_time = time.time()
    db.init_db()

    if source in ("nar", "all"):
        if period:
            click.echo(f"Downloading NAR {period} ...")
            download_start = time.time()
            download_nar(period)
            download_duration = time.time() - download_start
            tracker.mark_file_downloaded("NAR", period)
            click.echo(f"  -> NAR {period} downloaded in {download_duration:.2f}s")
        else:
            click.echo("Downloading all NAR snapshots ...")
            download_start = time.time()
            download_all_nar()
            download_duration = time.time() - download_start
            # Mark all NAR periods as downloaded
            from src.config import NAR_SNAPSHOTS
            for nar_period in NAR_SNAPSHOTS.keys():
                tracker.mark_file_downloaded("NAR", nar_period)
            click.echo(f"  -> All NAR snapshots downloaded in {download_duration:.2f}s")

    if source in ("geocoder", "all"):
        click.echo("Downloading Geocoder.ca ...")
        download_start = time.time()
        download_geocoder()
        download_duration = time.time() - download_start
        tracker.mark_file_downloaded("Geocoder", "Latest")
        click.echo(f"  -> Geocoder.ca downloaded in {download_duration:.2f}s")

    if source in ("geonames", "all"):
        click.echo("Downloading GeoNames ...")
        download_start = time.time()
        download_geonames()
        download_duration = time.time() - download_start
        tracker.mark_file_downloaded("GeoNames", "CA_full")
        click.echo(f"  -> GeoNames downloaded in {download_duration:.2f}s")

    total_duration = time.time() - start_time
    tracker.update_overall_status("Download", "Completed")
    click.echo(f"Download complete. Total time: {total_duration:.2f}s.")


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

    # Update progress tracker
    tracker.update_overall_status("Process", "In Progress")
    
    start_time = time.time()
    db.init_db()

    if source in ("nar", "all"):
        if period:
            click.echo(f"Processing NAR {period} ...")
            process_start = time.time()
            count = process_nar_snapshot(period, force=force)
            process_duration = time.time() - process_start
            # Mark the file as processed with details
            processed_file = f"nar_{period}_unique.parquet"
            tracker.mark_file_processed("NAR", period, processed_file=processed_file, size=f"{count} postal codes")
            click.echo(f"  -> {count} unique postal codes processed in {process_duration:.2f}s")
        else:
            click.echo("Processing all NAR snapshots ...")
            process_start = time.time()
            results = process_all_nar(force=force)
            process_duration = time.time() - process_start
            total_count = sum(results.values())
            for p, c in results.items():
                processed_file = f"nar_{p}_unique.parquet"
                tracker.mark_file_processed("NAR", p, processed_file=processed_file, size=f"{c} postal codes")
                click.echo(f"  {p}: {c} postal codes processed")
            click.echo(f"  -> Total: {total_count} postal codes in {process_duration:.2f}s")

    if source in ("geocoder", "all"):
        click.echo("Processing Geocoder.ca ...")
        process_start = time.time()
        count = process_geocoder(force=force)
        process_duration = time.time() - process_start
        tracker.mark_file_processed("Geocoder", "Latest", processed_file="geocoder_unique.parquet", size=f"{count} postal codes")
        click.echo(f"  -> {count} postal codes processed in {process_duration:.2f}s")

    if source in ("geonames", "all"):
        click.echo("Processing GeoNames ...")
        process_start = time.time()
        count = process_geonames(force=force)
        process_duration = time.time() - process_start
        tracker.mark_file_processed("GeoNames", "CA_full", processed_file="geonames_unique.parquet", size=f"{count} postal codes")
        click.echo(f"  -> {count} postal codes processed in {process_duration:.2f}s")

    total_duration = time.time() - start_time
    tracker.update_overall_status("Process", "Completed")
    click.echo(f"Processing complete. Total time: {total_duration:.2f}s.")


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

    # Update progress tracker
    tracker.update_overall_status("Diff", "In Progress")

    start_time = time.time()
    sources = [source] if source != "all" else ["nar", "geocoder"]

    for src_type in sources:
        if from_date and to_date:
            click.echo(f"Diffing {src_type} {from_date} -> {to_date} ...")
            diff_start = time.time()
            changes = diff_snapshots(src_type, from_date, to_date)
            count = store_changes(changes)
            diff_duration = time.time() - diff_start
            click.echo(f"  -> {count} changes detected in {diff_duration:.2f}s")
        else:
            click.echo(f"Diffing all {src_type} snapshot pairs ...")
            diff_start = time.time()
            results = diff_all_pairs(src_type)
            diff_duration = time.time() - diff_start
            total_changes = sum(results.values())
            for label, count in results.items():
                click.echo(f"  {label}: {count} changes")
            click.echo(f"  -> Total: {total_changes} changes in {diff_duration:.2f}s")

    # Rebuild summary
    click.echo("Rebuilding summary table ...")
    summary_start = time.time()
    summary_count = db.rebuild_summary()
    summary_duration = time.time() - summary_start
    tracker.add_note(f"Summary rebuilt with {summary_count} postal codes (took {summary_duration:.2f}s)")

    # Update diff status
    tracker.update_pipeline_stage_status("Diff", "Complete")

    total_duration = time.time() - start_time
    tracker.update_overall_status("Diff", "Completed")
    click.echo(f"  -> {summary_count} postal codes in summary")
    click.echo(f"Diff complete. Total time: {total_duration:.2f}s.")


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
    # Update progress tracker
    tracker.update_overall_status("Reprocess", "In Progress")
    
    start_time = time.time()
    
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
        tracker.log_processing_operation("NAR", "reprocess", "Started")
        process_start = time.time()
        process_all_nar(force=True)
        process_duration = time.time() - process_start
        tracker.log_processing_operation("NAR", "reprocess", "Completed", duration=process_duration)

    if source in ("geocoder", "all"):
        click.echo("Reprocessing Geocoder.ca ...")
        tracker.log_processing_operation("Geocoder.ca", "reprocess", "Started")
        process_start = time.time()
        process_geocoder(force=True)
        process_duration = time.time() - process_start
        tracker.log_processing_operation("Geocoder.ca", "reprocess", "Completed", duration=process_duration)

    if source in ("geonames", "all"):
        click.echo("Reprocessing GeoNames ...")
        tracker.log_processing_operation("GeoNames", "reprocess", "Started")
        process_start = time.time()
        process_geonames(force=True)
        process_duration = time.time() - process_start
        tracker.log_processing_operation("GeoNames", "reprocess", "Completed", duration=process_duration)

    # Re-diff
    from src.differ import diff_all_pairs

    click.echo("Re-running diffs ...")
    diff_start = time.time()
    if source in ("nar", "all"):
        diff_all_pairs("nar")
    if source in ("geocoder", "all"):
        diff_all_pairs("geocoder")
    diff_duration = time.time() - diff_start
    click.echo(f"  -> Diffs completed in {diff_duration:.2f}s")

    # Rebuild summary
    click.echo("Rebuilding summary ...")
    summary_start = time.time()
    count = db.rebuild_summary()
    summary_duration = time.time() - summary_start
    tracker.add_note(f"Summary rebuilt with {count} postal codes (took {summary_duration:.2f}s)")

    # Update diff status
    tracker.update_pipeline_stage_status("Diff", "Complete")

    total_duration = time.time() - start_time
    tracker.update_overall_status("Reprocess", "Completed")
    click.echo(f"Reprocess complete. Summary: {count} postal codes. Total time: {total_duration:.2f}s.")


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

    # Update progress tracker
    tracker.update_overall_status("Refresh", "In Progress")
    
    start_time = time.time()
    db.init_db()

    if source in ("nar", "all"):
        click.echo("Checking for NAR updates ...")
        download_start = time.time()
        download_all_nar()
        download_duration = time.time() - download_start
        # Mark NAR files as downloaded
        from src.config import NAR_SNAPSHOTS
        for nar_period in NAR_SNAPSHOTS.keys():
            tracker.mark_file_downloaded("NAR", nar_period)
        click.echo(f"  -> NAR updates checked in {download_duration:.2f}s")
        
        click.echo("Processing new NAR data ...")
        process_start = time.time()
        results = process_all_nar(force=False)
        process_duration = time.time() - process_start
        # Mark NAR files as processed
        for p, c in results.items():
            processed_file = f"nar_{p}_unique.parquet"
            tracker.mark_file_processed("NAR", p, processed_file=processed_file, size=f"{c} postal codes")
        click.echo(f"  -> NAR data processed in {process_duration:.2f}s")
        
        click.echo("Running NAR diffs ...")
        diff_start = time.time()
        diff_all_pairs("nar")
        diff_duration = time.time() - diff_start
        click.echo(f"  -> NAR diffs completed in {diff_duration:.2f}s")

    if source in ("geocoder", "all"):
        click.echo("Checking Geocoder.ca ...")
        download_start = time.time()
        download_geocoder()
        download_duration = time.time() - download_start
        tracker.mark_file_downloaded("Geocoder", "Latest")
        click.echo(f"  -> Geocoder.ca checked in {download_duration:.2f}s")
        
        click.echo("Processing Geocoder.ca ...")
        process_start = time.time()
        count = process_geocoder(force=False)
        process_duration = time.time() - process_start
        tracker.mark_file_processed("Geocoder", "Latest", processed_file="geocoder_unique.parquet", size=f"{count} postal codes")
        click.echo(f"  -> Geocoder.ca processed in {process_duration:.2f}s")
        
        click.echo("Running Geocoder.ca diffs ...")
        diff_start = time.time()
        diff_all_pairs("geocoder")
        diff_duration = time.time() - diff_start
        click.echo(f"  -> Geocoder.ca diffs completed in {diff_duration:.2f}s")

    summary_start = time.time()
    count = db.rebuild_summary()
    summary_duration = time.time() - summary_start
    tracker.add_note(f"Summary rebuilt with {count} postal codes (took {summary_duration:.2f}s)")

    # Update pipeline stages
    tracker.update_pipeline_stage_status("Diff", "Complete")

    total_duration = time.time() - start_time
    tracker.update_overall_status("Refresh", "Completed")
    click.echo(f"Refresh complete. Summary: {count} postal codes. Total time: {total_duration:.2f}s.")


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
        
        # Update database status in progress tracker
        changes_row = conn.execute("SELECT COUNT(*) AS n FROM postal_code_changes").fetchone()
        total_changes = changes_row["n"] if changes_row["n"] else 0
        
        tracker.update_database_status(
            db_path=db.DB_PATH,
            total_postal_codes=row['n'],
            total_changes=total_changes
        )

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

    # Update progress tracker
    tracker.update_overall_status("Export", "In Progress")
    
    start_time = time.time()
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
        tracker.add_note(f"No changes to export for {source} source")
        tracker.update_overall_status("Export", "Completed - No Data")
        return

    if output is None:
        output = f"postal_code_changes_{source}.{fmt}"

    export_start = time.time()
    if fmt == "csv":
        df.to_csv(output, index=False)
    else:
        df.to_json(output, orient="records", indent=2)
    export_duration = time.time() - export_start

    total_duration = time.time() - start_time
    tracker.add_note(f"Exported {len(df)} changes to {output} (took {export_duration:.2f}s)")
    tracker.update_overall_status("Export", "Completed")
    click.echo(f"Exported {len(df)} changes to {output}. Total time: {total_duration:.2f}s.")


if __name__ == "__main__":
    cli()
