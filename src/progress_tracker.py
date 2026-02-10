"""
Progress tracking module for the Canadian Postal Code Change Tracking System.

This module provides functionality to automatically track and update progress
information during data processing operations.
"""

import datetime
import os
from pathlib import Path
from typing import Optional

PROGRESS_FILE_PATH = Path("PROCESSING_PROGRESS.md")


class ProgressTracker:
    """Tracks processing progress and updates the human-readable progress file."""

    def __init__(self, progress_file: Path = PROGRESS_FILE_PATH):
        self.progress_file = progress_file
        self.current_operation = None

    def _ensure_file_exists(self):
        """Ensure the progress file exists with basic template."""
        if not self.progress_file.exists():
            self.progress_file.write_text("""# Processing Progress Tracker

This file tracks the status of data processing operations for the Canadian Postal Code Change Tracking System.

## Overall Status
- Last Updated:
- Processing Pipeline:
- Status:

## Data Sources Status

### NAR (National Address Register) - Statistics Canada
| Period | Downloaded | Processed | Processed File | Size | Notes |
|--------|------------|-----------|----------------|------|-------|
| 2022 | Yes | Yes | nar_2022_unique.parquet | 8,229,111 bytes | Complete |
| 2023 | Yes | Yes | nar_2023_unique.parquet | 8,511,472 bytes | Complete |
| 2024-06 | Yes | No | | | Pending |
| 2024-12 | Yes | No | | | Pending |
| 2025-07 | Yes | No | | | Pending |
| 2025-12 | Yes | No | | | Pending |

### Geocoder.ca
| Period | Downloaded | Processed | Processed File | Size | Notes |
|--------|------------|-----------|----------------|------|-------|
| Latest | Yes | No | | | Pending |

### GeoNames
| Period | Downloaded | Processed | Processed File | Size | Notes |
|--------|------------|-----------|----------------|------|-------|
| CA_full | Yes | No | | | Pending |

## Database Status
- Database File: postal_codes.db
- Database Size: 247,164,928 bytes
- Snapshots Loaded:
- Total Postal Codes:
- Total Changes Detected:

## Processing Pipeline Status
- Download Status: Complete
- Processing Status: Partial (2022, 2023 processed; others pending)
- Diff Status: Pending
- Export Status: Pending

## Next Steps
- [ ] Process remaining NAR snapshots (2024-06, 2024-12, 2025-07, 2025-12)
- [ ] Process Geocoder.ca data
- [ ] Process GeoNames data
- [ ] Run diff operations between snapshots
- [ ] Export results

## Notes
-

""")

    def update_overall_status(self, operation: str, status: str):
        """Update the overall status section."""
        self._ensure_file_exists()

        content = self.progress_file.read_text()

        # Update Last Updated
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        content = self._replace_line(content, "- Last Updated:", f"- Last Updated: {now}")

        # Update Processing Pipeline
        content = self._replace_line(content, "- Processing Pipeline:", f"- Processing Pipeline: {operation}")

        # Update Status
        content = self._replace_line(content, "- Status:", f"- Status: {status}")

        self.progress_file.write_text(content)
        self.current_operation = operation

    def mark_file_downloaded(self, source: str, period: str):
        """Mark a file as downloaded in the progress file."""
        self._ensure_file_exists()

        content = self.progress_file.read_text()

        # Update the specific row in the appropriate table
        if source.lower() == "nar":
            # Update the NAR table
            content = self._update_table_row(content, "NAR", period, "Downloaded", "Yes")
        elif source.lower() == "geocoder":
            # Update the Geocoder.ca table
            content = self._update_table_row(content, "Geocoder.ca", "Latest", "Downloaded", "Yes")
        elif source.lower() == "geonames":
            # Update the GeoNames table
            content = self._update_table_row(content, "GeoNames", "CA_full", "Downloaded", "Yes")

        # Update the overall download status
        content = self._update_pipeline_status(content, "Download", "Complete")

        self.progress_file.write_text(content)

    def mark_file_processed(self, source: str, period: str, processed_file: str = "", size: str = ""):
        """Mark a file as processed in the progress file."""
        self._ensure_file_exists()

        content = self.progress_file.read_text()

        # Update the specific row in the appropriate table
        if source.lower() == "nar":
            # Update the NAR table
            content = self._update_table_row(content, "NAR", period, "Processed", "Yes")
            if processed_file:
                content = self._update_table_row(content, "NAR", period, "Processed File", processed_file)
            if size:
                content = self._update_table_row(content, "NAR", period, "Size", size)
        elif source.lower() == "geocoder":
            # Update the Geocoder.ca table
            content = self._update_table_row(content, "Geocoder.ca", "Latest", "Processed", "Yes")
            if processed_file:
                content = self._update_table_row(content, "Geocoder.ca", "Latest", "Processed File", processed_file)
            if size:
                content = self._update_table_row(content, "Geocoder.ca", "Latest", "Size", size)
        elif source.lower() == "geonames":
            # Update the GeoNames table
            content = self._update_table_row(content, "GeoNames", "CA_full", "Processed", "Yes")
            if processed_file:
                content = self._update_table_row(content, "GeoNames", "CA_full", "Processed File", processed_file)
            if size:
                content = self._update_table_row(content, "GeoNames", "CA_full", "Size", size)

        # Update the overall processing status
        content = self._update_pipeline_status(content, "Processing", self._get_processing_status(content))

        self.progress_file.write_text(content)

    def _update_table_row(self, content: str, table_name: str, period: str, column: str, value: str) -> str:
        """Update a specific cell in a table."""
        lines = content.split('\n')
        new_lines = []

        in_correct_table = False
        header_found = False
        header_columns = []

        for line in lines:
            new_lines.append(line)

            # Find the table header
            if f"### {table_name}" in line or (table_name in line and "###" in line):
                in_correct_table = True
                header_found = False
            elif in_correct_table and '|' in line and '----' not in line.lower():
                # This could be the header row
                if 'Period' in line and 'Downloaded' in line:
                    header_found = True
                    header_columns = [col.strip() for col in line.split('|')]
                elif header_found and period in line:
                    # This is the row for the specific period
                    parts = line.split('|')
                    if len(parts) >= len(header_columns):
                        # Find the column index
                        col_idx = -1
                        for i, h_col in enumerate(header_columns):
                            if column.strip() in h_col.strip():
                                col_idx = i
                                break

                        if col_idx != -1 and col_idx < len(parts):
                            parts[col_idx] = f" {value} "
                            new_lines[-1] = '|'.join(parts)
                    break
            elif '###' in line and table_name not in line:
                # Moving to a different table, reset
                in_correct_table = False
                header_found = False

        return '\n'.join(new_lines)

    def _update_pipeline_status(self, content: str, stage: str, status: str) -> str:
        """Update the status of a specific pipeline stage."""
        if stage == "Download":
            content = self._replace_line(content, "- Download Status:", f"- Download Status: {status}")
        elif stage == "Processing":
            content = self._replace_line(content, "- Processing Status:", f"- Processing Status: {status}")
        elif stage == "Diff":
            content = self._replace_line(content, "- Diff Status:", f"- Diff Status: {status}")
        elif stage == "Export":
            content = self._replace_line(content, "- Export Status:", f"- Export Status: {status}")

        return content

    def _get_processing_status(self, content: str) -> str:
        """Get the overall processing status based on individual file statuses."""
        # Count how many files are processed vs total
        lines = content.split('\n')

        nar_processed = 0
        nar_total = 0
        geocoder_processed = 0
        geocoder_total = 0
        geonames_processed = 0
        geonames_total = 0

        in_nar_table = False
        in_geocoder_table = False
        in_geonames_table = False

        for line in lines:
            if "### NAR" in line:
                in_nar_table = True
                in_geocoder_table = False
                in_geonames_table = False
            elif "### Geocoder.ca" in line:
                in_nar_table = False
                in_geocoder_table = True
                in_geonames_table = False
            elif "### GeoNames" in line:
                in_nar_table = False
                in_geocoder_table = False
                in_geonames_table = True
            elif '|' in line and '----' not in line and 'Period' not in line:
                # This is a data row
                if in_nar_table and '|' in line:
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 3:  # At least Period | Downloaded | Processed
                        nar_total += 1
                        if len(parts) > 2 and 'Yes' in parts[2]:  # Processed column
                            nar_processed += 1
                elif in_geocoder_table and '|' in line:
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 3:
                        geocoder_total += 1
                        if len(parts) > 2 and 'Yes' in parts[2]:
                            geocoder_processed += 1
                elif in_geonames_table and '|' in line:
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 3:
                        geonames_total += 1
                        if len(parts) > 2 and 'Yes' in parts[2]:
                            geonames_processed += 1

        # Determine overall status
        total_files = nar_total + geocoder_total + geonames_total
        processed_files = nar_processed + geocoder_processed + geonames_processed

        if processed_files == 0:
            return "Not Started"
        elif processed_files == total_files:
            return "Complete"
        else:
            return f"Partial ({processed_files}/{total_files} files)"

    def update_database_status(self, db_path: Optional[Path] = None,
                              total_postal_codes: Optional[int] = None,
                              total_changes: Optional[int] = None):
        """Update the database status section."""
        self._ensure_file_exists()

        content = self.progress_file.read_text()

        if db_path:
            content = self._replace_line(content, "- Database File:", f"- Database File: {db_path.name}")
            # Also update size if we can get it
            if db_path.exists():
                size = db_path.stat().st_size
                content = self._replace_line(content, "- Database Size:", f"- Database Size: {size} bytes")

        if total_postal_codes is not None:
            content = self._replace_line(content, "- Total Postal Codes:", f"- Total Postal Codes: {total_postal_codes}")

        if total_changes is not None:
            content = self._replace_line(content, "- Total Changes Detected:", f"- Total Changes Detected: {total_changes}")

        self.progress_file.write_text(content)

    def update_pipeline_stage_status(self, stage: str, status: str):
        """Update the status of a specific pipeline stage."""
        self._ensure_file_exists()

        content = self.progress_file.read_text()
        content = self._update_pipeline_status(content, stage, status)

        self.progress_file.write_text(content)

    def update_data_source_status(self, source: str, latest_snapshot: Optional[str] = None,
                                 record_count: Optional[int] = None,
                                 last_processed: Optional[str] = None, status: Optional[str] = None):
        """Update the status of a data source by adding a note."""
        self._ensure_file_exists()

        # Create a note about the data source status
        note_parts = [f"{source} source:"]
        if latest_snapshot:
            note_parts.append(f"latest snapshot {latest_snapshot}")
        if record_count:
            note_parts.append(f"{record_count} records")
        if last_processed:
            note_parts.append(f"processed {last_processed}")
        if status:
            note_parts.append(f"status {status}")

        note = ", ".join(note_parts)
        self.add_note(note)

    def add_note(self, note: str):
        """Add a note to the notes section."""
        self._ensure_file_exists()

        content = self.progress_file.read_text()

        # Simply append the note to the end of the file in the notes section
        lines = content.split('\n')
        new_lines = []
        notes_found = False

        for line in lines:
            new_lines.append(line)
            if line.startswith("## Notes"):
                notes_found = True

        # If we found the notes section, add the note after the last note
        if notes_found:
            new_lines.append(f"- {note}")
        else:
            # If no notes section exists, add it at the end
            new_lines.extend([
                "",
                "## Notes",
                f"- {note}",
                ""
            ])

        content = '\n'.join(new_lines)
        self.progress_file.write_text(content)

    def _replace_line(self, content: str, start_marker: str, replacement: str) -> str:
        """Replace a line that starts with the marker."""
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if line.strip().startswith(start_marker):
                lines[i] = replacement
                break
        return '\n'.join(lines)


# Global instance for easy access
tracker = ProgressTracker()