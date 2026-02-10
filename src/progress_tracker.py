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

This file tracks the progress of data processing operations for the Canadian Postal Code Change Tracking System.

## Overall Status
- Last Updated: 
- Current Operation: 
- Status: 

## Operations Log

### Download Operations
| Date | Source | Period | Status | Records | Duration |
|------|--------|--------|--------|---------|----------|
|      |        |        |        |         |          |

### Processing Operations  
| Date | Source | Period | Status | Input Records | Output Records | Duration |
|------|--------|--------|--------|---------------|----------------|----------|
|      |        |        |        |               |                |          |

### Diff Operations
| Date | Source | From Date | To Date | Status | Changes Detected | Duration |
|------|--------|-----------|---------|--------|------------------|----------|
|      |        |           |         |        |                  |          |

## Data Sources Status
| Source | Latest Snapshot | Record Count | Last Processed | Status |
|--------|-----------------|--------------|----------------|--------|
| NAR    |                 |              |                |        |
| Geocoder.ca |             |              |                |        |
| GeoNames |               |              |                |        |

## Database Status
- Database Path: 
- Tables Initialized: 
- Total Postal Codes: 
- Total Changes Detected: 

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
        
        # Update Current Operation
        content = self._replace_line(content, "- Current Operation:", f"- Current Operation: {operation}")
        
        # Update Status
        content = self._replace_line(content, "- Status:", f"- Status: {status}")
        
        self.progress_file.write_text(content)
        self.current_operation = operation
    
    def log_download_operation(self, source: str, period: Optional[str], status: str, 
                              records: Optional[int] = None, duration: Optional[float] = None):
        """Log a download operation to the progress file."""
        self._ensure_file_exists()
        
        content = self.progress_file.read_text()
        
        # Format the row data
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        records_str = str(records) if records is not None else ""
        duration_str = f"{duration:.2f}s" if duration is not None else ""
        period_str = period or ""
        
        # Create the row
        row = f"| {date} | {source} | {period_str} | {status} | {records_str} | {duration_str} |"
        
        # Find the exact location to insert: after the header separator in the download operations table
        lines = content.split('\n')
        new_lines = []
        found_download_section = False
        found_header = False
        found_separator = False
        
        for line in lines:
            new_lines.append(line)
            
            if "### Download Operations" in line:
                found_download_section = True
                found_header = False
                found_separator = False
            elif found_download_section and not found_header and "| Date | Source | Period | Status | Records | Duration |" in line:
                found_header = True
            elif found_download_section and found_header and not found_separator and "|------|" in line:
                found_separator = True
                # Insert the new row right after the separator
                new_lines.append(row)
        
        content = '\n'.join(new_lines)
        self.progress_file.write_text(content)
    
    def log_processing_operation(self, source: str, period: Optional[str], status: str,
                                input_records: Optional[int] = None, 
                                output_records: Optional[int] = None,
                                duration: Optional[float] = None):
        """Log a processing operation to the progress file."""
        self._ensure_file_exists()
        
        content = self.progress_file.read_text()
        
        # Format the row data
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        input_str = str(input_records) if input_records is not None else ""
        output_str = str(output_records) if output_records is not None else ""
        duration_str = f"{duration:.2f}s" if duration is not None else ""
        period_str = period or ""
        
        # Create the row
        row = f"| {date} | {source} | {period_str} | {status} | {input_str} | {output_str} | {duration_str} |"
        
        # Find the exact location to insert: after the header separator in the processing operations table
        lines = content.split('\n')
        new_lines = []
        found_processing_section = False
        found_header = False
        found_separator = False
        
        for line in lines:
            new_lines.append(line)
            
            if "### Processing Operations" in line:
                found_processing_section = True
                found_header = False
                found_separator = False
            elif found_processing_section and not found_header and "| Date | Source | Period | Status | Input Records | Output Records | Duration |" in line:
                found_header = True
            elif found_processing_section and found_header and not found_separator and "|------|" in line:
                found_separator = True
                # Insert the new row right after the separator
                new_lines.append(row)
        
        content = '\n'.join(new_lines)
        self.progress_file.write_text(content)
    
    def log_diff_operation(self, source: str, from_date: Optional[str], to_date: Optional[str],
                          status: str, changes_detected: Optional[int] = None,
                          duration: Optional[float] = None):
        """Log a diff operation to the progress file."""
        self._ensure_file_exists()
        
        content = self.progress_file.read_text()
        
        # Format the row data
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        from_str = from_date or ""
        to_str = to_date or ""
        changes_str = str(changes_detected) if changes_detected is not None else ""
        duration_str = f"{duration:.2f}s" if duration is not None else ""
        
        # Create the row
        row = f"| {date} | {source} | {from_str} | {to_str} | {status} | {changes_str} | {duration_str} |"
        
        # Find the exact location to insert: after the header separator in the diff operations table
        lines = content.split('\n')
        new_lines = []
        found_diff_section = False
        found_header = False
        found_separator = False
        
        for line in lines:
            new_lines.append(line)
            
            if "### Diff Operations" in line:
                found_diff_section = True
                found_header = False
                found_separator = False
            elif found_diff_section and not found_header and "| Date | Source | From Date | To Date | Status | Changes Detected | Duration |" in line:
                found_header = True
            elif found_diff_section and found_header and not found_separator and "|------|" in line:
                found_separator = True
                # Insert the new row right after the separator
                new_lines.append(row)
        
        content = '\n'.join(new_lines)
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
    
    def update_database_status(self, db_path: Optional[Path] = None, 
                              tables_initialized: Optional[bool] = None,
                              total_postal_codes: Optional[int] = None,
                              total_changes: Optional[int] = None):
        """Update the database status section."""
        self._ensure_file_exists()
        
        content = self.progress_file.read_text()
        
        if db_path:
            content = self._replace_line(content, "- Database Path:", f"- Database Path: {db_path}")
        if tables_initialized is not None:
            status = "Yes" if tables_initialized else "No"
            content = self._replace_line(content, "- Tables Initialized:", f"- Tables Initialized: {status}")
        if total_postal_codes is not None:
            content = self._replace_line(content, "- Total Postal Codes:", f"- Total Postal Codes: {total_postal_codes}")
        if total_changes is not None:
            content = self._replace_line(content, "- Total Changes Detected:", f"- Total Changes Detected: {total_changes}")
        
        self.progress_file.write_text(content)
    
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