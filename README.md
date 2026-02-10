# Canadian Postal Code Change Tracking System

This project tracks historical changes to Canadian postal codes (~893,000 unique codes) and visualizes these changes over time. The system detects additions, retirements, and attribute changes to postal codes across different time periods.

## Project Structure

- `src/` - Main source code with parsers, database, CLI, and web components
- `data-spec.md` - Comprehensive documentation of Canadian postal code system and data sources
- `pyproject.toml` - Project dependencies and configuration
- `data/` - Raw and processed data (excluded from git due to size)
- `postal_codes.db` - SQLite database (excluded from git due to size)

## What is Tracked in Git

### ✅ **Tracked Files:**
- Source code (`src/` directory)
- Configuration files (`pyproject.toml`, `src/config.py`)
- Documentation (`data-spec.md`, `README.md`)
- Web interface files (`src/web/`)
- CLI entry points (`src/cli.py`)
- Database schema and helper functions (`src/db.py`)
- Parser implementations (`src/parser_*.py`)
- Data processing logic (`src/differ.py`, `src/downloader.py`)
- `.gitignore` file

### ❌ **Not Tracked (in .gitignore):**
- Raw data files (`data/raw/`) - Large files from data sources
- Processed data files (`data/processed/`) - Intermediate processing results
- Database files (`postal_codes.db`) - Generated from processing
- Python virtual environments (`.venv/`, `venv/`)
- Python cache files (`__pycache__/`, `*.pyc`)
- Log files (`*.log`)
- Temporary files and OS-specific files

## Setup Instructions

1. Clone the repository
2. Install dependencies: `pip install -e .`
3. Download data: `python -m src.cli download`
4. Process data: `python -m src.cli process`
5. Run change detection: `python -m src.cli diff`
6. Start web interface: `python -m src.cli serve`

## Data Pipeline

The system uses multiple data sources:
1. Statistics Canada NAR (National Address Register) - Primary historical source
2. Geocoder.ca - Crowdsourced current data
3. GeoNames - Global postal code extract

## Important Notes

- The `data/` directory contains large files (GBs) and is excluded from git
- The database file is generated and excluded from git
- Only source code, configuration, and documentation are tracked in version control
- Data processing results are regenerated from source code and raw data

## License

This software is licensed under the [MIT License](LICENSE).

### Data Source Attribution

This project uses data from the following sources, each with their own licensing terms:

- **Statistics Canada National Address Register (NAR)** - [Statistics Canada Open Licence](https://www.statcan.gc.ca/en/reference/licence)
- **Geocoder.ca** - [Creative Commons Attribution 2.5 Canada (CC BY 2.5)](https://creativecommons.org/licenses/by/2.5/ca/)
- **GeoNames** - [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)

When using or redistributing this software with data:
- Maintain attribution to the original data sources as listed above
- Respect the license terms of each data source
- See [data-spec.md](data-spec.md) for detailed information about data sources and their licensing

### Dependencies

This project uses the following open-source libraries:
- pandas (BSD-3-Clause)
- pyarrow (Apache-2.0)
- requests (Apache-2.0)
- tqdm (MIT/MPL-2.0)
- click (BSD-3-Clause)
- fastapi (MIT)
- uvicorn (BSD)

All dependencies are compatible with the MIT license of this software.