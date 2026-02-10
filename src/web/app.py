"""FastAPI application setup.

Copyright (c) 2026 Canadian Postal Code Change Tracking System Contributors
Licensed under the MIT License - see LICENSE file for details.

Data sources used by this software have their own licenses:
- Statistics Canada NAR: Statistics Canada Open Licence
- Geocoder.ca: CC BY 2.5 Canada
- GeoNames: CC BY 4.0
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.web.api import router

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Canadian Postal Code Change Tracker", version="0.1.0")
app.include_router(router, prefix="/api")

# Serve static files (HTML/JS/CSS) at the root
if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
