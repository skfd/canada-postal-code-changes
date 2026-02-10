"""FastAPI application setup."""

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
