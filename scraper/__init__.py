"""Utilities for orchestrating scraper runs outside the Flask app context."""

from .filmpalast_series_scraper import (
    SeriesScraperStats,
    run_scraper as run_filmpalast_series,
)

__all__ = ["run_filmpalast_series", "SeriesScraperStats"]
