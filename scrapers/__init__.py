"""Scraper package for Medusa server."""

from .base import BaseScraper, ProgressCallback, ScraperResult
from .manager import ScraperManager, get_scraper_manager

__all__ = [
    "BaseScraper",
    "ProgressCallback",
    "ScraperResult",
    "ScraperManager",
    "get_scraper_manager",
]
