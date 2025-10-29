"""Provider implementations for the scraper manager."""

from .filmpalast import FilmpalastScraper
from .kinox import KinoxScraper

__all__ = ["FilmpalastScraper", "KinoxScraper"]
