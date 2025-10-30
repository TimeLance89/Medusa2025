"""Provider implementations for the scraper manager."""

from .filmpalast import FilmpalastScraper, FilmpalastSeriesScraper
from .kinox import KinoxScraper

__all__ = ["FilmpalastScraper", "FilmpalastSeriesScraper", "KinoxScraper"]
