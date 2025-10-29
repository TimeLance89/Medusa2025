"""Scraper manager responsible for orchestrating provider scrapers."""
from __future__ import annotations

from typing import Dict, Iterable, Optional, Sequence

from .base import BaseScraper, ProgressCallback, ScraperResult
from .providers.filmpalast import FilmpalastScraper
from .providers.kinox import KinoxScraper


class ScraperManager:
    """Central registry that keeps track of scraper providers."""

    def __init__(self) -> None:
        self._scrapers: Dict[str, BaseScraper] = {}
        self.register_scraper(KinoxScraper())
        self.register_scraper(FilmpalastScraper())

    def register_scraper(self, scraper: BaseScraper) -> None:
        self._scrapers[scraper.name] = scraper

    def get_scraper(self, provider: str) -> Optional[BaseScraper]:
        return self._scrapers.get(provider)

    def available_providers(self) -> Sequence[BaseScraper]:
        return tuple(self._scrapers.values())

    def scrape_page(
        self,
        provider: str,
        page: int,
        progress_callback: ProgressCallback = None,
    ) -> Iterable[ScraperResult]:
        scraper = self.get_scraper(provider)
        if scraper is None:
            raise ValueError(f"No scraper registered for provider '{provider}'.")
        return scraper.scrape_page(page, progress_callback=progress_callback)


_SCRAPER_MANAGER: Optional[ScraperManager] = None


def get_scraper_manager() -> ScraperManager:
    global _SCRAPER_MANAGER
    if _SCRAPER_MANAGER is None:
        _SCRAPER_MANAGER = ScraperManager()
    return _SCRAPER_MANAGER


__all__ = ["ScraperManager", "get_scraper_manager"]
