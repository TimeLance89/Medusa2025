"""Background runner for the Filmpalast series scraper.

This module orchestrates scraping Serien listings from filmpalast.to via the
``FilmpalastSeriesScraper`` provider and persists the discovered links via the
Flask application helpers.  It is designed to be imported lazily from ``app``
so all imports that would otherwise create circular dependencies happen inside
functions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Tuple

from scrapers import ScraperResult, get_scraper_manager

PROVIDER_NAME = "filmpalast_series"

SeriesScraperCallback = Optional[
    Callable[[ScraperResult, str, Optional[str]], None]
]


@dataclass(slots=True)
class SeriesScraperStats:
    """Collect basic statistics for a scraper run."""

    start_page: int
    total_entries: int = 0
    created: int = 0
    updated: int = 0
    exists: int = 0
    skipped: int = 0
    errors: int = 0

    def register(self, status: str) -> None:
        """Increment the counter for ``status`` if it is known."""

        normalized = (status or "").lower()
        if normalized == "created":
            self.created += 1
        elif normalized == "updated":
            self.updated += 1
        elif normalized == "exists":
            self.exists += 1
        elif normalized == "skipped":
            self.skipped += 1
        else:
            self.errors += 1

    def as_dict(self) -> dict:
        """Return a serialisable representation of the collected stats."""

        processed = self.created + self.updated + self.exists
        return {
            "start_page": self.start_page,
            "total_entries": self.total_entries,
            "created": self.created,
            "updated": self.updated,
            "exists": self.exists,
            "skipped": self.skipped,
            "errors": self.errors,
            "processed": processed,
        }


def _emit_callback(
    callback: SeriesScraperCallback,
    entry: ScraperResult,
    status: str,
    identifier: Optional[str],
) -> None:
    if not callback:
        return
    try:
        callback(entry, status, identifier)
    except Exception:
        # The callback is best-effort logging – it should never break the run.
        pass


def _ensure_series_metadata(entry: ScraperResult) -> Optional[Tuple[str, int, int]]:
    """Return parsed series metadata for the scraper result.

    ``FilmpalastSeriesScraper`` already provides structured metadata for each
    entry.  Nevertheless the helper adds a fallback parser for resilience.
    """

    metadata = entry.metadata or {}
    series_title = metadata.get("series_title")
    season = metadata.get("season")
    episode = metadata.get("episode")

    if series_title and isinstance(season, int) and isinstance(episode, int):
        return series_title, season, episode

    # Fallback: parse the information from the title via the Flask helper.
    try:
        from app import _extract_series_metadata
    except Exception:
        return None

    parsed_title, parsed_season, parsed_episode = _extract_series_metadata(
        entry.title or ""
    )
    if not parsed_title or parsed_season is None or parsed_episode is None:
        return None

    metadata.setdefault("series_title", parsed_title)
    metadata.setdefault("season", parsed_season)
    metadata.setdefault("episode", parsed_episode)
    entry.metadata = metadata
    return parsed_title, parsed_season, parsed_episode


def run_scraper(
    page: int = 1,
    callback: SeriesScraperCallback = None,
) -> Tuple[int, Optional[SeriesScraperStats]]:
    """Run a single scraping pass for the Filmpalast series provider.

    Parameters
    ----------
    page:
        The listing page that should be scraped.  Must be greater than zero.
    callback:
        Optional function used to relay progress information back to the
        caller.  The callback receives the current ``ScraperResult``, the
        persistence status (``created``, ``updated`` …) and a human readable
        identifier.

    Returns
    -------
    tuple
        ``(actual_start_page, stats)``.  ``stats`` is ``None`` if no entries
        were discovered on the requested page.
    """

    if page < 1:
        raise ValueError("page must be greater than or equal to 1")

    manager = get_scraper_manager()
    scraper = manager.get_scraper(PROVIDER_NAME)
    if scraper is None:
        raise ValueError("Filmpalast series scraper is not registered")

    # Import Flask helpers lazily to avoid circular imports.
    from app import (
        attach_series_streaming_entry,
        get_scraper_int_setting,
        set_scraper_setting,
    )

    set_scraper_setting(PROVIDER_NAME, "page", page)

    entries = list(scraper.scrape_page(page))
    if not entries:
        # Make sure the stored page does not advance when no entries were found.
        set_scraper_setting(
            PROVIDER_NAME,
            "page",
            get_scraper_int_setting(PROVIDER_NAME, "page", page),
        )
        return page, None

    stats = SeriesScraperStats(start_page=page)

    for entry in entries:
        stats.total_entries += 1
        ensured = _ensure_series_metadata(entry)
        if ensured is None:
            stats.skipped += 1
            _emit_callback(callback, entry, "skipped", entry.title)
            continue

        try:
            status, identifier = attach_series_streaming_entry(entry)
        except Exception:
            stats.errors += 1
            _emit_callback(callback, entry, "error", entry.title)
            continue

        stats.register(status)
        _emit_callback(callback, entry, status, identifier)

    # Advance the stored page pointer so subsequent runs continue where we
    # left off.  ``page`` acts as "last processed" while ``next_page`` is
    # maintained by the Flask application itself.
    set_scraper_setting(PROVIDER_NAME, "page", page + 1)

    return page, stats


__all__ = ["run_scraper", "SeriesScraperStats"]
