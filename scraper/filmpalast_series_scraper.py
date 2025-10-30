"""Standalone Filmpalast series scraper script.

This script scrapes series stream links from filmpalast.to, enriches the
metadata with TMDB and persists them inside the application's database.

Usage::

    python scraper/filmpalast_series_scraper.py [--page N] [--reset]

The scraper keeps track of the next page to process in the ``settings``
table using the ``filmpalast_series_page`` key. Every successful run
continues where the previous one stopped unless a different page is
provided via ``--page`` or ``--reset`` is passed to start from page 1.
"""
from __future__ import annotations

import argparse
import logging
import sys
from contextlib import suppress
from dataclasses import dataclass
from typing import Dict, Iterable

from app import (
    app,
    attach_series_streaming_entry,
    get_scraper_int_setting,
    set_scraper_setting,
)
from scrapers.base import ScraperResult
from scrapers.providers.filmpalast import FilmpalastSeriesScraper


LOGGER = logging.getLogger("filmpalast_series_scraper")
PROVIDER_NAME = "filmpalast_series"
SETTING_SUFFIX_PAGE = "page"
DEFAULT_START_PAGE = 1


@dataclass(slots=True)
class ProcessingStats:
    """Collects information about the processed streaming links."""

    created: int = 0
    updated: int = 0
    exists: int = 0
    skipped: int = 0
    errors: int = 0

    def register(self, status: str) -> None:
        status = status.lower()
        if status == "created":
            self.created += 1
        elif status == "updated":
            self.updated += 1
        elif status == "exists":
            self.exists += 1
        elif status == "skipped":
            self.skipped += 1
        else:
            self.errors += 1

    def as_dict(self) -> Dict[str, int]:
        return {
            "created": self.created,
            "updated": self.updated,
            "exists": self.exists,
            "skipped": self.skipped,
            "errors": self.errors,
        }


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--page",
        type=int,
        help="Page number to scrape instead of using the stored progress",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset the stored page progress to the default start page",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Configure logging verbosity (default: INFO)",
    )
    return parser.parse_args(argv)


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(levelname)s [%(name)s] %(message)s",
    )


def get_start_page(args: argparse.Namespace) -> int:
    """Determine which page the scraper should process."""

    start_page = DEFAULT_START_PAGE
    if args.reset:
        LOGGER.info("Resetting stored page progress to %s", DEFAULT_START_PAGE)
        set_scraper_setting(PROVIDER_NAME, SETTING_SUFFIX_PAGE, DEFAULT_START_PAGE)
        start_page = DEFAULT_START_PAGE

    if args.page is not None:
        if args.page < 1:
            raise ValueError("Page must be >= 1")
        LOGGER.info("Overriding start page via CLI to %s", args.page)
        set_scraper_setting(PROVIDER_NAME, SETTING_SUFFIX_PAGE, args.page)
        return args.page

    stored_page = get_scraper_int_setting(
        PROVIDER_NAME, SETTING_SUFFIX_PAGE, DEFAULT_START_PAGE
    )
    LOGGER.info("Continuing from stored page %s", stored_page)
    return max(stored_page, DEFAULT_START_PAGE)


def persist_results(results: Iterable[ScraperResult]) -> ProcessingStats:
    """Persist scraped results inside the application's database."""

    stats = ProcessingStats()
    for entry in results:
        try:
            status, identifier = attach_series_streaming_entry(entry)
            stats.register(status)
            if identifier:
                LOGGER.info("%s – %s", status.capitalize(), identifier)
            else:
                LOGGER.info("%s – %s", status.capitalize(), entry.title)
        except Exception as exc:  # pragma: no cover - logging safeguard
            stats.register("error")
            LOGGER.exception("Failed to persist %s: %s", entry.title, exc)
    return stats


def main(argv: Iterable[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    args = parse_args(argv)
    setup_logging(args.log_level)

    scraper = FilmpalastSeriesScraper()

    with app.app_context():
        try:
            start_page = get_start_page(args)
        except ValueError as exc:
            LOGGER.error("%s", exc)
            return 1

        LOGGER.info("Scraping filmpalast.to series page %s", start_page)
        results = scraper.scrape_page(start_page)
        if not results:
            LOGGER.warning("No results found on page %s", start_page)
            return 0

        stats = persist_results(results)
        LOGGER.info("Finished processing page %s", start_page)
        LOGGER.info("Summary: %s", stats.as_dict())

        next_page = start_page + 1
        LOGGER.info("Updating stored page to %s", next_page)
        set_scraper_setting(PROVIDER_NAME, SETTING_SUFFIX_PAGE, next_page)

    return 0


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        sys.exit(main())
