"""Common types and interfaces for scraper providers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol, Sequence


ProgressCallback = Optional[Callable[["ScraperResult"], None]]


@dataclass(slots=True)
class ScraperResult:
    """Represents a single streaming link discovered by a scraper."""

    title: str
    streaming_url: str
    provider: str
    source_name: str
    detail_url: Optional[str] = None
    mirror_info: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseScraper(Protocol):
    """Protocol that every scraper provider implementation must follow."""

    name: str
    label: str

    def scrape_page(
        self, page: int, progress_callback: ProgressCallback = None
    ) -> Sequence[ScraperResult]:
        """Scrape a single page and return discovered streaming links."""


__all__ = ["BaseScraper", "ProgressCallback", "ScraperResult"]
