"""Utilities to scrape kinox.farm movie listings."""
from __future__ import annotations

from typing import List, Optional

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://kinox.farm/kinofilme-online/page/{page}/"
SELECTOR_TITLE = "div.short-entry-title a"
SELECTOR_MIRROR = "li.MirBtn.MirBtnA.MirBaseStyleflv"


def scrape_page(page: int) -> List[dict]:
    """Scrape a kinox listing page for movie titles and links."""
    url = BASE_URL.format(page=page)
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    results: List[dict] = []
    for anchor in soup.select(SELECTOR_TITLE):
        title = anchor.get_text(strip=True)
        detail_url = anchor.get("href")
        stream_url = scrape_detail(detail_url) if detail_url else None
        if title and stream_url:
            results.append(
                {
                    "title": title,
                    "streaming_url": stream_url,
                    "detail_url": detail_url,
                }
            )
    return results


def scrape_detail(detail_url: str) -> Optional[str]:
    """Return the first streaming link from a kinox detail page."""
    response = requests.get(detail_url, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    mirror = soup.select_one(SELECTOR_MIRROR)
    if not mirror:
        return None
    return mirror.get("data-link")
