"""Utilities to scrape kinox.farm movie listings."""
from __future__ import annotations

from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://kinox.farm/kinofilme-online/page/{page}/"
SELECTOR_TITLE = "div.short-entry-title a"
SELECTOR_MIRROR = "li.MirBtn.MirBtnA.MirBaseStyleflv"


def scrape_page(page: int) -> List[dict]:
    """Scrape a kinox listing page for movie titles and streaming links."""
    url = BASE_URL.format(page=page)
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    results: List[dict] = []
    for anchor in soup.select(SELECTOR_TITLE):
        title = anchor.get_text(strip=True)
        href = anchor.get("href")
        detail_url = urljoin(url, href) if href else None
        stream_data = scrape_detail(detail_url) if detail_url else None
        if title and stream_data:
            results.append(
                {
                    "title": title,
                    "streaming_url": stream_data["url"],
                    "detail_url": detail_url,
                    "mirror": stream_data.get("mirror_info"),
                }
            )
    return results


def scrape_detail(detail_url: str) -> Optional[Dict[str, Optional[str]]]:
    """Return the Supervideo streaming link data from a kinox detail page."""
    response = requests.get(detail_url, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for mirror in soup.select(SELECTOR_MIRROR):
        data_link = mirror.get("data-link")
        if not data_link:
            continue

        named = mirror.select_one(".Named")
        name_text = named.get_text(strip=True).lower() if named else ""
        if "supervideo" not in name_text and "supervideo" not in data_link.lower():
            continue

        data = mirror.select_one(".Data")
        mirror_info = data.get_text(strip=True) if data else None
        return {"url": data_link, "mirror_info": mirror_info}

    return None
