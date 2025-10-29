"""Scraper implementation for filmpalast.to movie listings."""
from __future__ import annotations

from typing import List, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from ..base import BaseScraper, ProgressCallback, ScraperResult


class FilmpalastScraper(BaseScraper):
    """Scraper for the filmpalast.to provider."""

    name = "filmpalast"
    label = "Filmpalast"

    BASE_URL = "https://filmpalast.to/movies/new/page/{page}"
    SELECTOR_TITLE = "h2.rb > a.rb"
    VOE_HOSTNAME = "voe.sx"
    OFFLINE_MARKER = "404 - Nicht gefunden"
    REQUEST_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://filmpalast.to/",
    }

    def scrape_page(
        self, page: int, progress_callback: ProgressCallback = None
    ) -> List[ScraperResult]:
        url = self.BASE_URL.format(page=page)
        response = requests.get(url, timeout=20, headers=self.REQUEST_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        results: List[ScraperResult] = []
        for anchor in soup.select(self.SELECTOR_TITLE):
            title = anchor.get_text(strip=True)
            href = anchor.get("href")
            if not href:
                continue
            detail_url = self._normalize_detail_url(url, href)
            stream_data = self._scrape_detail(detail_url)
            for data in stream_data:
                result = ScraperResult(
                    title=title,
                    streaming_url=data["url"],
                    detail_url=detail_url,
                    mirror_info=data.get("mirror_info"),
                    provider=self.name,
                    source_name=self.label,
                    metadata={"host_name": data.get("host_name")},
                )
                if progress_callback:
                    try:
                        progress_callback(result)
                    except Exception:
                        pass
                results.append(result)
        return results

    def _normalize_detail_url(self, base_url: str, href: str) -> str:
        if href.startswith("//"):
            return "https:" + href
        return urljoin(base_url, href)

    def _scrape_detail(self, detail_url: str) -> List[dict[str, Optional[str]]]:
        response = requests.get(detail_url, timeout=20, headers=self.REQUEST_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        results: List[dict[str, Optional[str]]] = []
        for host_item in soup.select("li.hostBg"):
            host_name_elem = host_item.select_one(".hostName")
            host_name = host_name_elem.get_text(strip=True) if host_name_elem else None
            link_item = host_item.find_next_sibling("li", class_="streamPlayBtn")
            if not link_item:
                continue
            anchor = link_item.find("a", href=True)
            if not anchor:
                continue
            streaming_url = anchor["href"].strip()
            streaming_url = self._normalize_streaming_url(streaming_url, detail_url)
            if not self._is_voe_stream(streaming_url):
                continue
            if not self._is_voe_link_online(streaming_url):
                continue
            results.append(
                {
                    "url": streaming_url,
                    "mirror_info": host_name,
                    "host_name": host_name,
                }
            )
        return results

    def _normalize_streaming_url(self, streaming_url: str, base_url: str) -> str:
        if streaming_url.startswith("//"):
            streaming_url = "https:" + streaming_url
        elif streaming_url.startswith("/"):
            streaming_url = urljoin(base_url, streaming_url)

        parsed = urlparse(streaming_url)
        if parsed.netloc.endswith("voe.sx") and not parsed.path.startswith("/e/"):
            path = parsed.path.lstrip("/")
            path = "e/" + path
            parsed = parsed._replace(path="/" + path)
            streaming_url = urlunparse(parsed)
        return streaming_url

    def _is_voe_stream(self, streaming_url: str) -> bool:
        parsed = urlparse(streaming_url)
        hostname = parsed.netloc.lower()
        return hostname.endswith(self.VOE_HOSTNAME)

    def _is_voe_link_online(self, streaming_url: str) -> bool:
        try:
            response = requests.get(
                streaming_url, timeout=20, headers=self.REQUEST_HEADERS
            )
        except requests.RequestException:
            return False

        if response.status_code >= 400:
            return False

        if not self._is_voe_stream(response.url):
            return False

        if self.OFFLINE_MARKER.lower() in response.text.lower():
            return False

        return True


__all__ = ["FilmpalastScraper"]
