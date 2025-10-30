"""Scraper implementations for filmpalast.to listings."""
from __future__ import annotations

import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag

from ..base import BaseScraper, ProgressCallback, ScraperResult


class _FilmpalastBase:
    """Shared functionality for Filmpalast scrapers."""

    label = "Filmpalast"

    MOVIE_BASE_URL = "https://filmpalast.to/movies/new/page/{page}"
    SERIES_BASE_URL = "https://filmpalast.to/serien/view/page/{page}"
    MOVIE_SELECTOR_TITLE = "h2.rb > a.rb"
    SERIES_SELECTOR_TITLE = "#serien-nav li > a"
    VOE_HOSTNAME = "voe.sx"
    VEEV_HOSTNAME = "veev.to"
    GENERIC_HOSTNAMES = (
        "savefiles.com",
        "bigwarp.pro",
        "strmup.to",
    )
    OFFLINE_MARKER = "404 - Nicht gefunden"
    REQUEST_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://filmpalast.to/",
    }

    # ------------------------------------------------------------------
    # Movie handling
    # ------------------------------------------------------------------
    def _scrape_movies_page(
        self, page: int, progress_callback: ProgressCallback = None
    ) -> List[ScraperResult]:
        url = self.MOVIE_BASE_URL.format(page=page)
        response = requests.get(url, timeout=20, headers=self.REQUEST_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        results: List[ScraperResult] = []
        for anchor in soup.select(self.MOVIE_SELECTOR_TITLE):
            title = anchor.get_text(strip=True)
            href = anchor.get("href")
            if not href:
                continue
            detail_url = self._normalize_detail_url(url, href)
            stream_data = self._scrape_detail(detail_url)
            for data in stream_data:
                metadata = {"host_name": data.get("host_name"), "type": "movie"}
                result = ScraperResult(
                    title=title,
                    streaming_url=data["url"],
                    detail_url=detail_url,
                    mirror_info=data.get("mirror_info"),
                    provider=self.name,
                    source_name=self.label,
                    metadata=metadata,
                )
                self._emit_progress(progress_callback, result)
                results.append(result)
        return results

    # ------------------------------------------------------------------
    # Series handling
    # ------------------------------------------------------------------
    def _scrape_series_page(
        self, page: int, progress_callback: ProgressCallback = None
    ) -> List[ScraperResult]:
        url = self.SERIES_BASE_URL.format(page=page)
        response = requests.get(url, timeout=20, headers=self.REQUEST_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        series_links: Dict[str, str] = {}
        for anchor in soup.select(self.SERIES_SELECTOR_TITLE):
            href = anchor.get("href")
            if not href:
                continue
            detail_url = self._normalize_detail_url(url, href)
            series_slug = self._extract_series_slug(detail_url)
            if not series_slug or series_slug in series_links:
                continue
            series_links[series_slug] = detail_url

        results: List[ScraperResult] = []
        for detail_url in series_links.values():
            series_results = self._scrape_series_detail(detail_url, progress_callback)
            results.extend(series_results)
        return results

    def _extract_series_slug(self, detail_url: str) -> Optional[str]:
        parsed = urlparse(detail_url)
        slug = parsed.path.rstrip("/").split("/")[-1]
        match = re.match(r"(?P<slug>.+)-s\d+e\d+$", slug, re.IGNORECASE)
        if not match:
            return None
        return match.group("slug")

    def _scrape_series_detail(
        self, detail_url: str, progress_callback: ProgressCallback = None
    ) -> List[ScraperResult]:
        response = requests.get(detail_url, timeout=20, headers=self.REQUEST_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        dropdown = soup.select_one("#dropdown-btn")
        dropdown_text = dropdown.get_text(strip=True) if dropdown else ""
        series_title = dropdown_text.split("(")[0].strip() or None

        seen_episode_urls: set[str] = set()
        results: List[ScraperResult] = []
        for anchor in soup.select("a.getStaffelStream"):
            href = anchor.get("href")
            if not href:
                continue
            episode_url = self._normalize_detail_url(detail_url, href)
            if episode_url in seen_episode_urls:
                continue
            seen_episode_urls.add(episode_url)

            episode_meta = self._parse_episode_metadata(anchor, series_title)
            if episode_meta is None:
                continue

            stream_data = self._scrape_detail(episode_url)
            if not stream_data:
                continue

            for data in stream_data:
                metadata = {
                    "host_name": data.get("host_name"),
                    "type": "series",
                    "series_title": episode_meta["series_title"],
                    "season": episode_meta["season"],
                    "episode": episode_meta["episode"],
                }
                result = ScraperResult(
                    title=episode_meta["title"],
                    streaming_url=data["url"],
                    detail_url=episode_url,
                    mirror_info=data.get("mirror_info"),
                    provider=self.name,
                    source_name=self.label,
                    metadata=metadata,
                )
                self._emit_progress(progress_callback, result)
                results.append(result)
        return results

    def _parse_episode_metadata(
        self, anchor: Tag, fallback_series_title: Optional[str]
    ) -> Optional[Dict[str, object]]:
        direct_texts = [
            text.strip()
            for text in anchor.find_all(string=True, recursive=False)
            if text and text.strip()
        ]
        if not direct_texts:
            return None
        combined = " ".join(direct_texts)
        match = re.search(r"(.+?)\s*S(\d{1,2})E(\d{1,2})", combined, re.IGNORECASE)
        if not match:
            return None

        series_title = match.group(1).strip() or fallback_series_title
        if not series_title:
            return None

        try:
            season = int(match.group(2))
            episode = int(match.group(3))
        except ValueError:
            return None

        formatted_title = f"{series_title} S{season:02d}E{episode:02d}"
        return {
            "series_title": series_title,
            "season": season,
            "episode": episode,
            "title": formatted_title,
        }

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _emit_progress(
        self, progress_callback: ProgressCallback, result: ScraperResult
    ) -> None:
        if not progress_callback:
            return
        try:
            progress_callback(result)
        except Exception:
            pass

    def _normalize_detail_url(self, base_url: str, href: str) -> str:
        if href.startswith("//"):
            return "https:" + href
        return urljoin(base_url, href)

    def _scrape_detail(self, detail_url: str) -> List[dict[str, Optional[str]]]:
        response = requests.get(detail_url, timeout=20, headers=self.REQUEST_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        return self._parse_stream_links(soup, detail_url)

    def _parse_stream_links(
        self, soup: BeautifulSoup, detail_url: str
    ) -> List[dict[str, Optional[str]]]:
        results: List[dict[str, Optional[str]]] = []
        for host_item in soup.select("li.hostBg"):
            host_name_elem = host_item.select_one(".hostName")
            host_name = host_name_elem.get_text(strip=True) if host_name_elem else None
            link_item = host_item.find_next_sibling("li", class_="streamPlayBtn")
            if not link_item:
                continue
            anchor = link_item.find("a")
            if not anchor:
                continue

            streaming_url = (
                anchor.get("data-player-url")
                or anchor.get("href")
                or ""
            ).strip()
            if not streaming_url:
                continue
            streaming_url = self._normalize_streaming_url(streaming_url, detail_url)

            host_type = self._identify_host_type(streaming_url)
            if host_type is None:
                continue
            if not self._is_stream_online(streaming_url, host_type):
                continue

            results.append(
                {
                    "url": streaming_url,
                    "mirror_info": host_name,
                    "host_name": host_name,
                    "host_type": host_type,
                }
            )
        return results

    def _normalize_streaming_url(self, streaming_url: str, base_url: str) -> str:
        if streaming_url.startswith("//"):
            streaming_url = "https:" + streaming_url
        elif streaming_url.startswith("/"):
            streaming_url = urljoin(base_url, streaming_url)

        parsed = urlparse(streaming_url)
        if parsed.netloc.endswith(self.VOE_HOSTNAME) and not parsed.path.startswith("/e/"):
            path = parsed.path.lstrip("/")
            path = "e/" + path
            parsed = parsed._replace(path="/" + path)
            streaming_url = urlunparse(parsed)
        return streaming_url

    def _identify_host_type(self, streaming_url: str) -> Optional[str]:
        hostname = urlparse(streaming_url).netloc.lower()
        if hostname.endswith(self.VOE_HOSTNAME):
            return "voe"
        if hostname.endswith(self.VEEV_HOSTNAME):
            return "veev"
        for generic_host in self.GENERIC_HOSTNAMES:
            if hostname.endswith(generic_host):
                return "generic"
        if hostname:
            return "generic"
        return None

    def _is_stream_online(self, streaming_url: str, host_type: str) -> bool:
        if host_type == "voe":
            return self._is_voe_link_online(streaming_url)
        if host_type == "veev":
            return self._is_veev_link_online(streaming_url)
        return True

    def _is_voe_link_online(self, streaming_url: str) -> bool:
        try:
            response = requests.get(
                streaming_url, timeout=20, headers=self.REQUEST_HEADERS
            )
        except requests.RequestException:
            return False

        if response.status_code >= 400:
            return False

        if not self._identify_host_type(response.url) == "voe":
            return False

        if self.OFFLINE_MARKER.lower() in response.text.lower():
            return False

        return True

    def _is_veev_link_online(self, streaming_url: str) -> bool:
        try:
            response = requests.get(
                streaming_url, timeout=20, headers=self.REQUEST_HEADERS
            )
        except requests.RequestException:
            return False

        if response.status_code >= 400:
            return False

        return True


class FilmpalastScraper(_FilmpalastBase, BaseScraper):
    """Scraper for filmpalast.to movie listings."""

    name = "filmpalast"
    label = "Filmpalast"
    content_categories = ("movies",)

    def scrape_page(
        self, page: int, progress_callback: ProgressCallback = None
    ) -> List[ScraperResult]:
        return self._scrape_movies_page(page, progress_callback)


class FilmpalastSeriesScraper(_FilmpalastBase, BaseScraper):
    """Scraper for filmpalast.to series listings."""

    name = "filmpalast_series"
    label = "Filmpalast Serien"
    content_categories = ("series",)

    def scrape_page(
        self, page: int, progress_callback: ProgressCallback = None
    ) -> List[ScraperResult]:
        return self._scrape_series_page(page, progress_callback)


__all__ = ["FilmpalastScraper", "FilmpalastSeriesScraper"]
