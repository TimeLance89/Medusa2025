import os
import re
import time
from collections import deque
from datetime import datetime
from threading import Lock, Thread
from typing import List, Optional, Set, Tuple

import requests
from flask import Flask, jsonify, render_template, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import case, func, or_
from sqlalchemy.engine import make_url

from scrapers import ScraperResult, get_scraper_manager


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "database", "mediahub.db")
DATABASE_URL = os.environ.get("DATABASE_URL", f"sqlite:///{DEFAULT_DB_PATH}")
_ENV_TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

SCRAPER_MANAGER = get_scraper_manager()

SCRAPER_STATUS_LOCK = Lock()
SCRAPER_LOG_MAXLEN = 200
SCRAPER_LOG: dict[str, deque] = {}
SCRAPER_STATUS: dict[str, dict] = {}
SCRAPER_THREADS: dict[str, Thread] = {}


MOVIE_RUNTIME_CACHE: dict[int, Optional[int]] = {}
MOVIE_RUNTIME_CACHE_LOCK = Lock()


class Movie(db.Model):
    __tablename__ = "movies"

    id = db.Column(db.Integer, primary_key=True)
    tmdb_id = db.Column(db.Integer, unique=True, nullable=False)
    title = db.Column(db.String(255), nullable=False)
    overview = db.Column(db.Text)
    poster_path = db.Column(db.String(255))
    backdrop_path = db.Column(db.String(255))
    release_date = db.Column(db.String(32))
    rating = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    streaming_links = db.relationship(
        "StreamingLink",
        back_populates="movie",
        cascade="all, delete-orphan",
        lazy="joined",
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "tmdb_id": self.tmdb_id,
            "title": self.title,
            "overview": self.overview,
            "poster_path": self.poster_path,
            "backdrop_path": self.backdrop_path,
            "release_date": self.release_date,
            "rating": self.rating,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "streaming_links": [link.to_dict() for link in self.streaming_links],
        }


class StreamingLink(db.Model):
    __tablename__ = "streaming_links"

    id = db.Column(db.Integer, primary_key=True)
    movie_id = db.Column(db.Integer, db.ForeignKey("movies.id"), nullable=False)
    source_name = db.Column(db.String(120), nullable=False)
    url = db.Column(db.String(500), nullable=False)
    mirror_info = db.Column(db.String(120))

    movie = db.relationship("Movie", back_populates="streaming_links")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "movie_id": self.movie_id,
            "source_name": self.source_name,
            "url": self.url,
            "mirror_info": self.mirror_info,
        }


class Setting(db.Model):
    __tablename__ = "settings"

    key = db.Column(db.String(64), primary_key=True)
    value = db.Column(db.String(255), nullable=False)


def movie_has_valid_streaming_link():
    return Movie.streaming_links.any(func.length(func.trim(StreamingLink.url)) > 0)


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    setting = Setting.query.filter_by(key=key).first()
    if setting is None:
        return default
    return setting.value


def set_setting(key: str, value: str) -> Setting:
    setting = Setting.query.filter_by(key=key).first()
    if setting is None:
        setting = Setting(key=key, value=value)
    else:
        setting.value = value
    db.session.add(setting)
    db.session.commit()
    return setting


def _scraper_setting_key(provider: str, suffix: str) -> str:
    return f"{provider}_{suffix}"


def get_scraper_int_setting(provider: str, suffix: str, default: int) -> int:
    return get_int_setting(_scraper_setting_key(provider, suffix), default)


def set_scraper_setting(provider: str, suffix: str, value: int) -> None:
    set_setting(_scraper_setting_key(provider, suffix), str(value))


def _default_scraper_status(provider: str, label: str) -> dict:
    return {
        "provider": provider,
        "provider_label": label,
        "running": False,
        "start_page": None,
        "current_page": None,
        "next_page": None,
        "last_page": None,
        "processed_pages": 0,
        "total_pages": 0,
        "processed_links": 0,
        "last_title": None,
        "message": "Bereit.",
        "error": None,
        "started_at": None,
        "finished_at": None,
        "last_update": None,
        "log": [],
    }


def _initialize_scraper_state(provider: str) -> str:
    scraper = SCRAPER_MANAGER.get_scraper(provider)
    if scraper is None:
        raise ValueError(f"Unbekannter Scraper: {provider}")

    label = scraper.label
    with SCRAPER_STATUS_LOCK:
        if provider not in SCRAPER_STATUS:
            SCRAPER_STATUS[provider] = _default_scraper_status(provider, label)
        else:
            SCRAPER_STATUS[provider].setdefault("provider", provider)
            SCRAPER_STATUS[provider].setdefault("provider_label", label)
        if provider not in SCRAPER_LOG:
            SCRAPER_LOG[provider] = deque(maxlen=SCRAPER_LOG_MAXLEN)
    return label


def get_tmdb_api_key() -> str:
    stored = get_setting("tmdb_api_key")
    if stored:
        return stored
    return _ENV_TMDB_API_KEY


def get_int_setting(key: str, default: int) -> int:
    value = get_setting(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def ensure_database() -> None:
    """Create the database schema when the app starts."""
    with app.app_context():
        db_uri = app.config["SQLALCHEMY_DATABASE_URI"]
        url = make_url(db_uri)
        if url.drivername == "sqlite" and url.database:
            db_path = url.database
            # SQLAlchemy returns relative paths as given, so resolve them to ensure
            # the parent directory exists before connecting to SQLite.
            if not os.path.isabs(db_path):
                db_path = os.path.join(BASE_DIR, db_path)
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
        db.create_all()


def fetch_tmdb_movies(category: str = "popular", page: int = 1) -> List[dict]:
    """Fetch movies from the TMDB API and return the JSON payload."""
    api_key = get_tmdb_api_key()
    if not api_key:
        raise RuntimeError(
            "TMDB_API_KEY is not set. Please provide a valid key in the settings area or as an environment variable."
        )

    api_url = f"https://api.themoviedb.org/3/movie/{category}"
    params = {"api_key": api_key, "language": "de-DE", "page": page}
    response = requests.get(api_url, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    return payload.get("results", [])


def fetch_tmdb_details(tmdb_id: int) -> dict:
    """Fetch detailed information for a movie from TMDB."""
    api_key = get_tmdb_api_key()
    if not api_key or tmdb_id <= 0:
        return {}

    api_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {
        "api_key": api_key,
        "language": "de-DE",
        "append_to_response": "credits,videos",
        "include_video_language": "de-DE,en-US",
    }
    try:
        response = requests.get(api_url, params=params, timeout=20)
        response.raise_for_status()
    except requests.RequestException as exc:
        app.logger.warning("TMDB detail request failed for %s: %s", tmdb_id, exc)
        return {}

    payload = response.json()
    cast_entries = payload.get("credits", {}).get("cast", [])
    cast_details: List[dict] = []
    for member in cast_entries:
        name = member.get("name")
        if not name:
            continue
        cast_details.append(
            {
                "name": name,
                "character": member.get("character"),
                "profile_path": member.get("profile_path"),
            }
        )
        if len(cast_details) >= 12:
            break

    trailer_payload = {}
    for video in payload.get("videos", {}).get("results", []):
        if not video.get("key") or not video.get("site"):
            continue
        if video.get("site") != "YouTube":
            continue
        video_type = (video.get("type") or "").lower()
        if video_type in {"trailer", "teaser"}:
            trailer_payload = {
                "site": video.get("site"),
                "key": video.get("key"),
                "name": video.get("name"),
                "official": video.get("official", False),
            }
            if video_type == "trailer":
                break

    return {
        "runtime": payload.get("runtime"),
        "genres": [genre.get("name") for genre in payload.get("genres", []) if genre.get("name")],
        "tagline": payload.get("tagline"),
        "cast": cast_details,
        "trailer": trailer_payload,
    }


def build_tmdb_image(path: Optional[str], size: str = "w500") -> Optional[str]:
    if not path:
        return None
    return f"https://image.tmdb.org/t/p/{size}{path}"


_TRAILING_DESCRIPTOR_PATTERN = re.compile(
    r"(?i)\b(stream|online|anschauen|kostenlos|gratis|hd|ganzer\s+film|full\s+movie|german|deutsch|kino)\b.*$"
)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _extract_title_and_year(raw_title: str) -> Tuple[str, Optional[int]]:
    if not raw_title:
        return "", None

    title = raw_title.strip()
    year_match = re.search(r"(19|20)\d{2}", title)
    year: Optional[int] = None
    if year_match:
        year = int(year_match.group())
        title = title[: year_match.start()]

    title = _TRAILING_DESCRIPTOR_PATTERN.sub("", title).strip(" -:|()[]")
    title = re.sub(r"\s+", " ", title).strip()
    if not title:
        title = raw_title.strip()

    return title, year


def search_tmdb_by_title(raw_title: str) -> Optional[dict]:
    api_key = get_tmdb_api_key()
    if not api_key:
        return None

    base_title, year = _extract_title_and_year(raw_title)
    normalized_base = _normalize_text(base_title) if base_title else ""

    attempts: List[Tuple[str, Optional[int]]] = []
    if base_title:
        attempts.append((base_title, year))
        attempts.append((base_title, None))
    stripped = raw_title.strip()
    if stripped:
        attempts.append((stripped, year))
        attempts.append((stripped, None))

    seen: Set[Tuple[str, Optional[int]]] = set()

    for query, year_hint in attempts:
        clean_query = query.strip()
        if not clean_query:
            continue

        key = (clean_query.lower(), year_hint)
        if key in seen:
            continue
        seen.add(key)

        params = {
            "api_key": api_key,
            "language": "de-DE",
            "query": clean_query,
            "include_adult": "false",
        }
        if year_hint:
            params["year"] = year_hint

        try:
            response = requests.get(
                "https://api.themoviedb.org/3/search/movie",
                params=params,
                timeout=20,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            app.logger.warning("TMDB search failed for %s: %s", clean_query, exc)
            continue

        results = response.json().get("results") or []
        if not results:
            continue

        if year_hint:
            for candidate in results:
                release_year = (candidate.get("release_date") or "")[:4]
                if release_year.isdigit() and int(release_year) == year_hint:
                    return candidate

        if normalized_base:
            for candidate in results:
                candidate_title = candidate.get("title") or candidate.get("original_title") or ""
                if _normalize_text(candidate_title) == normalized_base:
                    return candidate

        return results[0]

    return None


def _apply_tmdb_metadata(movie: Movie, tmdb_data: dict) -> None:
    movie.title = (
        tmdb_data.get("title")
        or tmdb_data.get("name")
        or tmdb_data.get("original_title")
        or movie.title
    )
    movie.overview = tmdb_data.get("overview")
    movie.poster_path = tmdb_data.get("poster_path")
    movie.backdrop_path = tmdb_data.get("backdrop_path")
    movie.release_date = tmdb_data.get("release_date")
    movie.rating = tmdb_data.get("vote_average")


def upsert_movies(tmdb_movies: List[dict]) -> List[Movie]:
    """Store TMDB movies in the database, avoiding duplicates."""
    stored_movies: List[Movie] = []
    for entry in tmdb_movies:
        movie = Movie.query.filter_by(tmdb_id=entry["id"]).first()
        if movie is None:
            movie = Movie(tmdb_id=entry["id"], title=entry.get("title", "Unbekannt"))
        movie.overview = entry.get("overview")
        movie.poster_path = entry.get("poster_path")
        movie.backdrop_path = entry.get("backdrop_path")
        movie.release_date = entry.get("release_date")
        movie.rating = entry.get("vote_average")
        db.session.add(movie)
        stored_movies.append(movie)
    db.session.commit()
    return stored_movies


def _generate_placeholder_tmdb_id() -> int:
    """Return a unique negative TMDB id for locally scraped titles."""
    lowest_placeholder = (
        db.session.query(func.min(Movie.tmdb_id))
        .filter(Movie.tmdb_id < 0)
        .scalar()
    )
    if lowest_placeholder is None:
        return -1
    return lowest_placeholder - 1


def attach_streaming_link(
    movie_title: str,
    streaming_url: str,
    mirror_info: Optional[str] = None,
    source_name: str = "Unbekannt",
) -> StreamingLink:
    """Create or update a streaming link for a movie based on its title."""
    normalized_title = (movie_title or "").strip()
    movie: Optional[Movie] = None
    base_title, _ = _extract_title_and_year(normalized_title)
    if normalized_title:
        movie = (
            Movie.query.filter(func.lower(Movie.title) == normalized_title.lower())
            .first()
        )

    if movie is None and base_title and base_title.lower() != normalized_title.lower():
        movie = (
            Movie.query.filter(func.lower(Movie.title) == base_title.lower())
            .first()
        )

    if movie is None and base_title and len(base_title) >= 3:
        safe_title = base_title.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        like_pattern = f"%{safe_title}%"
        movie = (
            Movie.query.filter(Movie.title.ilike(like_pattern, escape="\\"))
            .order_by(func.length(Movie.title))
            .first()
        )

    tmdb_entry: Optional[dict] = None
    if normalized_title and (movie is None or movie.tmdb_id < 0):
        tmdb_entry = search_tmdb_by_title(normalized_title)

    if tmdb_entry:
        existing_tmdb_movie = Movie.query.filter_by(tmdb_id=tmdb_entry["id"]).first()
        if existing_tmdb_movie and movie and existing_tmdb_movie.id != movie.id:
            for existing_link in list(movie.streaming_links):
                existing_link.movie = existing_tmdb_movie
            db.session.delete(movie)
            movie = existing_tmdb_movie
        elif existing_tmdb_movie:
            movie = existing_tmdb_movie
        else:
            if movie is None:
                movie = Movie(
                    tmdb_id=tmdb_entry["id"],
                    title=(
                        tmdb_entry.get("title")
                        or tmdb_entry.get("name")
                        or normalized_title
                        or "Unbekannt"
                    ),
                )
            elif movie.tmdb_id < 0:
                movie.tmdb_id = tmdb_entry["id"]

        if movie is None:
            movie = Movie(tmdb_id=tmdb_entry["id"], title=normalized_title or "Unbekannt")

        _apply_tmdb_metadata(movie, tmdb_entry)
    elif movie is None:
        fallback_title = normalized_title or movie_title or "Unbekannt"
        movie = Movie(tmdb_id=_generate_placeholder_tmdb_id(), title=fallback_title)

    db.session.add(movie)
    db.session.flush()

    link = StreamingLink.query.filter_by(movie_id=movie.id, url=streaming_url).first()
    if link is None:
        link = StreamingLink(
            movie=movie,
            url=streaming_url,
            source_name=source_name,
            mirror_info=mirror_info,
        )
    else:
        link.source_name = source_name
        link.mirror_info = mirror_info

    db.session.add(link)
    db.session.commit()
    return link


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _append_scraper_log(provider: str, message: str, level: str = "info") -> None:
    try:
        _initialize_scraper_state(provider)
    except ValueError:
        return

    entry = {"timestamp": _now_iso(), "message": message, "level": level}
    with SCRAPER_STATUS_LOCK:
        SCRAPER_LOG[provider].append(entry)
        SCRAPER_STATUS[provider]["last_update"] = entry["timestamp"]


def _set_scraper_status(provider: str, **kwargs) -> None:
    try:
        label = _initialize_scraper_state(provider)
    except ValueError:
        return

    timestamp = _now_iso()
    with SCRAPER_STATUS_LOCK:
        status = SCRAPER_STATUS[provider]
        status.update(kwargs)
        status["provider"] = provider
        status.setdefault("provider_label", label)
        status["last_update"] = timestamp


def _collect_scraper_status(provider: str) -> dict:
    try:
        label = _initialize_scraper_state(provider)
    except ValueError:
        return {}

    with SCRAPER_STATUS_LOCK:
        status = dict(SCRAPER_STATUS.get(provider, {}))
        log_entries = list(SCRAPER_LOG.get(provider, ()))

    status.setdefault("provider", provider)
    status.setdefault("provider_label", label)

    total_pages = status.get("total_pages") or 0
    processed_pages = status.get("processed_pages") or 0

    if total_pages:
        progress = max(0.0, min(100.0, (processed_pages / total_pages) * 100.0))
        progress_mode = "determinate"
    else:
        progress = 0.0
        progress_mode = "indeterminate" if status.get("running") else "idle"

    if not status.get("next_page"):
        status["next_page"] = get_scraper_int_setting(provider, "next_page", 1)
    if not status.get("last_page"):
        last_page = get_scraper_int_setting(provider, "last_page", 0)
        status["last_page"] = last_page or None

    status["progress"] = progress
    status["progress_mode"] = progress_mode
    status["log"] = log_entries
    return status


def get_scraper_status(provider: Optional[str] = None) -> dict:
    if provider:
        return _collect_scraper_status(provider)

    statuses: dict[str, dict] = {}
    for scraper in SCRAPER_MANAGER.available_providers():
        statuses[scraper.name] = _collect_scraper_status(scraper.name)
    return statuses


def _start_scraper(provider: str, start_page: int) -> bool:
    _initialize_scraper_state(provider)

    now_iso = _now_iso()
    scraper = SCRAPER_MANAGER.get_scraper(provider)
    if scraper is None:
        _append_scraper_log(provider, f"Unbekannter Scraper: {provider}", "error")
        return False

    with SCRAPER_STATUS_LOCK:
        existing_thread = SCRAPER_THREADS.get(provider)
        if existing_thread and existing_thread.is_alive():
            return False

        status = SCRAPER_STATUS[provider]
        status.update(
            {
                "running": True,
                "start_page": start_page,
                "current_page": start_page,
                "next_page": start_page,
                "last_page": None,
                "processed_pages": 0,
                "processed_links": 0,
                "last_title": None,
                "message": "Scraper wird gestartet…",
                "error": None,
                "started_at": now_iso,
                "finished_at": None,
                "last_update": now_iso,
            }
        )
        SCRAPER_LOG[provider].clear()

    _append_scraper_log(
        provider,
        f"{scraper.label}-Scraper gestartet (ab Seite {start_page}).",
    )

    thread = Thread(
        target=_run_scraper,
        args=(provider, start_page),
        daemon=True,
    )
    with SCRAPER_STATUS_LOCK:
        SCRAPER_THREADS[provider] = thread
    thread.start()
    return True


def _run_scraper(provider: str, start_page: int) -> None:
    processed_links = 0
    processed_pages = 0
    page = start_page
    last_completed_page: Optional[int] = None
    finished_naturally = False
    scraper = SCRAPER_MANAGER.get_scraper(provider)
    if scraper is None:
        _append_scraper_log(provider, f"Unbekannter Scraper: {provider}", "error")
        _set_scraper_status(
            provider,
            running=False,
            error="Scraper nicht gefunden.",
            message="Scraper nicht verfügbar.",
            finished_at=_now_iso(),
        )
        return
    provider_label = scraper.label

    try:
        with app.app_context():
            while True:
                _append_scraper_log(
                    provider, f"[{provider_label}] Seite {page} wird geladen…"
                )
                _set_scraper_status(
                    provider,
                    current_page=page,
                    next_page=page,
                    message=f"{provider_label}: Seite {page} wird geladen…",
                    error=None,
                )

                def progress_callback(entry: ScraperResult) -> None:
                    title = entry.title or "Unbekannt"
                    _set_scraper_status(
                        provider,
                        last_title=title,
                        message=f"{provider_label}: Gefunden {title}",
                        current_page=page,
                    )

                try:
                    entries = list(
                        SCRAPER_MANAGER.scrape_page(
                            provider,
                            page,
                            progress_callback=progress_callback,
                        )
                    )
                except Exception as exc:  # pragma: no cover - network errors are not predictable
                    db.session.rollback()
                    error_text = (
                        f"[{provider_label}] Fehler beim Laden von Seite {page}: {exc}"
                    )
                    _append_scraper_log(provider, error_text, "error")
                    _set_scraper_status(
                        provider,
                        running=False,
                        error=str(exc),
                        message=f"{provider_label}: Fehler beim Laden einer Seite.",
                        finished_at=_now_iso(),
                        current_page=page,
                        next_page=page,
                    )
                    return

                if not entries:
                    finished_naturally = True
                    last_page_value = (
                        last_completed_page
                        if last_completed_page is not None
                        else max(0, page - 1)
                    )
                    set_scraper_setting(provider, "last_page", last_page_value)
                    set_scraper_setting(provider, "next_page", 1)
                    _append_scraper_log(
                        provider,
                        f"[{provider_label}] Keine weiteren Einträge gefunden. Scraper beendet.",
                        "success",
                    )
                    _set_scraper_status(
                        provider,
                        message=f"{provider_label}: Keine weiteren Einträge gefunden.",
                        last_title=None,
                        current_page=page,
                        next_page=1,
                        last_page=last_page_value or None,
                    )
                    break

                for entry in entries:
                    title = entry.title or "Unbekannt"
                    _set_scraper_status(
                        provider,
                        last_title=title,
                        message=f"{provider_label}: Speichere {title}",
                        error=None,
                    )
                    try:
                        existing_link = StreamingLink.query.filter_by(
                            url=entry.streaming_url
                        ).first()
                        if existing_link:
                            updated = False
                            if entry.source_name and (
                                existing_link.source_name != entry.source_name
                            ):
                                existing_link.source_name = entry.source_name
                                updated = True
                            if entry.mirror_info != existing_link.mirror_info:
                                existing_link.mirror_info = entry.mirror_info
                                updated = True
                            if updated:
                                db.session.add(existing_link)
                                db.session.commit()
                                _append_scraper_log(
                                    provider,
                                    f"[{provider_label}] Link aktualisiert: {title}",
                                    "success",
                                )
                            else:
                                _append_scraper_log(
                                    provider,
                                    f"[{provider_label}] Link bereits vorhanden: {title}",
                                    "info",
                                )
                            continue

                        attach_streaming_link(
                            entry.title or title,
                            entry.streaming_url,
                            entry.mirror_info,
                            entry.source_name,
                        )
                        processed_links += 1
                        _set_scraper_status(
                            provider, processed_links=processed_links, error=None
                        )
                        _append_scraper_log(
                            provider,
                            f"[{provider_label}] Link gespeichert: {title}",
                            "success",
                        )
                    except Exception as exc:  # pragma: no cover - depends on DB state
                        db.session.rollback()
                        _append_scraper_log(
                            provider,
                            f"[{provider_label}] Fehler beim Speichern von {title}: {exc}",
                            "error",
                        )
                        _set_scraper_status(
                            provider,
                            error=str(exc),
                            message=f"{provider_label}: Fehler bei {title}",
                        )

                processed_pages += 1
                last_completed_page = page
                next_page = page + 1
                set_scraper_setting(provider, "last_page", page)
                set_scraper_setting(provider, "next_page", next_page)
                _set_scraper_status(
                    provider,
                    processed_pages=processed_pages,
                    message=f"{provider_label}: Seite {page} abgeschlossen",
                    error=None,
                    current_page=page,
                    next_page=next_page,
                    last_page=page,
                    processed_links=processed_links,
                )
                _append_scraper_log(
                    provider,
                    f"[{provider_label}] Seite {page} abgeschlossen. Nächste Seite: {next_page}.",
                )

                page = next_page
                time.sleep(1)

            if finished_naturally:
                final_last_page = (
                    last_completed_page if last_completed_page is not None else max(0, page - 1)
                )
                _set_scraper_status(
                    provider,
                    running=False,
                    message=f"{provider_label}: Alle Seiten verarbeitet.",
                    finished_at=_now_iso(),
                    current_page=last_completed_page or page,
                    next_page=1,
                    last_page=final_last_page or None,
                    processed_pages=processed_pages,
                    processed_links=processed_links,
                )
                _append_scraper_log(
                    provider,
                    f"[{provider_label}] Scraper abgeschlossen. Verarbeitete Seiten: {processed_pages}, neue Links: {processed_links}.",
                    "success",
                )
    finally:
        db.session.remove()
        with SCRAPER_STATUS_LOCK:
            SCRAPER_THREADS.pop(provider, None)
            still_running = SCRAPER_STATUS.get(provider, {}).get("running")
        if still_running:
            _set_scraper_status(
                provider,
                running=False,
                message=f"{provider_label}: Scraper angehalten.",
                finished_at=_now_iso(),
            )


def _start_multiple_scrapers(start_pages: Optional[dict[str, int]] = None) -> dict[str, bool]:
    pages = start_pages or {}
    results: dict[str, bool] = {}
    for scraper in SCRAPER_MANAGER.available_providers():
        provider = scraper.name
        page = pages.get(provider)
        if page is None:
            page = get_scraper_int_setting(provider, "next_page", 1)
        else:
            set_scraper_setting(provider, "next_page", page)
            set_scraper_setting(provider, "last_page", max(0, page - 1))
        results[provider] = _start_scraper(provider, page)
    return results


def _extract_start_pages(payload: dict) -> tuple[dict[str, int], dict[str, str]]:
    start_pages_raw = payload.get("start_pages")
    if not isinstance(start_pages_raw, dict):
        return {}, {}

    start_pages: dict[str, int] = {}
    errors: dict[str, str] = {}
    for provider, value in start_pages_raw.items():
        try:
            page = int(value)
        except (TypeError, ValueError):
            errors[provider] = "Ungültige Startseite."
            continue
        if page < 1:
            errors[provider] = "Startseite muss größer als 0 sein."
            continue
        start_pages[provider] = page
    return start_pages, errors


def build_library_context() -> dict:
    valid_filter = movie_has_valid_streaming_link()
    popular_movies = (
        Movie.query.filter(valid_filter)
        .order_by(Movie.rating.desc().nullslast())
        .limit(20)
        .all()
    )
    recent_movies = (
        Movie.query.filter(valid_filter)
        .order_by(Movie.created_at.desc())
        .limit(20)
        .all()
    )
    linked_movies = (
        Movie.query.filter(valid_filter)
        .order_by(Movie.title.asc())
        .limit(20)
        .all()
    )

    categories = {
        "Beliebt": popular_movies,
        "Neu hinzugefügt": recent_movies,
    }

    film_sections = [
        {"title": "Top bewertet", "items": popular_movies},
        {"title": "Neu hinzugefügt", "items": recent_movies},
    ]
    if linked_movies:
        film_sections.append({"title": "Mit Streaming Links", "items": linked_movies})

    series_sections: List[dict] = []
    scraped = StreamingLink.query.order_by(StreamingLink.id.desc()).limit(25).all()

    hero_movies = popular_movies[:5]

    now_playing_movies: List[Movie] = []
    try:
        now_playing_payload = fetch_tmdb_movies("now_playing")
    except Exception as exc:  # pragma: no cover - network errors
        app.logger.warning("Failed to load TMDB now playing titles: %s", exc)
    else:
        tmdb_ids = [entry.get("id") for entry in now_playing_payload if isinstance(entry.get("id"), int)]
        if tmdb_ids:
            order_mapping = {tmdb_id: index for index, tmdb_id in enumerate(tmdb_ids)}
            order_whens = [(tmdb_id, position) for tmdb_id, position in order_mapping.items()]
            order_case = case(*order_whens, value=Movie.tmdb_id)
            now_playing_movies = (
                Movie.query.filter(valid_filter, Movie.tmdb_id.in_(tmdb_ids))
                .order_by(order_case)
                .limit(20)
                .all()
            )
            now_playing_movies.sort(key=lambda movie: order_mapping.get(movie.tmdb_id, len(order_mapping)))

    return {
        "categories": categories,
        "film_sections": film_sections,
        "series_sections": series_sections,
        "scraped": scraped,
        "hero_movies": hero_movies,
        "now_playing_movies": now_playing_movies,
    }


@app.route("/")
def index():
    context = build_library_context()
    return render_template(
        "start.html",
        active_page="start",
        show_detail_panel=True,
        page_title="Medusa – Startseite",
        **context,
    )


@app.route("/filme")
def filme():
    context = build_library_context()
    return render_template(
        "filme.html",
        active_page="filme",
        show_detail_panel=True,
        show_all=False,
        page_title="Medusa – Filme",
        **context,
    )


@app.route("/filme/alle")
def filme_all():
    valid_filter = movie_has_valid_streaming_link()
    total_movies = Movie.query.filter(valid_filter).count()
    average_rating_value = db.session.query(func.avg(Movie.rating)).filter(valid_filter).scalar()
    average_rating = (
        round(float(average_rating_value), 1) if average_rating_value is not None else None
    )
    linked_movies_count = total_movies

    latest_movie = (
        Movie.query.filter(valid_filter).order_by(Movie.created_at.desc()).first()
    )
    latest_movie_added = (
        latest_movie.created_at.strftime("%d.%m.%Y")
        if latest_movie and latest_movie.created_at
        else None
    )

    highlight_movie = (
        Movie.query.filter(valid_filter)
        .order_by(Movie.rating.desc().nullslast(), Movie.created_at.desc())
        .first()
    )
    if highlight_movie:
        highlight_image = build_tmdb_image(highlight_movie.backdrop_path, "w780") or build_tmdb_image(
            highlight_movie.poster_path
        )
    else:
        highlight_image = None

    return render_template(
        "filme_all.html",
        active_page="filme",
        show_detail_panel=True,
        load_all_movies=True,
        page_title="Medusa – Alle Filme",
        total_movies=total_movies,
        average_rating=average_rating,
        linked_movies_count=linked_movies_count,
        latest_movie=latest_movie,
        latest_movie_added=latest_movie_added,
        highlight_movie=highlight_movie,
        highlight_image=highlight_image,
    )


@app.route("/serien")
def serien():
    context = build_library_context()
    return render_template(
        "serien.html",
        active_page="serien",
        show_detail_panel=True,
        page_title="Medusa – Serien",
        **context,
    )


@app.route("/scraper")
def scraper_view():
    context = build_library_context()
    providers = SCRAPER_MANAGER.available_providers()
    context["scraper_providers"] = providers
    context["scraper_statuses"] = get_scraper_status()
    context["scraper_settings"] = {
        provider.name: {
            "next_page": get_scraper_int_setting(provider.name, "next_page", 1),
            "last_page": get_scraper_int_setting(provider.name, "last_page", 0),
        }
        for provider in providers
    }
    return render_template(
        "scraper.html",
        active_page="scraper",
        show_detail_panel=True,
        page_title="Medusa – Scraper",
        **context,
    )


@app.route("/einstellungen")
def settings_view():
    providers = SCRAPER_MANAGER.available_providers()
    return render_template(
        "settings.html",
        active_page="settings",
        show_detail_panel=False,
        page_title="Medusa – Einstellungen",
        scraper_providers=providers,
        scraper_settings={
            provider.name: {
                "next_page": get_scraper_int_setting(provider.name, "next_page", 1),
                "last_page": get_scraper_int_setting(provider.name, "last_page", 0),
            }
            for provider in providers
        },
    )


@app.route("/api/movies")
def api_movies():
    movies = (
        Movie.query.filter(movie_has_valid_streaming_link())
        .order_by(Movie.rating.desc().nullslast())
        .all()
    )
    return jsonify([movie.to_dict() for movie in movies])


def _get_movie_runtime(movie: Movie) -> Optional[int]:
    tmdb_id = movie.tmdb_id
    if not tmdb_id:
        return None
    with MOVIE_RUNTIME_CACHE_LOCK:
        if tmdb_id in MOVIE_RUNTIME_CACHE:
            return MOVIE_RUNTIME_CACHE[tmdb_id]
    details = fetch_tmdb_details(tmdb_id)
    runtime = details.get("runtime") if isinstance(details, dict) else None
    with MOVIE_RUNTIME_CACHE_LOCK:
        MOVIE_RUNTIME_CACHE[tmdb_id] = runtime
    return runtime


@app.route("/api/movies/runtime", methods=["POST"])
def api_movies_runtime():
    payload = request.get_json(silent=True) or {}
    movie_ids = payload.get("movie_ids")
    if not isinstance(movie_ids, list):
        return jsonify({"success": False, "message": "movie_ids must be provided as a list."}), 400

    try:
        normalized_ids = {int(movie_id) for movie_id in movie_ids}
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid movie id provided."}), 400

    if not normalized_ids:
        return jsonify({"success": True, "items": []})

    movies = (
        Movie.query.filter(movie_has_valid_streaming_link(), Movie.id.in_(normalized_ids))
        .order_by(Movie.id.asc())
        .all()
    )

    items = []
    for movie in movies:
        runtime = _get_movie_runtime(movie)
        items.append({"id": movie.id, "runtime": runtime})

    return jsonify({"success": True, "items": items})


def _escape_search_query(raw_query: str) -> str:
    """Escape SQL wildcard characters in user provided search strings."""

    return (
        raw_query.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
        .strip()
    )


@app.route("/api/search")
def api_search():
    raw_query = (request.args.get("q") or "").strip()
    if len(raw_query) < 2:
        return jsonify({"success": True, "query": raw_query, "results": []})

    safe_query = _escape_search_query(raw_query)
    like_pattern = f"%{safe_query}%"

    valid_filter = movie_has_valid_streaming_link()

    movies = (
        Movie.query.filter(valid_filter)
        .filter(
            or_(
                Movie.title.ilike(like_pattern, escape="\\"),
                Movie.overview.ilike(like_pattern, escape="\\"),
                Movie.release_date.ilike(like_pattern, escape="\\"),
                Movie.streaming_links.any(
                    StreamingLink.source_name.ilike(like_pattern, escape="\\")
                ),
                Movie.streaming_links.any(
                    StreamingLink.url.ilike(like_pattern, escape="\\")
                ),
            )
        )
        .order_by(
            Movie.rating.desc().nullslast(),
            Movie.created_at.desc().nullslast(),
            Movie.title.asc(),
        )
        .limit(20)
        .all()
    )

    results = []
    for movie in movies:
        poster_url = build_tmdb_image(movie.poster_path)
        backdrop_url = build_tmdb_image(movie.backdrop_path, "w780")
        streams = [link for link in movie.streaming_links if (link.url or "").strip()]
        results.append(
            {
                "id": movie.id,
                "title": movie.title,
                "overview": movie.overview,
                "release_date": movie.release_date,
                "rating": movie.rating,
                "poster_url": poster_url,
                "backdrop_url": backdrop_url,
                "streams": len(streams),
            }
        )

    return jsonify({"success": True, "query": raw_query, "results": results})


@app.route("/api/movies/<int:movie_id>")
def api_movie_detail(movie_id: int):
    movie = Movie.query.get_or_404(movie_id)
    tmdb_details = fetch_tmdb_details(movie.tmdb_id)

    poster_url = build_tmdb_image(movie.poster_path)
    backdrop_url = build_tmdb_image(movie.backdrop_path, "w1280")

    movie_payload = {
        "id": movie.id,
        "title": movie.title,
        "overview": movie.overview,
        "poster_url": poster_url,
        "backdrop_url": backdrop_url,
        "release_date": movie.release_date,
        "rating": movie.rating,
        "runtime": tmdb_details.get("runtime"),
        "genres": tmdb_details.get("genres", []),
        "tagline": tmdb_details.get("tagline"),
        "cast": tmdb_details.get("cast", []),
        "streaming_links": [link.to_dict() for link in movie.streaming_links],
        "trailer": tmdb_details.get("trailer"),
    }

    return jsonify({"success": True, "movie": movie_payload})


@app.route("/api/scrape/<provider>", methods=["POST"])
@app.route("/api/scrape/kinox", methods=["POST"], defaults={"provider": "kinox"})
def api_scrape_provider(provider: str):
    provider = (provider or "").lower()
    scraper = SCRAPER_MANAGER.get_scraper(provider)
    if scraper is None:
        return jsonify({"success": False, "message": "Unbekannter Scraper."}), 404

    data = request.get_json(silent=True) or {}
    start_page = data.get("from_page") or data.get("start_page")
    if start_page is not None:
        try:
            start_page = int(start_page)
        except (TypeError, ValueError):
            return jsonify({"success": False, "message": "Ungültige Startseite."}), 400
        if start_page < 1:
            return jsonify({"success": False, "message": "Startseite muss größer als 0 sein."}), 400
        set_scraper_setting(provider, "next_page", start_page)
        set_scraper_setting(provider, "last_page", max(0, start_page - 1))
    else:
        start_page = get_scraper_int_setting(provider, "next_page", 1)

    started = _start_scraper(provider, start_page)
    status = get_scraper_status()
    message = (
        f"{scraper.label}-Scraper gestartet." if started else f"{scraper.label}-Scraper läuft bereits."
    )
    started_map = {provider: started}
    return jsonify(
        {
            "success": True,
            "status": status,
            "started": started_map,
            "started_any": any(started_map.values()),
            "message": message,
        }
    )


@app.route("/api/scrape/all", methods=["POST"])
def api_scrape_all():
    data = request.get_json(silent=True) or {}
    start_pages, errors = _extract_start_pages(data)
    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    started_map = _start_multiple_scrapers(start_pages)
    status = get_scraper_status()
    started_providers = [
        SCRAPER_MANAGER.get_scraper(provider).label
        for provider, started in started_map.items()
        if started and SCRAPER_MANAGER.get_scraper(provider)
    ]

    if started_providers:
        if len(started_providers) == len(started_map):
            message = "Alle Scraper laufen im Hintergrund."
        else:
            message = f"Scraper gestartet: {', '.join(started_providers)}."
    else:
        message = "Scraper laufen bereits."

    return jsonify(
        {
            "success": True,
            "status": status,
            "started": started_map,
            "started_any": any(started_map.values()),
            "message": message,
        }
    )


@app.route("/api/scrape/status")
def api_scrape_status():
    return jsonify({"success": True, "status": get_scraper_status()})


@app.route("/api/reset/scraped", methods=["POST"])
def api_reset_scraped():
    placeholder_count = Movie.query.filter(Movie.tmdb_id < 0).count()
    tmdb_count = Movie.query.filter(Movie.tmdb_id > 0).count()

    removed_links = StreamingLink.query.delete(synchronize_session=False)
    removed_movies = Movie.query.delete(synchronize_session=False)
    db.session.commit()

    return jsonify(
        {
            "success": True,
            "removed_links": removed_links,
            "removed_movies": removed_movies,
            "removed_placeholder_movies": placeholder_count,
            "removed_tmdb_movies": tmdb_count,
        }
    )


@app.route("/api/tmdb/<category>")
def api_tmdb(category: str):
    page = int(request.args.get("page", 1))
    tmdb_movies = fetch_tmdb_movies(category=category, page=page)
    movies = upsert_movies(tmdb_movies)
    return jsonify({"success": True, "count": len(movies)})


@app.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    if request.method == "GET":
        scraper_settings = {
            provider.name: {
                "next_page": get_scraper_int_setting(provider.name, "next_page", 1),
                "last_page": get_scraper_int_setting(provider.name, "last_page", 0),
            }
            for provider in SCRAPER_MANAGER.available_providers()
        }
        return jsonify(
            {
                "tmdb_api_key": get_tmdb_api_key(),
                "scrapers": scraper_settings,
            }
        )

    data = request.get_json(silent=True) or {}

    response_payload = {}
    errors = {}

    tmdb_api_key = (data.get("tmdb_api_key") or "").strip()
    if tmdb_api_key:
        set_setting("tmdb_api_key", tmdb_api_key)
        response_payload["tmdb_api_key"] = tmdb_api_key
    elif data.get("tmdb_api_key") is not None:
        errors["tmdb_api_key"] = "TMDB API Key darf nicht leer sein."

    scraper_updates: dict[str, dict] = {}

    scrapers_data = data.get("scrapers")
    if isinstance(scrapers_data, dict):
        for provider, values in scrapers_data.items():
            if SCRAPER_MANAGER.get_scraper(provider) is None:
                continue
            next_page_value = (values or {}).get("next_page")
            if next_page_value is None:
                continue
            try:
                next_page = int(next_page_value)
            except (TypeError, ValueError):
                errors[f"{provider}_next_page"] = "Ungültige Zahl."
                continue
            if next_page < 1:
                errors[f"{provider}_next_page"] = "Wert muss größer oder gleich 1 sein."
                continue
            set_scraper_setting(provider, "next_page", next_page)
            set_scraper_setting(provider, "last_page", max(0, next_page - 1))
            scraper_updates[provider] = {
                "next_page": next_page,
                "last_page": max(0, next_page - 1),
            }

    # Backwards compatibility for legacy fields
    for scraper in SCRAPER_MANAGER.available_providers():
        key = f"{scraper.name}_next_page"
        if key not in data:
            continue
        try:
            next_page = int(data[key])
        except (TypeError, ValueError):
            errors[key] = "Ungültige Zahl."
            continue
        if next_page < 1:
            errors[key] = "Wert muss größer oder gleich 1 sein."
            continue
        set_scraper_setting(scraper.name, "next_page", next_page)
        set_scraper_setting(scraper.name, "last_page", max(0, next_page - 1))
        scraper_updates[scraper.name] = {
            "next_page": next_page,
            "last_page": max(0, next_page - 1),
        }

    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    if scraper_updates:
        response_payload["scrapers"] = scraper_updates

    return jsonify({"success": True, "settings": response_payload})


ensure_database()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
