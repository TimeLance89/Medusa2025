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

from scrapers import BaseScraper, ScraperResult, get_scraper_manager


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


class Series(db.Model):
    __tablename__ = "series"

    id = db.Column(db.Integer, primary_key=True)
    tmdb_id = db.Column(db.Integer, unique=True, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    overview = db.Column(db.Text)
    poster_path = db.Column(db.String(255))
    backdrop_path = db.Column(db.String(255))
    first_air_date = db.Column(db.String(32))
    last_air_date = db.Column(db.String(32))
    rating = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    seasons = db.relationship(
        "SeriesSeason",
        back_populates="series",
        cascade="all, delete-orphan",
        order_by="SeriesSeason.season_number",
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "tmdb_id": self.tmdb_id,
            "name": self.name,
            "overview": self.overview,
            "poster_path": self.poster_path,
            "backdrop_path": self.backdrop_path,
            "first_air_date": self.first_air_date,
            "last_air_date": self.last_air_date,
            "rating": self.rating,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class SeriesSeason(db.Model):
    __tablename__ = "series_seasons"

    id = db.Column(db.Integer, primary_key=True)
    series_id = db.Column(db.Integer, db.ForeignKey("series.id"), nullable=False)
    tmdb_id = db.Column(db.Integer)
    season_number = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(255))
    overview = db.Column(db.Text)
    poster_path = db.Column(db.String(255))
    air_date = db.Column(db.String(32))
    episode_count = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    series = db.relationship("Series", back_populates="seasons")
    episodes = db.relationship(
        "SeriesEpisode",
        back_populates="season",
        cascade="all, delete-orphan",
        order_by="SeriesEpisode.episode_number",
    )

    __table_args__ = (
        db.UniqueConstraint(
            "series_id", "season_number", name="uq_series_season_number"
        ),
    )


class SeriesEpisode(db.Model):
    __tablename__ = "series_episodes"

    id = db.Column(db.Integer, primary_key=True)
    season_id = db.Column(db.Integer, db.ForeignKey("series_seasons.id"), nullable=False)
    tmdb_id = db.Column(db.Integer)
    episode_number = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(255))
    overview = db.Column(db.Text)
    still_path = db.Column(db.String(255))
    air_date = db.Column(db.String(32))
    runtime = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    season = db.relationship("SeriesSeason", back_populates="episodes")
    streaming_links = db.relationship(
        "EpisodeStreamingLink",
        back_populates="episode",
        cascade="all, delete-orphan",
        order_by="EpisodeStreamingLink.id",
    )

    __table_args__ = (
        db.UniqueConstraint(
            "season_id", "episode_number", name="uq_season_episode_number"
        ),
    )


class EpisodeStreamingLink(db.Model):
    __tablename__ = "episode_streaming_links"

    id = db.Column(db.Integer, primary_key=True)
    episode_id = db.Column(
        db.Integer, db.ForeignKey("series_episodes.id"), nullable=False
    )
    source_name = db.Column(db.String(120), nullable=False)
    url = db.Column(db.String(500), nullable=False)
    mirror_info = db.Column(db.String(120))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    episode = db.relationship("SeriesEpisode", back_populates="streaming_links")

    __table_args__ = (
        db.UniqueConstraint("episode_id", "url", name="uq_episode_stream"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "episode_id": self.episode_id,
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


def series_has_valid_streaming_link():
    return Series.seasons.any(
        SeriesSeason.episodes.any(
            SeriesEpisode.streaming_links.any(
                func.length(func.trim(EpisodeStreamingLink.url)) > 0
            )
        )
    )


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


def _get_scraper_categories(scraper: BaseScraper) -> Tuple[str, ...]:
    categories = getattr(scraper, "content_categories", ("movies",))
    if isinstance(categories, str):
        categories = (categories,)
    return tuple(
        category.lower()
        for category in categories
        if isinstance(category, str) and category
    )


def _describe_scraper_scope(
    scraper: BaseScraper, include_series: bool = False
) -> Optional[str]:
    categories = _get_scraper_categories(scraper)
    has_movies = "movies" in categories
    has_series = "series" in categories

    if include_series and has_movies and has_series:
        return "Filme & Serien"
    if has_series and not has_movies:
        return "Serien"
    if include_series and has_series:
        return "Serien"
    return None


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
        "content_scope": None,
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


def fetch_tmdb_series_details(tmdb_id: int) -> dict:
    api_key = get_tmdb_api_key()
    if not api_key or tmdb_id <= 0:
        return {}

    params = {
        "api_key": api_key,
        "language": "de-DE",
        "append_to_response": "aggregate_credits,videos",
        "include_video_language": "de-DE,en-US",
    }

    try:
        response = requests.get(
            f"https://api.themoviedb.org/3/tv/{tmdb_id}", params=params, timeout=20
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        app.logger.warning("TMDB series detail request failed for %s: %s", tmdb_id, exc)
        return {}

    payload = response.json()

    cast_entries = payload.get("aggregate_credits", {}).get("cast", [])
    cast_details: List[dict] = []
    for member in cast_entries:
        name = member.get("name")
        if not name:
            continue
        roles = member.get("roles") or []
        character_name = None
        for role in roles:
            character_name = role.get("character")
            if character_name:
                break
        cast_details.append(
            {
                "name": name,
                "character": character_name,
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

    seasons = payload.get("seasons") or []

    return {
        "name": payload.get("name") or payload.get("original_name"),
        "overview": payload.get("overview"),
        "poster_path": payload.get("poster_path"),
        "backdrop_path": payload.get("backdrop_path"),
        "first_air_date": payload.get("first_air_date"),
        "last_air_date": payload.get("last_air_date"),
        "rating": payload.get("vote_average"),
        "genres": [
            genre.get("name")
            for genre in payload.get("genres", [])
            if genre.get("name")
        ],
        "tagline": payload.get("tagline"),
        "cast": cast_details,
        "trailer": trailer_payload,
        "seasons": seasons,
        "status": payload.get("status"),
        "episode_run_time": payload.get("episode_run_time") or [],
    }


def fetch_tmdb_season_details(tmdb_id: int, season_number: int) -> List[dict]:
    api_key = get_tmdb_api_key()
    if not api_key or tmdb_id <= 0 or season_number < 0:
        return []

    params = {"api_key": api_key, "language": "de-DE"}
    try:
        response = requests.get(
            f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season_number}",
            params=params,
            timeout=20,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        app.logger.warning(
            "TMDB season request failed for %s S%02d: %s", tmdb_id, season_number, exc
        )
        return []

    payload = response.json()
    return payload.get("episodes", []) or []


def build_tmdb_image(path: Optional[str], size: str = "w500") -> Optional[str]:
    if not path:
        return None
    return f"https://image.tmdb.org/t/p/{size}{path}"


_TRAILING_DESCRIPTOR_PATTERN = re.compile(
    r"(?i)\b(stream|online|anschauen|kostenlos|gratis|hd|ganzer\s+film|full\s+movie|german|deutsch|kino)\b.*$"
)


_SERIES_EPISODE_PATTERN = re.compile(
    r"(?i)(?P<title>.+?)\s*S(?P<season>\d{1,2})E(?P<episode>\d{1,2})"
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


def _extract_series_metadata(raw_title: str) -> Tuple[str, Optional[int], Optional[int]]:
    if not raw_title:
        return "", None, None

    match = _SERIES_EPISODE_PATTERN.search(raw_title)
    if not match:
        title, _ = _extract_title_and_year(raw_title)
        return title, None, None

    title = match.group("title") or ""
    title = title.strip()
    season: Optional[int]
    episode: Optional[int]
    try:
        season = int(match.group("season"))
    except (TypeError, ValueError):
        season = None
    try:
        episode = int(match.group("episode"))
    except (TypeError, ValueError):
        episode = None

    if not title:
        title, _ = _extract_title_and_year(raw_title)

    return title, season, episode


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


def search_tmdb_series_by_title(raw_title: str) -> Optional[dict]:
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
            params["first_air_date_year"] = year_hint

        try:
            response = requests.get(
                "https://api.themoviedb.org/3/search/tv",
                params=params,
                timeout=20,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            app.logger.warning("TMDB series search failed for %s: %s", clean_query, exc)
            continue

        results = response.json().get("results") or []
        if not results:
            continue

        if year_hint:
            for candidate in results:
                air_date = (candidate.get("first_air_date") or "")[:4]
                if air_date.isdigit() and int(air_date) == year_hint:
                    return candidate

        if normalized_base:
            for candidate in results:
                candidate_title = candidate.get("name") or candidate.get("original_name") or ""
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


def _apply_tmdb_series_metadata(series: Series, tmdb_data: dict) -> None:
    series.name = (
        tmdb_data.get("name")
        or tmdb_data.get("original_name")
        or tmdb_data.get("title")
        or series.name
    )
    series.overview = tmdb_data.get("overview")
    series.poster_path = tmdb_data.get("poster_path")
    series.backdrop_path = tmdb_data.get("backdrop_path")
    series.first_air_date = tmdb_data.get("first_air_date")
    series.last_air_date = tmdb_data.get("last_air_date")
    series.rating = tmdb_data.get("vote_average")


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


def _generate_series_placeholder_tmdb_id() -> int:
    lowest_placeholder = (
        db.session.query(func.min(Series.tmdb_id))
        .filter(Series.tmdb_id < 0)
        .scalar()
    )
    if lowest_placeholder is None:
        return -1
    return lowest_placeholder - 1


def sync_series_with_tmdb(
    series: Series, tmdb_payload: Optional[dict] = None, *, include_episodes: bool = True
) -> None:
    if series.tmdb_id <= 0:
        return

    if tmdb_payload is None:
        tmdb_payload = fetch_tmdb_series_details(series.tmdb_id)

    if not tmdb_payload:
        return

    _apply_tmdb_series_metadata(series, tmdb_payload)

    seasons_payload = tmdb_payload.get("seasons") or []

    for season_entry in seasons_payload:
        try:
            season_number = int(season_entry.get("season_number"))
        except (TypeError, ValueError):
            continue
        if season_number < 0:
            continue

        season = (
            SeriesSeason.query.filter_by(
                series_id=series.id, season_number=season_number
            ).first()
        )
        if season is None:
            season = SeriesSeason(series=series, season_number=season_number)

        season.tmdb_id = season_entry.get("id")
        season.name = season_entry.get("name") or season.name
        season.overview = season_entry.get("overview")
        season.poster_path = season_entry.get("poster_path")
        season.air_date = season_entry.get("air_date")
        season.episode_count = season_entry.get("episode_count")
        db.session.add(season)
        db.session.flush()

        if not include_episodes:
            continue

        existing_episode_numbers = {
            episode.episode_number for episode in season.episodes
        }
        total_expected = season_entry.get("episode_count")
        should_fetch = not existing_episode_numbers
        if (
            isinstance(total_expected, int)
            and total_expected > 0
            and len(existing_episode_numbers) < total_expected
        ):
            should_fetch = True

        if not should_fetch:
            continue

        episodes_payload = fetch_tmdb_season_details(series.tmdb_id, season_number)
        for episode_entry in episodes_payload:
            try:
                episode_number = int(episode_entry.get("episode_number"))
            except (TypeError, ValueError):
                continue
            if episode_number < 0:
                continue

            episode = (
                SeriesEpisode.query.filter_by(
                    season_id=season.id, episode_number=episode_number
                ).first()
            )
            if episode is None:
                episode = SeriesEpisode(season=season, episode_number=episode_number)

            episode.tmdb_id = episode_entry.get("id")
            episode.name = episode_entry.get("name") or episode.name
            episode.overview = episode_entry.get("overview")
            episode.still_path = episode_entry.get("still_path")
            episode.air_date = episode_entry.get("air_date")
            runtime = episode_entry.get("runtime")
            episode.runtime = runtime if isinstance(runtime, int) else None
            db.session.add(episode)

    db.session.flush()


def attach_movie_streaming_link(
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


def attach_series_streaming_entry(entry: ScraperResult) -> Tuple[str, Optional[str]]:
    metadata = entry.metadata or {}
    raw_title = metadata.get("series_title") or metadata.get("title") or entry.title or ""
    series_title, parsed_season, parsed_episode = _extract_series_metadata(raw_title)

    season_value = metadata.get("season", parsed_season)
    episode_value = metadata.get("episode", parsed_episode)

    try:
        season_number = int(season_value)
    except (TypeError, ValueError):
        season_number = parsed_season

    try:
        episode_number = int(episode_value)
    except (TypeError, ValueError):
        episode_number = parsed_episode

    if season_number is None or episode_number is None:
        return "skipped", None

    normalized_title = (series_title or "").strip()
    if not normalized_title:
        normalized_title = (raw_title or "").strip()
    if not normalized_title:
        normalized_title = f"Unbekannte Serie"

    base_title, _ = _extract_title_and_year(normalized_title)

    series: Optional[Series] = None
    if normalized_title:
        series = (
            Series.query.filter(func.lower(Series.name) == normalized_title.lower())
            .first()
        )

    if series is None and base_title and base_title.lower() != normalized_title.lower():
        series = (
            Series.query.filter(func.lower(Series.name) == base_title.lower())
            .first()
        )

    if series is None and base_title:
        safe_title = (
            base_title.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        like_pattern = f"%{safe_title}%"
        series = (
            Series.query.filter(Series.name.ilike(like_pattern, escape="\\"))
            .order_by(func.length(Series.name))
            .first()
        )

    tmdb_entry: Optional[dict] = None
    if normalized_title and (series is None or series.tmdb_id < 0):
        tmdb_entry = search_tmdb_series_by_title(normalized_title)
        if not tmdb_entry and base_title and base_title != normalized_title:
            tmdb_entry = search_tmdb_series_by_title(base_title)

    if tmdb_entry:
        existing_tmdb_series = Series.query.filter_by(tmdb_id=tmdb_entry["id"]).first()
        if existing_tmdb_series and series and existing_tmdb_series.id != series.id:
            for season in list(series.seasons):
                season.series = existing_tmdb_series
            db.session.flush()
            db.session.delete(series)
            series = existing_tmdb_series
        elif existing_tmdb_series:
            series = existing_tmdb_series
        else:
            if series is None:
                series = Series(
                    tmdb_id=tmdb_entry["id"],
                    name=(
                        tmdb_entry.get("name")
                        or tmdb_entry.get("original_name")
                        or normalized_title
                        or "Unbekannte Serie"
                    ),
                )
            elif series.tmdb_id < 0:
                series.tmdb_id = tmdb_entry["id"]

        if series is None:
            series = Series(
                tmdb_id=tmdb_entry["id"],
                name=normalized_title or "Unbekannte Serie",
            )

        tmdb_details = fetch_tmdb_series_details(tmdb_entry["id"])
        if tmdb_details:
            db.session.add(series)
            db.session.flush()
            sync_series_with_tmdb(series, tmdb_details)
        else:
            _apply_tmdb_series_metadata(series, tmdb_entry)
    else:
        if series is None:
            fallback_title = normalized_title or base_title or raw_title or "Unbekannte Serie"
            series = Series(
                tmdb_id=_generate_series_placeholder_tmdb_id(),
                name=fallback_title,
            )
        db.session.add(series)
        db.session.flush()

        if series.tmdb_id > 0:
            sync_series_with_tmdb(series)

    db.session.add(series)
    db.session.flush()

    if series.tmdb_id > 0:
        season = (
            SeriesSeason.query.filter_by(
                series_id=series.id, season_number=season_number
            ).first()
        )
        episode = None
        season_has_episode = False
        if season is not None:
            season_has_episode = any(
                ep.episode_number == episode_number for ep in season.episodes
            )
        if season is None or not season_has_episode:
            sync_series_with_tmdb(series)
            season = (
                SeriesSeason.query.filter_by(
                    series_id=series.id, season_number=season_number
                ).first()
            )
        if season is not None:
            episode = (
                SeriesEpisode.query.filter_by(
                    season_id=season.id, episode_number=episode_number
                ).first()
            )
    else:
        season = (
            SeriesSeason.query.filter_by(
                series_id=series.id, season_number=season_number
            ).first()
        )
        episode = (
            SeriesEpisode.query.filter_by(
                season_id=season.id, episode_number=episode_number
            ).first()
            if season
            else None
        )

    if season is None:
        season = SeriesSeason(series=series, season_number=season_number)
        season.name = season.name or f"Staffel {season_number}"
        db.session.add(season)
        db.session.flush()

    if episode is None:
        episode = SeriesEpisode(season=season, episode_number=episode_number)
        db.session.add(episode)

    if not episode.name:
        episode.name = f"Episode {episode_number:02d}"
    if metadata.get("episode_title"):
        episode.name = metadata.get("episode_title")
    if not episode.overview and metadata.get("overview"):
        episode.overview = metadata.get("overview")

    db.session.add(episode)
    db.session.flush()

    source_name = entry.source_name or metadata.get("source_name") or "Unbekannt"
    mirror_info = metadata.get("mirror_info") or entry.mirror_info
    if not mirror_info:
        mirror_info = metadata.get("host_name")

    link = EpisodeStreamingLink.query.filter_by(
        episode_id=episode.id, url=entry.streaming_url
    ).first()
    if link:
        updated = False
        if source_name and link.source_name != source_name:
            link.source_name = source_name
            updated = True
        if mirror_info != link.mirror_info:
            link.mirror_info = mirror_info
            updated = True
        if updated:
            db.session.add(link)
            db.session.commit()
            return "updated", f"{series.name} S{season_number:02d}E{episode_number:02d}"
        return "exists", f"{series.name} S{season_number:02d}E{episode_number:02d}"

    link = EpisodeStreamingLink(
        episode=episode,
        url=entry.streaming_url,
        source_name=source_name,
        mirror_info=mirror_info,
    )
    db.session.add(link)
    db.session.commit()
    return "created", f"{series.name} S{season_number:02d}E{episode_number:02d}"


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


def _start_scraper(
    provider: str, start_page: int, *, include_series: bool = False
) -> bool:
    _initialize_scraper_state(provider)

    now_iso = _now_iso()
    scraper = SCRAPER_MANAGER.get_scraper(provider)
    if scraper is None:
        _append_scraper_log(provider, f"Unbekannter Scraper: {provider}", "error")
        return False

    scope_label = _describe_scraper_scope(scraper, include_series)
    scope_suffix = f" ({scope_label})" if scope_label else ""
    start_message = (
        f"{scraper.label}: {scope_label} werden vorbereitet…"
        if scope_label
        else f"{scraper.label}: Scraper wird gestartet…"
    )

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
                "message": start_message,
                "error": None,
                "content_scope": scope_label,
                "started_at": now_iso,
                "finished_at": None,
                "last_update": now_iso,
            }
        )
        SCRAPER_LOG[provider].clear()

    _append_scraper_log(
        provider,
        f"{scraper.label}-Scraper gestartet{scope_suffix} (ab Seite {start_page}).",
    )

    thread_target = _run_scraper
    thread_args: tuple = (provider, start_page, include_series)
    if provider == "filmpalast_series":
        thread_target = _run_filmpalast_series_scraper
        thread_args = (provider, start_page)

    thread = Thread(target=thread_target, args=thread_args, daemon=True)
    with SCRAPER_STATUS_LOCK:
        SCRAPER_THREADS[provider] = thread
    thread.start()
    return True


def _run_filmpalast_series_scraper(provider: str, start_page: int) -> None:
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
    scope_label = _describe_scraper_scope(scraper, include_series=False)
    scope_log_suffix = f" ({scope_label})" if scope_label else ""
    scope_message_suffix = f" · {scope_label}" if scope_label else ""

    stats_counter = {
        "created": 0,
        "updated": 0,
        "exists": 0,
        "skipped": 0,
        "errors": 0,
    }

    try:
        with app.app_context():
            _append_scraper_log(
                provider,
                f"[{provider_label}{scope_log_suffix}] Seite {start_page} wird verarbeitet…",
            )
            _set_scraper_status(
                provider,
                current_page=start_page,
                next_page=start_page,
                total_pages=1,
                processed_pages=0,
                message=f"{provider_label}: Seite {start_page} wird verarbeitet{scope_message_suffix}…",
                error=None,
            )

            try:
                from scraper.filmpalast_series_scraper import run_scraper as run_series_scraper
            except Exception as exc:  # pragma: no cover - import safety
                error_text = (
                    f"[{provider_label}] Serien-Scraper konnte nicht geladen werden: {exc}"
                )
                _append_scraper_log(provider, error_text, "error")
                _set_scraper_status(
                    provider,
                    running=False,
                    error=str(exc),
                    message=f"{provider_label}: Serien-Scraper nicht verfügbar.",
                    finished_at=_now_iso(),
                )
                return

            def log_callback(entry: ScraperResult, status: str, identifier: Optional[str]) -> None:
                normalized_status = (status or "").lower()
                title = identifier or entry.title or "Unbekannt"

                if normalized_status == "created":
                    stats_counter["created"] += 1
                    log_level = "success"
                    log_message = f"[{provider_label}] Serien-Link gespeichert: {title}"
                elif normalized_status == "updated":
                    stats_counter["updated"] += 1
                    log_level = "success"
                    log_message = f"[{provider_label}] Serien-Link aktualisiert: {title}"
                elif normalized_status == "exists":
                    stats_counter["exists"] += 1
                    log_level = "info"
                    log_message = f"[{provider_label}] Serien-Link bereits vorhanden: {title}"
                elif normalized_status == "skipped":
                    stats_counter["skipped"] += 1
                    log_level = "info"
                    log_message = f"[{provider_label}] Serien-Link übersprungen: {title}"
                else:
                    stats_counter["errors"] += 1
                    log_level = "error"
                    log_message = f"[{provider_label}] Fehler beim Speichern: {title}"

                _append_scraper_log(provider, log_message, log_level)
                processed_links = stats_counter["created"] + stats_counter["updated"]
                _set_scraper_status(
                    provider,
                    last_title=title,
                    processed_links=processed_links,
                    message=f"{provider_label}: Verarbeite {title}{scope_message_suffix}",
                )

            try:
                actual_start_page, stats = run_series_scraper(
                    page=start_page,
                    callback=log_callback,
                )
            except ValueError as exc:
                db.session.rollback()
                error_text = f"[{provider_label}] Ungültige Startseite: {exc}"
                _append_scraper_log(provider, error_text, "error")
                _set_scraper_status(
                    provider,
                    running=False,
                    error=str(exc),
                    message=f"{provider_label}: Ungültige Startseite.",
                    finished_at=_now_iso(),
                    current_page=start_page,
                    next_page=start_page,
                )
                return
            except Exception as exc:  # pragma: no cover - network/database safeguards
                db.session.rollback()
                error_text = f"[{provider_label}] Fehler beim Ausführen: {exc}"
                _append_scraper_log(provider, error_text, "error")
                _set_scraper_status(
                    provider,
                    running=False,
                    error=str(exc),
                    message=f"{provider_label}: Fehler beim Scrapen.",
                    finished_at=_now_iso(),
                    current_page=start_page,
                    next_page=start_page,
                )
                return

            if stats is None:
                page_progress = get_scraper_int_setting(provider, "page", start_page)
                last_page_value = max(0, page_progress - 1)
                set_scraper_setting(provider, "next_page", page_progress)
                set_scraper_setting(provider, "last_page", last_page_value)
                _append_scraper_log(
                    provider,
                    f"[{provider_label}{scope_log_suffix}] Keine weiteren Einträge gefunden. Scraper beendet.",
                    "info",
                )
                _set_scraper_status(
                    provider,
                    running=False,
                    processed_pages=0,
                    processed_links=0,
                    message=f"{provider_label}: Keine weiteren Einträge gefunden{scope_message_suffix}.",
                    current_page=actual_start_page,
                    next_page=page_progress,
                    last_page=last_page_value or None,
                    finished_at=_now_iso(),
                    total_pages=1,
                )
                return

            summary = stats.as_dict()
            processed_links = stats_counter["created"] + stats_counter["updated"]
            page_progress = get_scraper_int_setting(
                provider, "page", actual_start_page + 1
            )
            last_page_value = max(0, page_progress - 1)
            set_scraper_setting(provider, "next_page", page_progress)
            set_scraper_setting(provider, "last_page", last_page_value)
            _append_scraper_log(
                provider,
                f"[{provider_label}{scope_log_suffix}] Serien-Scraper abgeschlossen. Zusammenfassung: {summary}",
                "success",
            )
            _set_scraper_status(
                provider,
                running=False,
                processed_pages=1,
                processed_links=processed_links,
                message=f"{provider_label}: Serien-Seite {actual_start_page} verarbeitet{scope_message_suffix}.",
                current_page=actual_start_page,
                next_page=page_progress,
                last_page=last_page_value or None,
                finished_at=_now_iso(),
                total_pages=1,
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


def _run_scraper(provider: str, start_page: int, include_series: bool = False) -> None:
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
    scope_label = _describe_scraper_scope(scraper, include_series)
    scope_log_suffix = f" ({scope_label})" if scope_label else ""
    scope_message_suffix = f" · {scope_label}" if scope_label else ""

    try:
        with app.app_context():
            while True:
                _append_scraper_log(
                    provider,
                    f"[{provider_label}{scope_log_suffix}] Seite {page} wird geladen…",
                )
                _set_scraper_status(
                    provider,
                    current_page=page,
                    next_page=page,
                    message=f"{provider_label}: Seite {page} wird geladen{scope_message_suffix}…",
                    error=None,
                )

                def progress_callback(entry: ScraperResult) -> None:
                    title = entry.title or "Unbekannt"
                    _set_scraper_status(
                        provider,
                        last_title=title,
                        message=f"{provider_label}: Gefunden {title}{scope_message_suffix}",
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
                        f"[{provider_label}{scope_log_suffix}] Keine weiteren Einträge gefunden. Scraper beendet.",
                        "success",
                    )
                    _set_scraper_status(
                        provider,
                        message=f"{provider_label}: Keine weiteren Einträge gefunden{scope_message_suffix}.",
                        last_title=None,
                        current_page=page,
                        next_page=1,
                        last_page=last_page_value or None,
                    )
                    break

                for entry in entries:
                    title = entry.title or "Unbekannt"
                    metadata = entry.metadata or {}
                    content_type = (metadata.get("type") or "").lower()
                    looks_like_series = content_type == "series"
                    if not looks_like_series:
                        _, season_guess, episode_guess = _extract_series_metadata(
                            entry.title or ""
                        )
                        if season_guess is not None and episode_guess is not None:
                            looks_like_series = True
                    _set_scraper_status(
                        provider,
                        last_title=title,
                        message=f"{provider_label}: Speichere {title}{scope_message_suffix}",
                        error=None,
                    )
                    try:
                        if looks_like_series:
                            status, series_title = attach_series_streaming_entry(entry)
                            display_title = series_title or title
                            if status == "created":
                                processed_links += 1
                                _set_scraper_status(
                                    provider,
                                    processed_links=processed_links,
                                    error=None,
                                )
                                _append_scraper_log(
                                    provider,
                                    f"[{provider_label}] Serien-Link gespeichert: {display_title}",
                                    "success",
                                )
                            elif status == "updated":
                                _append_scraper_log(
                                    provider,
                                    f"[{provider_label}] Serien-Link aktualisiert: {display_title}",
                                    "success",
                                )
                            elif status == "exists":
                                _append_scraper_log(
                                    provider,
                                    f"[{provider_label}] Serien-Link bereits vorhanden: {display_title}",
                                    "info",
                                )
                            else:
                                _append_scraper_log(
                                    provider,
                                    f"[{provider_label}] Serien-Link übersprungen: {display_title}",
                                    "info",
                                )
                            if status != "skipped":
                                continue

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

                        attach_movie_streaming_link(
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
                    message=f"{provider_label}: Alle Seiten verarbeitet{scope_message_suffix}.",
                    finished_at=_now_iso(),
                    current_page=last_completed_page or page,
                    next_page=1,
                    last_page=final_last_page or None,
                    processed_pages=processed_pages,
                    processed_links=processed_links,
                )
                _append_scraper_log(
                    provider,
                    f"[{provider_label}{scope_log_suffix}] Scraper abgeschlossen. Verarbeitete Seiten: {processed_pages}, neue Links: {processed_links}.",
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
    episode_link_filter = func.length(func.trim(EpisodeStreamingLink.url)) > 0
    series_query = (
        Series.query.join(SeriesSeason)
        .join(SeriesEpisode)
        .join(EpisodeStreamingLink)
        .filter(episode_link_filter)
        .distinct()
    )

    popular_series = (
        series_query.order_by(Series.rating.desc().nullslast(), Series.updated_at.desc())
        .limit(20)
        .all()
    )
    recent_series = (
        series_query.order_by(
            Series.updated_at.desc().nullslast(), Series.created_at.desc()
        )
        .limit(20)
        .all()
    )

    if popular_series:
        series_sections.append({"title": "Top Serien", "items": popular_series})
    if recent_series:
        series_sections.append({"title": "Neu hinzugefügt", "items": recent_series})

    scraped = StreamingLink.query.order_by(StreamingLink.id.desc()).limit(25).all()

    hero_movies = popular_movies[:5]

    total_movies = db.session.query(func.count(Movie.id)).filter(valid_filter).scalar() or 0
    latest_movie = recent_movies[0] if recent_movies else None
    top_rated_movie = next((movie for movie in popular_movies if movie.rating), popular_movies[0] if popular_movies else None)

    movie_library_stats = {
        "total_movies": int(total_movies),
        "latest_title": latest_movie.title if latest_movie else None,
        "latest_added": latest_movie.created_at.strftime("%d.%m.%Y") if latest_movie and latest_movie.created_at else None,
        "top_rated_title": top_rated_movie.title if top_rated_movie else None,
        "top_rating": round(float(top_rated_movie.rating), 1) if top_rated_movie and top_rated_movie.rating is not None else None,
    }

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
        "movie_library_stats": movie_library_stats,
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
    context["movie_scraper_providers"] = [
        provider
        for provider in providers
        if "movies" in _get_scraper_categories(provider)
    ]
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


@app.route("/api/series/<int:series_id>")
def api_series_detail(series_id: int):
    series = Series.query.get_or_404(series_id)

    tmdb_details: dict = {}
    if series.tmdb_id > 0:
        tmdb_details = fetch_tmdb_series_details(series.tmdb_id)
        if tmdb_details:
            sync_series_with_tmdb(series, tmdb_details)

    poster_url = build_tmdb_image(series.poster_path)
    backdrop_url = build_tmdb_image(series.backdrop_path, "w1280")

    seasons_payload: List[dict] = []
    default_episode_reference: Optional[dict] = None
    default_streams: List[dict] = []
    total_episodes = 0

    for season in sorted(series.seasons, key=lambda item: item.season_number):
        episodes_payload: List[dict] = []
        for episode in sorted(season.episodes, key=lambda item: item.episode_number):
            links = [link.to_dict() for link in episode.streaming_links if (link.url or "").strip()]
            if links and default_episode_reference is None:
                default_episode_reference = {
                    "id": episode.id,
                    "season_number": season.season_number,
                    "episode_number": episode.episode_number,
                }
                default_streams = links

            episodes_payload.append(
                {
                    "id": episode.id,
                    "episode_number": episode.episode_number,
                    "name": episode.name,
                    "overview": episode.overview,
                    "air_date": episode.air_date,
                    "runtime": episode.runtime,
                    "still_url": build_tmdb_image(episode.still_path, "w780"),
                    "streaming_links": links,
                }
            )
            total_episodes += 1

        seasons_payload.append(
            {
                "id": season.id,
                "season_number": season.season_number,
                "name": season.name or f"Staffel {season.season_number}",
                "overview": season.overview,
                "poster_url": build_tmdb_image(season.poster_path),
                "air_date": season.air_date,
                "episode_count": season.episode_count or len(episodes_payload),
                "episodes": episodes_payload,
            }
        )

    series_payload = {
        "id": series.id,
        "content_type": "series",
        "title": series.name,
        "overview": series.overview,
        "poster_url": poster_url,
        "backdrop_url": backdrop_url,
        "first_air_date": series.first_air_date,
        "last_air_date": series.last_air_date,
        "release_date": series.first_air_date,
        "rating": series.rating,
        "genres": tmdb_details.get("genres", []) if tmdb_details else [],
        "tagline": tmdb_details.get("tagline") if tmdb_details else None,
        "cast": tmdb_details.get("cast", []) if tmdb_details else [],
        "trailer": tmdb_details.get("trailer") if tmdb_details else None,
        "status": tmdb_details.get("status") if tmdb_details else None,
        "episode_run_time": tmdb_details.get("episode_run_time", []) if tmdb_details else [],
        "streaming_links": default_streams,
        "default_episode": default_episode_reference,
        "seasons": seasons_payload,
        "total_seasons": len(seasons_payload),
        "total_episodes": total_episodes,
    }

    db.session.commit()
    return jsonify({"success": True, "series": series_payload})


@app.route("/api/scrape/<provider>", methods=["POST"])
@app.route("/api/scrape/kinox", methods=["POST"], defaults={"provider": "kinox"})
def api_scrape_provider(provider: str):
    provider = (provider or "").lower()
    scraper = SCRAPER_MANAGER.get_scraper(provider)
    if scraper is None:
        return jsonify({"success": False, "message": "Unbekannter Scraper."}), 404

    data = request.get_json(silent=True) or {}
    include_series = bool(data.get("include_series"))
    stored_next_page = get_scraper_int_setting(provider, "next_page", 1)
    categories = _get_scraper_categories(scraper)
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
        start_page = stored_next_page

    if (
        include_series
        and "movies" in categories
        and "series" in categories
        and stored_next_page <= 1
        and start_page <= 1
    ):
        start_page = 1
        set_scraper_setting(provider, "next_page", start_page)
        set_scraper_setting(provider, "last_page", 0)
        _append_scraper_log(
            provider,
            f"[{scraper.label}] Serienmodus aktiviert: Starte Filme & Serien ab Seite 1.",
        )

    started = _start_scraper(provider, start_page, include_series=include_series)
    status = get_scraper_status()
    scope_label = _describe_scraper_scope(scraper, include_series)
    if scope_label:
        message = (
            f"{scraper.label}-Scraper für {scope_label} gestartet."
            if started
            else f"{scraper.label}-Scraper für {scope_label} läuft bereits."
        )
    else:
        message = (
            f"{scraper.label}-Scraper gestartet."
            if started
            else f"{scraper.label}-Scraper läuft bereits."
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
    placeholder_series = Series.query.filter(Series.tmdb_id < 0).count()
    tmdb_series = Series.query.filter(Series.tmdb_id > 0).count()

    removed_links = StreamingLink.query.delete(synchronize_session=False)
    removed_movies = Movie.query.delete(synchronize_session=False)
    removed_episode_links = EpisodeStreamingLink.query.delete(synchronize_session=False)
    removed_episodes = SeriesEpisode.query.delete(synchronize_session=False)
    removed_seasons = SeriesSeason.query.delete(synchronize_session=False)
    removed_series = Series.query.delete(synchronize_session=False)
    for provider in SCRAPER_MANAGER.available_providers():
        set_scraper_setting(provider.name, "next_page", 1)
        set_scraper_setting(provider.name, "last_page", 0)

    with SCRAPER_STATUS_LOCK:
        for provider in SCRAPER_MANAGER.available_providers():
            status = SCRAPER_STATUS.get(provider.name)
            if status is not None:
                status.update(
                    {
                        "running": False,
                        "start_page": None,
                        "current_page": None,
                        "next_page": 1,
                        "last_page": 0,
                        "processed_pages": 0,
                        "total_pages": 0,
                        "processed_links": 0,
                        "last_title": None,
                        "message": "Bereit.",
                        "error": None,
                        "started_at": None,
                        "finished_at": None,
                    }
                )
            log = SCRAPER_LOG.get(provider.name)
            if log is not None:
                log.clear()

    db.session.commit()

    return jsonify(
        {
            "success": True,
            "removed_links": removed_links,
            "removed_movies": removed_movies,
            "removed_episode_links": removed_episode_links,
            "removed_series_episodes": removed_episodes,
            "removed_series_seasons": removed_seasons,
            "removed_series": removed_series,
            "removed_placeholder_movies": placeholder_count,
            "removed_tmdb_movies": tmdb_count,
            "removed_placeholder_series": placeholder_series,
            "removed_tmdb_series": tmdb_series,
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
