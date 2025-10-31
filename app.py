import glob
import json
import os
import re
import time
from collections import deque
from datetime import date, datetime
from threading import Lock, Thread
from typing import List, Optional, Set, Tuple

import requests
from flask import Flask, jsonify, render_template, request, url_for, g
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import case, func, or_
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import selectinload
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from urllib.parse import urlparse

from scrapers import BaseScraper, ScraperResult, get_scraper_manager


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "database", "mediahub.db")
DATABASE_URL = os.environ.get("DATABASE_URL", f"sqlite:///{DEFAULT_DB_PATH}")
_ENV_TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["PROFILE_UPLOAD_FOLDER"] = os.path.join(app.static_folder, "uploads")
app.config["ALLOWED_AVATAR_EXTENSIONS"] = {".png", ".jpg", ".jpeg", ".webp", ".gif"}

db = SQLAlchemy(app)

SCRAPER_MANAGER = get_scraper_manager()

SCRAPER_STATUS_LOCK = Lock()
SCRAPER_LOG_MAXLEN = 200
SCRAPER_LOG: dict[str, deque] = {}
SCRAPER_STATUS: dict[str, dict] = {}
SCRAPER_THREADS: dict[str, Thread] = {}


MOVIE_RUNTIME_CACHE: dict[int, Optional[int]] = {}
MOVIE_RUNTIME_CACHE_LOCK = Lock()

TMDB_GENRE_CACHE: dict[int, str] = {}
TMDB_GENRE_CACHE_LOCK = Lock()
TMDB_GENRE_CACHE_LAST_FETCH: float = 0.0
TMDB_GENRE_CACHE_TTL_SECONDS = 12 * 60 * 60

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/"

PROFILE_AVATAR_BASENAME = "profile_avatar"


def _ensure_profile_upload_folder() -> str:
    upload_folder = app.config["PROFILE_UPLOAD_FOLDER"]
    os.makedirs(upload_folder, exist_ok=True)
    return upload_folder


def _find_existing_avatar_file() -> Optional[str]:
    upload_folder = _ensure_profile_upload_folder()
    pattern = os.path.join(upload_folder, f"{PROFILE_AVATAR_BASENAME}.*")
    files = sorted(glob.glob(pattern))
    for file_path in files:
        if os.path.isfile(file_path):
            return file_path
    return None


def _delete_existing_avatar_file(exclude: Optional[str] = None) -> None:
    upload_folder = _ensure_profile_upload_folder()
    pattern = os.path.join(upload_folder, f"{PROFILE_AVATAR_BASENAME}.*")
    exclude_normalized = os.path.abspath(exclude) if exclude else None
    for file_path in glob.glob(pattern):
        try:
            if exclude_normalized and os.path.abspath(file_path) == exclude_normalized:
                continue
            os.remove(file_path)
        except OSError:
            continue


def _build_avatar_url(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    try:
        relative_path = os.path.relpath(path, app.static_folder)
    except ValueError:
        return None
    relative_path = relative_path.replace(os.sep, "/")
    return url_for("static", filename=relative_path)


def build_image_url(path: Optional[str], size: str = "w185") -> Optional[str]:
    if not path:
        return None
    normalized = path.lstrip("/")
    return f"{TMDB_IMAGE_BASE}{size}{normalized}"


def format_timestamp(value: Optional[datetime]) -> str:
    if not value:
        return "Unbekannt"
    return value.strftime("%d.%m.%Y %H:%M")


def _compute_avatar_initials(name: Optional[str], email: Optional[str]) -> str:
    if name:
        parts = [part for part in re.split(r"\s+", name.strip()) if part]
        if parts:
            initials = "".join(part[0] for part in parts[:2]).upper()
            if initials:
                return initials
    if email and "@" in email:
        local_part = email.split("@", 1)[0]
        if local_part:
            return local_part[:2].upper()
    return "--"


def _parse_favorite_genres(raw_value: Optional[str]) -> list[str]:
    if not raw_value:
        return []
    if isinstance(raw_value, list):
        return [str(item).strip() for item in raw_value if str(item).strip()]
    return [part.strip() for part in raw_value.split(",") if part.strip()]


def _serialize_favorite_genres(genres: list[str]) -> str:
    return ", ".join(sorted({genre.strip() for genre in genres if genre.strip()}))


def _normalize_genre_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    stripped = str(name).strip()
    return stripped or None


def fetch_tmdb_genre_map(force: bool = False) -> dict[int, str]:
    """Return a cached mapping of TMDB genre IDs to names."""

    global TMDB_GENRE_CACHE_LAST_FETCH
    now = time.time()
    with TMDB_GENRE_CACHE_LOCK:
        if (
            TMDB_GENRE_CACHE
            and not force
            and (now - TMDB_GENRE_CACHE_LAST_FETCH) < TMDB_GENRE_CACHE_TTL_SECONDS
        ):
            return dict(TMDB_GENRE_CACHE)

    api_key = get_tmdb_api_key()
    if not api_key:
        return {}

    params = {"api_key": api_key, "language": "de-DE"}
    try:
        response = requests.get(
            "https://api.themoviedb.org/3/genre/movie/list", params=params, timeout=20
        )
        response.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network
        app.logger.warning("TMDB genre list request failed: %s", exc)
        with TMDB_GENRE_CACHE_LOCK:
            if TMDB_GENRE_CACHE:
                return dict(TMDB_GENRE_CACHE)
        return {}

    payload = response.json()
    genres_payload = payload.get("genres") if isinstance(payload, dict) else None
    mapping: dict[int, str] = {}
    if isinstance(genres_payload, list):
        for entry in genres_payload:
            try:
                tmdb_id = int(entry.get("id"))
            except (TypeError, ValueError):
                continue
            name = _normalize_genre_name(entry.get("name"))
            if tmdb_id <= 0 or not name:
                continue
            mapping[tmdb_id] = name

    with TMDB_GENRE_CACHE_LOCK:
        TMDB_GENRE_CACHE.clear()
        TMDB_GENRE_CACHE.update(mapping)
        TMDB_GENRE_CACHE_LAST_FETCH = now

    return dict(mapping)


def _extract_tmdb_genre_entries(tmdb_data: Optional[dict]) -> list[tuple[Optional[int], Optional[str]]]:
    if not isinstance(tmdb_data, dict):
        return []

    entries: list[tuple[Optional[int], Optional[str]]] = []
    genres_payload = tmdb_data.get("genres")
    if isinstance(genres_payload, list):
        for entry in genres_payload:
            tmdb_id: Optional[int] = None
            name: Optional[str] = None
            if isinstance(entry, dict):
                try:
                    tmdb_id_value = entry.get("id")
                    tmdb_id = int(tmdb_id_value) if tmdb_id_value is not None else None
                except (TypeError, ValueError):
                    tmdb_id = None
                name = _normalize_genre_name(entry.get("name"))
            elif isinstance(entry, str):
                name = _normalize_genre_name(entry)
            elif entry is not None:
                try:
                    tmdb_id = int(entry)
                except (TypeError, ValueError):
                    tmdb_id = None
            entries.append((tmdb_id, name))

    genre_ids_payload = tmdb_data.get("genre_ids")
    if isinstance(genre_ids_payload, list):
        for entry in genre_ids_payload:
            try:
                tmdb_id = int(entry)
            except (TypeError, ValueError):
                continue
            entries.append((tmdb_id, None))

    return entries


def _update_movie_genres_from_tmdb(movie: "Movie", tmdb_data: Optional[dict]) -> None:
    if movie is None or not isinstance(tmdb_data, dict):
        return

    genre_entries = _extract_tmdb_genre_entries(tmdb_data)
    if not genre_entries:
        return

    needs_mapping = any(
        tmdb_id and not _normalize_genre_name(name)
        for tmdb_id, name in genre_entries
    )
    genre_map = fetch_tmdb_genre_map() if needs_mapping else {}

    new_genres: list[Genre] = []
    seen: set[tuple[Optional[int], Optional[str]]] = set()

    for tmdb_id_raw, name_raw in genre_entries:
        tmdb_id: Optional[int] = None
        if tmdb_id_raw is not None:
            try:
                tmdb_id_candidate = int(tmdb_id_raw)
            except (TypeError, ValueError):
                tmdb_id_candidate = None
            if tmdb_id_candidate and tmdb_id_candidate > 0:
                tmdb_id = tmdb_id_candidate

        normalized_name = _normalize_genre_name(name_raw)
        if normalized_name is None and tmdb_id is not None:
            normalized_name = genre_map.get(tmdb_id)
        if normalized_name is None:
            continue

        dedupe_key = (tmdb_id, normalized_name.lower())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        genre: Optional[Genre] = None
        if tmdb_id is not None:
            genre = Genre.query.filter_by(tmdb_id=tmdb_id).first()
        if genre is None:
            genre = (
                Genre.query.filter(func.lower(Genre.name) == normalized_name.lower())
                .first()
            )
        if genre is None:
            genre = Genre(tmdb_id=tmdb_id, name=normalized_name)
        else:
            if tmdb_id is not None and genre.tmdb_id != tmdb_id:
                genre.tmdb_id = tmdb_id
            if genre.name != normalized_name:
                genre.name = normalized_name

        db.session.add(genre)
        new_genres.append(genre)

    if not new_genres:
        return

    db.session.add(movie)
    movie.genres = new_genres


def get_or_create_user_profile_model() -> "UserProfile":
    profile = UserProfile.query.first()
    if profile is None:
        profile = UserProfile()
        db.session.add(profile)
        db.session.commit()
    return profile


def fetch_recently_viewed(limit: int = 6) -> list[dict[str, object]]:
    events = (
        UserViewEvent.query.order_by(UserViewEvent.created_at.desc())
        .limit(limit * 3)
        .all()
    )
    results: list[dict[str, object]] = []

    for event in events:
        item: Optional[dict[str, object]] = None
        if event.content_type == "movie" and event.movie:
            movie = event.movie
            item = {
                "id": movie.id,
                "title": movie.title,
                "category": "Film",
                "overview": movie.overview or "",
                "poster_url": build_image_url(movie.poster_path, "w154"),
                "backdrop_url": build_image_url(movie.backdrop_path, "w300"),
                "viewed_at_display": format_timestamp(event.created_at),
            }
        elif event.content_type == "series" and event.series:
            series = event.series
            item = {
                "id": series.id,
                "title": series.name,
                "category": "Serie",
                "overview": series.overview or "",
                "poster_url": build_image_url(series.poster_path, "w154"),
                "backdrop_url": build_image_url(series.backdrop_path, "w300"),
                "viewed_at_display": format_timestamp(event.created_at),
            }
        elif event.content_type == "episode" and event.episode:
            episode = event.episode
            season = episode.season
            series = season.series if season else None
            item = {
                "id": episode.id,
                "title": episode.name
                or (f"Episode {episode.episode_number}" if episode.episode_number else "Episode"),
                "category": "Serie",
                "overview": episode.overview or "",
                "poster_url": build_image_url(episode.still_path, "w185"),
                "backdrop_url": build_image_url(episode.still_path, "w300"),
                "parent_title": series.name if series else None,
                "season_number": season.season_number if season else None,
                "episode_number": episode.episode_number,
                "viewed_at_display": format_timestamp(event.created_at),
            }

        if item is not None:
            results.append(item)
        if len(results) >= limit:
            break

    return results


def fetch_library_stats() -> dict[str, int]:
    stats = {"movies": 0, "series": 0, "episodes": 0}
    try:
        stats["movies"] = db.session.query(func.count(Movie.id)).scalar() or 0
        stats["series"] = db.session.query(func.count(Series.id)).scalar() or 0
        stats["episodes"] = (
            db.session.query(func.count(SeriesEpisode.id)).scalar() or 0
        )
    except Exception:
        db.session.rollback()
    return stats


def get_user_profile() -> dict[str, object]:
    profile_model = get_or_create_user_profile_model()
    favorite_genres = _parse_favorite_genres(profile_model.favorite_genres)
    membership_since_display = "Unbekannt"
    membership_since_value = ""
    membership_since = profile_model.membership_since
    if isinstance(membership_since, date):
        membership_since_display = membership_since.strftime("%d.%m.%Y")
        membership_since_value = membership_since.isoformat()

    display_initials = profile_model.avatar_initials or _compute_avatar_initials(
        profile_model.name, profile_model.email
    )

    avatar_file = _find_existing_avatar_file()
    avatar_image_url = _build_avatar_url(avatar_file)

    profile: dict[str, object] = {
        "name": profile_model.name or "",
        "role": profile_model.role or "",
        "avatar_initials": display_initials,
        "avatar_initials_value": profile_model.avatar_initials or "",
        "avatar_image_url": avatar_image_url,
        "has_avatar_image": bool(avatar_image_url),
        "email": profile_model.email or "",
        "location": profile_model.location or "",
        "membership_since_display": membership_since_display,
        "membership_since_value": membership_since_value,
        "bio": profile_model.bio or "",
        "favorite_genres": favorite_genres,
    }

    profile["recently_viewed"] = fetch_recently_viewed()
    profile["library_stats"] = fetch_library_stats()
    profile["recent_count"] = len(profile["recently_viewed"])
    return profile


def _prune_view_history(max_items: int = 200) -> None:
    if max_items <= 0:
        return
    total = UserViewEvent.query.count()
    if total <= max_items:
        return
    excess = total - max_items
    stale_events = (
        UserViewEvent.query.order_by(UserViewEvent.created_at.asc())
        .limit(excess)
        .all()
    )
    for event in stale_events:
        db.session.delete(event)


def record_user_view_event(content_type: str, object_id: int) -> bool:
    normalized_type = (content_type or "").strip().lower()
    if object_id <= 0:
        return False

    try:
        now = datetime.utcnow()
        event: Optional[UserViewEvent] = None

        if normalized_type == "movie":
            movie = Movie.query.get(object_id)
            if not movie:
                return False
            event = UserViewEvent.query.filter_by(
                content_type="movie", movie_id=movie.id
            ).first()
            if event is None:
                event = UserViewEvent(content_type="movie", movie=movie)
                db.session.add(event)
            event.series = None
            event.episode = None
        elif normalized_type == "series":
            series = Series.query.get(object_id)
            if not series:
                return False
            event = UserViewEvent.query.filter_by(
                content_type="series", series_id=series.id
            ).first()
            if event is None:
                event = UserViewEvent(content_type="series", series=series)
                db.session.add(event)
            event.movie = None
            event.episode = None
        elif normalized_type == "episode":
            episode = SeriesEpisode.query.get(object_id)
            if not episode:
                return False
            event = UserViewEvent.query.filter_by(
                content_type="episode", episode_id=episode.id
            ).first()
            if event is None:
                event = UserViewEvent(content_type="episode", episode=episode)
                db.session.add(event)
            event.movie = None
            event.series = episode.season.series if episode.season else None
        else:
            return False

        event.created_at = now
        event.updated_at = now
        db.session.flush()
        _prune_view_history()
        db.session.commit()
        return True
    except Exception:
        db.session.rollback()
        return False


def update_user_profile_from_form(
    form_data: dict[str, str], avatar_file: Optional[FileStorage] = None
) -> tuple[bool, Optional[str]]:
    profile = get_or_create_user_profile_model()

    def _clean(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    profile.name = _clean(form_data.get("name"))
    profile.role = _clean(form_data.get("role"))
    profile.email = _clean(form_data.get("email"))
    profile.location = _clean(form_data.get("location"))
    profile.bio = _clean(form_data.get("bio"))

    avatar_input = form_data.get("avatar_initials") or ""
    avatar_initials = avatar_input.strip().upper()[:4]
    profile.avatar_initials = avatar_initials or None

    favorite_genres = _parse_favorite_genres(form_data.get("favorite_genres"))
    profile.favorite_genres = _serialize_favorite_genres(favorite_genres)

    remove_avatar_value = (form_data.get("remove_avatar") or "").strip().lower()
    remove_avatar_requested = remove_avatar_value in {"1", "true", "on", "yes"}

    if avatar_file and avatar_file.filename:
        filename = secure_filename(avatar_file.filename)
        _, ext = os.path.splitext(filename)
        ext = ext.lower()
        allowed_extensions = app.config.get("ALLOWED_AVATAR_EXTENSIONS", set())
        if ext not in allowed_extensions:
            return (
                False,
                "Das Profilbild muss als PNG, JPG, JPEG, WEBP oder GIF hochgeladen werden.",
            )

        upload_folder = _ensure_profile_upload_folder()
        target_filename = f"{secure_filename(PROFILE_AVATAR_BASENAME)}{ext}"
        target_path = os.path.join(upload_folder, target_filename)
        try:
            avatar_file.save(target_path)
        except OSError:
            return False, "Das Profilbild konnte nicht gespeichert werden."
        _delete_existing_avatar_file(exclude=target_path)
        remove_avatar_requested = False
    elif remove_avatar_requested:
        _delete_existing_avatar_file()

    membership_value = (form_data.get("membership_since") or "").strip()
    if membership_value:
        try:
            profile.membership_since = datetime.strptime(
                membership_value, "%Y-%m-%d"
            ).date()
        except ValueError:
            db.session.rollback()
            return False, "Das Datum muss im Format JJJJ-MM-TT angegeben werden."
    else:
        profile.membership_since = None

    try:
        profile.updated_at = datetime.utcnow()
        db.session.commit()
        return True, None
    except Exception:
        db.session.rollback()
        return False, "Profil konnte nicht gespeichert werden."


@app.context_processor
def inject_user_profile() -> dict[str, object]:
    return {"user_profile": get_user_profile()}


movie_genres = db.Table(
    "movie_genres",
    db.Column("movie_id", db.Integer, db.ForeignKey("movies.id"), primary_key=True),
    db.Column("genre_id", db.Integer, db.ForeignKey("genres.id"), primary_key=True),
)


class Genre(db.Model):
    __tablename__ = "genres"

    id = db.Column(db.Integer, primary_key=True)
    tmdb_id = db.Column(db.Integer, unique=True)
    name = db.Column(db.String(120), unique=True, nullable=False)

    movies = db.relationship(
        "Movie",
        secondary=movie_genres,
        back_populates="genres",
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "tmdb_id": self.tmdb_id,
            "name": self.name,
        }


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
        lazy="selectin",
    )
    genres = db.relationship(
        "Genre",
        secondary=movie_genres,
        back_populates="movies",
        lazy="selectin",
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
            "streaming_links": serialize_streaming_links(self.streaming_links),
            "genres": [genre.name for genre in self.genres if genre.name],
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
        preferences = get_stream_provider_preferences_cache()
        provider_key, display_name = identify_stream_provider(
            self.mirror_info, self.url, self.source_name
        )
        preference = preferences.get(provider_key)
        if preference is None:
            preference = register_stream_provider(provider_key, display_name)
            preferences[provider_key] = preference

        is_visible = preference.is_visible if preference else True
        is_enabled = preference.is_enabled if preference else True

        return {
            "id": self.id,
            "movie_id": self.movie_id,
            "source_name": self.source_name,
            "url": self.url,
            "mirror_info": self.mirror_info,
            "provider_key": provider_key,
            "provider_display_name": display_name,
            "provider_visible": bool(is_visible),
            "provider_enabled": bool(is_enabled),
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
        preferences = get_stream_provider_preferences_cache()
        provider_key, display_name = identify_stream_provider(
            self.mirror_info, self.url, self.source_name
        )
        preference = preferences.get(provider_key)
        if preference is None:
            preference = register_stream_provider(provider_key, display_name)
            preferences[provider_key] = preference

        is_visible = preference.is_visible if preference else True
        is_enabled = preference.is_enabled if preference else True

        return {
            "id": self.id,
            "episode_id": self.episode_id,
            "source_name": self.source_name,
            "url": self.url,
            "mirror_info": self.mirror_info,
            "provider_key": provider_key,
            "provider_display_name": display_name,
            "provider_visible": bool(is_visible),
            "provider_enabled": bool(is_enabled),
        }


class StreamProviderPreference(db.Model):
    __tablename__ = "stream_provider_preferences"

    provider_key = db.Column(db.String(160), primary_key=True)
    display_name = db.Column(db.String(255))
    is_visible = db.Column(db.Boolean, nullable=False, default=True)
    is_enabled = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    def to_dict(self) -> dict:
        return {
            "provider_key": self.provider_key,
            "display_name": self.display_name or self.provider_key,
            "is_visible": bool(self.is_visible),
            "is_enabled": bool(self.is_enabled),
        }


class UserProfile(db.Model):
    __tablename__ = "user_profiles"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255))
    role = db.Column(db.String(120))
    avatar_initials = db.Column(db.String(8))
    email = db.Column(db.String(255))
    location = db.Column(db.String(255))
    membership_since = db.Column(db.Date)
    bio = db.Column(db.Text)
    favorite_genres = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class UserViewEvent(db.Model):
    __tablename__ = "user_view_events"

    id = db.Column(db.Integer, primary_key=True)
    content_type = db.Column(db.String(20), nullable=False)
    movie_id = db.Column(db.Integer, db.ForeignKey("movies.id"))
    series_id = db.Column(db.Integer, db.ForeignKey("series.id"))
    episode_id = db.Column(db.Integer, db.ForeignKey("series_episodes.id"))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    movie = db.relationship("Movie")
    series = db.relationship("Series")
    episode = db.relationship("SeriesEpisode")

class Setting(db.Model):
    __tablename__ = "settings"

    key = db.Column(db.String(64), primary_key=True)
    value = db.Column(db.String(255), nullable=False)


MOVIE_CREATED_AT_INDEX = db.Index("ix_movies_created_at", Movie.created_at)
MOVIE_RATING_INDEX = db.Index("ix_movies_rating", Movie.rating)
STREAMING_LINK_MOVIE_INDEX = db.Index(
    "ix_streaming_links_movie_id", StreamingLink.movie_id
)
STREAMING_LINK_MOVIE_TRIMMED_URL_INDEX = db.Index(
    "ix_streaming_links_movie_trimmed_url",
    StreamingLink.movie_id,
    func.trim(StreamingLink.url),
)
STREAMING_LINK_SOURCE_INDEX = db.Index(
    "ix_streaming_links_source_name", StreamingLink.source_name
)
SERIES_UPDATED_AT_INDEX = db.Index("ix_series_updated_at", Series.updated_at)
SERIES_RATING_INDEX = db.Index("ix_series_rating", Series.rating)
SERIES_SEASON_SERIES_INDEX = db.Index(
    "ix_series_seasons_series_id", SeriesSeason.series_id
)
SERIES_EPISODE_SEASON_INDEX = db.Index(
    "ix_series_episodes_season_id", SeriesEpisode.season_id
)
EPISODE_STREAMING_EPISODE_INDEX = db.Index(
    "ix_episode_streaming_links_episode_id", EpisodeStreamingLink.episode_id
)
EPISODE_STREAMING_EPISODE_TRIMMED_INDEX = db.Index(
    "ix_episode_streaming_links_episode_trimmed_url",
    EpisodeStreamingLink.episode_id,
    func.trim(EpisodeStreamingLink.url),
)
USER_VIEW_CONTENT_CREATED_INDEX = db.Index(
    "ix_user_view_events_content_type_created_at",
    UserViewEvent.content_type,
    UserViewEvent.created_at,
)
USER_VIEW_MOVIE_INDEX = db.Index(
    "ix_user_view_events_movie_id", UserViewEvent.movie_id
)
USER_VIEW_SERIES_INDEX = db.Index(
    "ix_user_view_events_series_id", UserViewEvent.series_id
)
USER_VIEW_EPISODE_INDEX = db.Index(
    "ix_user_view_events_episode_id", UserViewEvent.episode_id
)

DATABASE_INDEXES = (
    MOVIE_CREATED_AT_INDEX,
    MOVIE_RATING_INDEX,
    STREAMING_LINK_MOVIE_INDEX,
    STREAMING_LINK_MOVIE_TRIMMED_URL_INDEX,
    STREAMING_LINK_SOURCE_INDEX,
    SERIES_UPDATED_AT_INDEX,
    SERIES_RATING_INDEX,
    SERIES_SEASON_SERIES_INDEX,
    SERIES_EPISODE_SEASON_INDEX,
    EPISODE_STREAMING_EPISODE_INDEX,
    EPISODE_STREAMING_EPISODE_TRIMMED_INDEX,
    USER_VIEW_CONTENT_CREATED_INDEX,
    USER_VIEW_MOVIE_INDEX,
    USER_VIEW_SERIES_INDEX,
    USER_VIEW_EPISODE_INDEX,
)


def _normalize_provider_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = re.sub(r"\s+", " ", str(value).strip())
    return normalized or None


def _extract_domain(streaming_url: Optional[str]) -> Optional[str]:
    if not streaming_url:
        return None
    try:
        parsed = urlparse(streaming_url)
    except ValueError:
        return None
    hostname = parsed.netloc.lower().strip()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname or None


def identify_stream_provider(
    mirror_info: Optional[str],
    streaming_url: Optional[str],
    source_name: Optional[str] = None,
) -> tuple[str, str]:
    """Return a normalized provider key and a human-readable display name."""

    display_candidates = [
        _normalize_provider_name(mirror_info),
        _normalize_provider_name(source_name),
        _extract_domain(streaming_url),
    ]
    display_name = next(
        (candidate for candidate in display_candidates if candidate),
        None,
    )
    if not display_name:
        display_name = "Unbekannter Anbieter"

    key_basis = display_name.lower()
    key = re.sub(r"[^a-z0-9]+", "-", key_basis).strip("-")
    if not key:
        fallback = _extract_domain(streaming_url) or (source_name or "provider")
        key = re.sub(r"[^a-z0-9]+", "-", fallback.lower()).strip("-") or "provider"

    return key, display_name


def register_stream_provider(
    provider_key: str, display_name: Optional[str]
) -> StreamProviderPreference:
    """Ensure that a provider preference exists and return it."""

    preference = StreamProviderPreference.query.filter_by(
        provider_key=provider_key
    ).first()
    created_or_updated = False
    if preference is None:
        preference = StreamProviderPreference(
            provider_key=provider_key,
            display_name=display_name or provider_key,
        )
        db.session.add(preference)
        created_or_updated = True
    elif display_name and preference.display_name != display_name:
        preference.display_name = display_name
        db.session.add(preference)
        created_or_updated = True

    if created_or_updated:
        db.session.commit()
    return preference


def get_stream_provider_preferences_cache() -> dict[str, StreamProviderPreference]:
    cache: Optional[dict[str, StreamProviderPreference]] = getattr(
        g, "_stream_provider_preferences", None
    )
    if cache is None:
        preferences = StreamProviderPreference.query.all()
        cache = {preference.provider_key: preference for preference in preferences}
        g._stream_provider_preferences = cache
    return cache


def invalidate_stream_provider_cache() -> None:
    if hasattr(g, "_stream_provider_preferences"):
        delattr(g, "_stream_provider_preferences")


def serialize_streaming_links(
    links: List[object],
    *,
    include_invisible: bool = False,
    include_disabled: bool = False,
) -> list[dict]:
    preferences = get_stream_provider_preferences_cache()
    serialized: list[dict] = []
    for link in links:
        mirror_info = getattr(link, "mirror_info", None)
        streaming_url = getattr(link, "url", None)
        source_name = getattr(link, "source_name", None)
        provider_key, display_name = identify_stream_provider(
            mirror_info, streaming_url, source_name
        )

        preference = preferences.get(provider_key)
        if preference is None:
            preference = register_stream_provider(provider_key, display_name)
            preferences[provider_key] = preference

        is_visible = preference.is_visible if preference else True
        is_enabled = preference.is_enabled if preference else True

        if (not include_invisible and not is_visible) or (
            not include_disabled and not is_enabled
        ):
            continue

        payload = {
            "id": getattr(link, "id", None),
            "movie_id": getattr(link, "movie_id", None),
            "episode_id": getattr(link, "episode_id", None),
            "source_name": source_name,
            "url": streaming_url,
            "mirror_info": mirror_info,
            "provider_key": provider_key,
            "provider_display_name": display_name,
            "provider_visible": bool(is_visible),
            "provider_enabled": bool(is_enabled),
        }
        serialized.append(payload)
    return serialized


def filter_visible_streaming_links(links: List[object]) -> list[object]:
    preferences = get_stream_provider_preferences_cache()
    visible_links: list[object] = []
    for link in links:
        mirror_info = getattr(link, "mirror_info", None)
        streaming_url = getattr(link, "url", None)
        source_name = getattr(link, "source_name", None)
        provider_key, display_name = identify_stream_provider(
            mirror_info, streaming_url, source_name
        )
        preference = preferences.get(provider_key)
        if preference is None:
            preference = register_stream_provider(provider_key, display_name)
            preferences[provider_key] = preference

        if preference and not preference.is_visible:
            continue
        visible_links.append(link)
    return visible_links


def collect_stream_provider_stats() -> list[dict]:
    preferences = get_stream_provider_preferences_cache()
    stats: dict[str, dict] = {}

    def ensure_entry(provider_key: str, display_name: str) -> dict:
        preference = preferences.get(provider_key)
        if preference is None:
            preference = register_stream_provider(provider_key, display_name)
            preferences[provider_key] = preference
        entry = stats.get(provider_key)
        if entry is None:
            entry = {
                "provider_key": provider_key,
                "display_name": preference.display_name or display_name or provider_key,
                "is_visible": bool(preference.is_visible if preference else True),
                "is_enabled": bool(preference.is_enabled if preference else True),
                "movie_links": 0,
                "episode_links": 0,
                "total_links": 0,
            }
            stats[provider_key] = entry
        else:
            entry["display_name"] = preference.display_name or display_name or provider_key
            entry["is_visible"] = bool(preference.is_visible if preference else True)
            entry["is_enabled"] = bool(preference.is_enabled if preference else True)
        return entry

    movie_links = StreamingLink.query.all()
    for link in movie_links:
        provider_key, display_name = identify_stream_provider(
            link.mirror_info, link.url, link.source_name
        )
        entry = ensure_entry(provider_key, display_name)
        entry["movie_links"] += 1
        entry["total_links"] += 1

    episode_links = EpisodeStreamingLink.query.all()
    for link in episode_links:
        provider_key, display_name = identify_stream_provider(
            link.mirror_info, link.url, link.source_name
        )
        entry = ensure_entry(provider_key, display_name)
        entry["episode_links"] += 1
        entry["total_links"] += 1

    for provider_key, preference in preferences.items():
        if provider_key not in stats:
            stats[provider_key] = {
                "provider_key": provider_key,
                "display_name": preference.display_name or provider_key,
                "is_visible": bool(preference.is_visible if preference else True),
                "is_enabled": bool(preference.is_enabled if preference else True),
                "movie_links": 0,
                "episode_links": 0,
                "total_links": 0,
            }

    return sorted(
        stats.values(), key=lambda item: (item["display_name"].lower(), item["provider_key"])
    )


def _delete_stream_provider_links(provider_keys: set[str]) -> dict:
    removed_movie_links = 0
    removed_episode_links = 0

    movie_links = StreamingLink.query.all()
    for link in movie_links:
        provider_key, _ = identify_stream_provider(
            link.mirror_info, link.url, link.source_name
        )
        if provider_key in provider_keys:
            db.session.delete(link)
            removed_movie_links += 1

    episode_links = EpisodeStreamingLink.query.all()
    for link in episode_links:
        provider_key, _ = identify_stream_provider(
            link.mirror_info, link.url, link.source_name
        )
        if provider_key in provider_keys:
            db.session.delete(link)
            removed_episode_links += 1

    db.session.commit()
    invalidate_stream_provider_cache()
    return {
        "removed_movie_links": removed_movie_links,
        "removed_episode_links": removed_episode_links,
        "removed_total_links": removed_movie_links + removed_episode_links,
    }


def apply_stream_provider_action(
    action: str, provider_keys: list[str]
) -> Tuple[dict, Optional[str], int]:
    normalized_keys = {
        key.strip(): key.strip()
        for key in (provider_keys or [])
        if isinstance(key, str) and key.strip()
    }

    if not normalized_keys:
        preferences = StreamProviderPreference.query.all()
        normalized_keys = {pref.provider_key: pref.provider_key for pref in preferences}

    if not normalized_keys:
        return {}, "Keine Anbieter ausgewählt.", 400

    action_normalized = (action or "").strip().lower()
    valid_actions = {"hide", "show", "disable", "enable", "delete"}
    if action_normalized not in valid_actions:
        return {}, "Unbekannte Aktion.", 400

    provider_key_list = list(normalized_keys.keys())
    preferences = StreamProviderPreference.query.filter(
        StreamProviderPreference.provider_key.in_(provider_key_list)
    ).all()

    missing_keys = set(provider_key_list) - {pref.provider_key for pref in preferences}
    for provider_key in missing_keys:
        register_stream_provider(provider_key, provider_key)
    if missing_keys:
        preferences.extend(
            StreamProviderPreference.query.filter(
                StreamProviderPreference.provider_key.in_(missing_keys)
            ).all()
        )

    if action_normalized == "hide":
        for preference in preferences:
            preference.is_visible = False
            db.session.add(preference)
        db.session.commit()
        invalidate_stream_provider_cache()
        return {"updated": len(preferences)}, None, 200

    if action_normalized == "show":
        for preference in preferences:
            preference.is_visible = True
            db.session.add(preference)
        db.session.commit()
        invalidate_stream_provider_cache()
        return {"updated": len(preferences)}, None, 200

    if action_normalized == "disable":
        for preference in preferences:
            preference.is_enabled = False
            db.session.add(preference)
        db.session.commit()
        invalidate_stream_provider_cache()
        return {"updated": len(preferences)}, None, 200

    if action_normalized == "enable":
        for preference in preferences:
            preference.is_enabled = True
            db.session.add(preference)
        db.session.commit()
        invalidate_stream_provider_cache()
        return {"updated": len(preferences)}, None, 200

    if action_normalized == "delete":
        result = _delete_stream_provider_links(set(provider_key_list))
        return result, None, 200

    return {}, "Aktion konnte nicht ausgeführt werden.", 400


def _has_non_empty_text(column):
    trimmed = func.trim(column)
    return trimmed != ""


def movie_has_valid_streaming_link():
    return Movie.streaming_links.any(_has_non_empty_text(StreamingLink.url))


def series_has_valid_streaming_link():
    return Series.seasons.any(
        SeriesSeason.episodes.any(
            SeriesEpisode.streaming_links.any(
                _has_non_empty_text(EpisodeStreamingLink.url)
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


def _scraper_hosts_setting_key(provider: str) -> str:
    return f"{provider}_hosts"


def get_scraper_host_setting(provider: str) -> set[str]:
    raw_value = get_setting(_scraper_hosts_setting_key(provider))
    if raw_value is None:
        return set()
    parsed: set[str] = set()
    if isinstance(raw_value, str):
        try:
            decoded = json.loads(raw_value)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, list):
            for item in decoded:
                if not isinstance(item, str):
                    continue
                normalized = item.strip()
                if normalized:
                    parsed.add(normalized)
        else:
            for part in raw_value.split(","):
                normalized = part.strip()
                if normalized:
                    parsed.add(normalized)
    return parsed


def set_scraper_host_setting(provider: str, hosts: List[str]) -> None:
    normalized = []
    seen: set[str] = set()
    for host in hosts:
        if not isinstance(host, str):
            continue
        key = host.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    set_setting(_scraper_hosts_setting_key(provider), json.dumps(normalized))


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


def _get_scraper_stream_host_options(scraper: BaseScraper) -> dict[str, dict[str, object]]:
    options_attr = getattr(scraper, "stream_host_options", None)
    if callable(options_attr):
        raw_options = options_attr()
    else:
        raw_options = options_attr
    options: dict[str, dict[str, object]] = {}
    if isinstance(raw_options, dict):
        for key, value in raw_options.items():
            if not isinstance(key, str):
                continue
            normalized_key = key.strip()
            if not normalized_key:
                continue
            if isinstance(value, dict):
                label = value.get("label")
                description = value.get("description")
                default = value.get("default", True)
            else:
                label = value
                description = None
                default = True
            options[normalized_key] = {
                "label": (str(label).strip() if label is not None else normalized_key),
                "description": (str(description).strip() if isinstance(description, str) else ""),
                "default": bool(default),
            }
    return options


def _resolve_scraper_host_selection(
    scraper: BaseScraper, options: dict[str, dict[str, object]]
) -> set[str]:
    if not options:
        return set()
    stored = get_scraper_host_setting(scraper.name)
    if stored:
        filtered = {key for key in stored if key in options}
        if filtered:
            return filtered
    defaults = {key for key, data in options.items() if data.get("default", True)}
    if defaults:
        return defaults
    return set(options.keys())


def _apply_scraper_host_preferences(scraper: BaseScraper) -> None:
    configure = getattr(scraper, "configure_stream_hosts", None)
    if not callable(configure):
        return
    options = _get_scraper_stream_host_options(scraper)
    selection = _resolve_scraper_host_selection(scraper, options) if options else set()
    configure(sorted(selection))


def _normalize_scraper_host_values(
    provider: str, host_values
) -> tuple[Optional[list[str]], Optional[str]]:
    scraper = SCRAPER_MANAGER.get_scraper(provider)
    if scraper is None:
        return None, "Unbekannter Scraper."
    options = _get_scraper_stream_host_options(scraper)
    if not options:
        return [], None
    if host_values is None:
        return None, None
    if isinstance(host_values, str):
        candidates = [part.strip() for part in host_values.split(",")]
    elif isinstance(host_values, (list, tuple, set)):
        candidates = list(host_values)
    else:
        return None, "Ungültige Auswahl."
    normalized: list[str] = []
    seen: set[str] = set()
    invalid: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        key = candidate.strip()
        if not key:
            continue
        if key not in options:
            invalid.append(key)
            continue
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    if invalid:
        return None, f"Ungültige Anbieter: {', '.join(sorted(set(invalid)))}"
    return normalized, None


def _build_scraper_provider_settings(scraper: BaseScraper) -> dict[str, object]:
    options = _get_scraper_stream_host_options(scraper)
    hosts = sorted(_resolve_scraper_host_selection(scraper, options)) if options else []
    return {
        "next_page": get_scraper_int_setting(scraper.name, "next_page", 1),
        "last_page": get_scraper_int_setting(scraper.name, "last_page", 0),
        "hosts": hosts,
    }


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
        ensure_database_indexes()


def ensure_database_indexes() -> None:
    engine = db.engine
    for index in DATABASE_INDEXES:
        try:
            index.create(bind=engine, checkfirst=True)
        except OperationalError as exc:
            message = str(exc).lower()
            if "already exists" in message:
                app.logger.debug("Skipping creation of existing index %s", index.name)
                continue
            raise


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
        "genres": [
            {
                "id": genre.get("id"),
                "name": genre.get("name"),
            }
            for genre in payload.get("genres", [])
            if genre.get("name")
        ],
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
    _update_movie_genres_from_tmdb(movie, tmdb_data)


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
        _update_movie_genres_from_tmdb(movie, entry)
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
) -> Tuple[str, Optional[str]]:
    """Create or update a streaming link for a movie based on its title.

    Returns a tuple of (status, normalized movie title).
    """

    provider_key, provider_display_name = identify_stream_provider(
        mirror_info, streaming_url, source_name
    )
    preference = register_stream_provider(provider_key, provider_display_name)
    if preference and not preference.is_enabled:
        return "skipped", None

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
    status = "exists"
    if link is None:
        link = StreamingLink(
            movie=movie,
            url=streaming_url,
            source_name=source_name,
            mirror_info=mirror_info,
        )
        status = "created"
    else:
        updated = False
        if link.source_name != source_name:
            link.source_name = source_name
            updated = True
        if link.mirror_info != mirror_info:
            link.mirror_info = mirror_info
            updated = True
        if updated:
            status = "updated"

    db.session.add(link)
    db.session.commit()
    return status, movie.title


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

    provider_key, provider_display_name = identify_stream_provider(
        mirror_info, entry.streaming_url, source_name
    )
    preference = register_stream_provider(provider_key, provider_display_name)
    if preference and not preference.is_enabled:
        return "skipped", None

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
            _apply_scraper_host_preferences(scraper)
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

                        status, movie_name = attach_movie_streaming_link(
                            entry.title or title,
                            entry.streaming_url,
                            entry.mirror_info,
                            entry.source_name,
                        )
                        if status == "skipped":
                            _append_scraper_log(
                                provider,
                                f"[{provider_label}] Link übersprungen (Anbieter deaktiviert): {title}",
                                "info",
                            )
                            continue

                        if status in {"created", "updated"}:
                            processed_links += 1
                            _set_scraper_status(
                                provider, processed_links=processed_links, error=None
                            )

                        if status == "updated":
                            _append_scraper_log(
                                provider,
                                f"[{provider_label}] Link aktualisiert: {movie_name or title}",
                                "success",
                            )
                        elif status == "created":
                            _append_scraper_log(
                                provider,
                                f"[{provider_label}] Link gespeichert: {movie_name or title}",
                                "success",
                            )
                        else:
                            _append_scraper_log(
                                provider,
                                f"[{provider_label}] Link bereits vorhanden: {movie_name or title}",
                                "info",
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


def fetch_movie_genre_stats(limit: Optional[int] = None) -> list[dict[str, object]]:
    valid_filter = movie_has_valid_streaming_link()
    query = (
        db.session.query(Genre, func.count(Movie.id).label("movie_count"))
        .join(movie_genres, Genre.id == movie_genres.c.genre_id)
        .join(Movie, Movie.id == movie_genres.c.movie_id)
        .filter(valid_filter)
        .group_by(Genre.id)
        .order_by(func.count(Movie.id).desc(), Genre.name.asc())
    )
    if limit is not None:
        query = query.limit(limit)

    results: list[dict[str, object]] = []
    for genre, movie_count in query:
        results.append(
            {
                "id": genre.id,
                "tmdb_id": genre.tmdb_id,
                "name": genre.name,
                "movie_count": int(movie_count or 0),
            }
        )
    return results


def build_library_context() -> dict:
    valid_filter = movie_has_valid_streaming_link()
    base_movie_query = Movie.query.options(selectinload(Movie.genres)).filter(valid_filter)
    popular_movies = (
        base_movie_query
        .order_by(Movie.rating.desc().nullslast())
        .limit(20)
        .all()
    )
    recent_movies = (
        base_movie_query
        .order_by(Movie.created_at.desc())
        .limit(20)
        .all()
    )
    linked_movies = (
        base_movie_query
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
    episode_link_filter = _has_non_empty_text(EpisodeStreamingLink.url)
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

    scraped_candidates = (
        StreamingLink.query.order_by(StreamingLink.id.desc()).limit(100).all()
    )
    scraped_visible = filter_visible_streaming_links(scraped_candidates)
    scraped = scraped_visible[:25]

    hero_movies: List[Movie] = []

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
        base_movie_query.filter(Movie.tmdb_id.in_(tmdb_ids))
        .order_by(order_case)
        .limit(20)
        .all()
    )
            now_playing_movies.sort(key=lambda movie: order_mapping.get(movie.tmdb_id, len(order_mapping)))

    if now_playing_movies:
        hero_movies = now_playing_movies[:5]
    elif recent_movies:
        hero_movies = recent_movies[:5]
    else:
        hero_movies = popular_movies[:5]

    return {
        "categories": categories,
        "film_sections": film_sections,
        "series_sections": series_sections,
        "scraped": scraped,
        "hero_movies": hero_movies,
        "now_playing_movies": now_playing_movies,
        "movie_library_stats": movie_library_stats,
        "movie_genres": fetch_movie_genre_stats(),
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


@app.route("/profil", methods=["GET", "POST"])
def profile_view():
    success_message: Optional[str] = None
    error_message: Optional[str] = None
    if request.method == "POST":
        form_payload = request.form.to_dict(flat=True)
        avatar_file = request.files.get("avatar_image")
        saved, message = update_user_profile_from_form(form_payload, avatar_file)
        if saved:
            success_message = "Profil wurde aktualisiert."
        else:
            error_message = message or "Profil konnte nicht gespeichert werden."

    profile = get_user_profile()
    profile_form_open = bool(error_message)
    return render_template(
        "profile.html",
        active_page="profile",
        page_title="Benutzerprofil",
        user_profile=profile,
        profile_message=success_message,
        profile_error=error_message,
        profile_form_open=profile_form_open,
    )


@app.route("/api/views", methods=["POST"])
def api_record_view():
    payload = request.get_json(silent=True) or {}
    content_type = payload.get("content_type") or payload.get("type")
    object_id = payload.get("object_id") or payload.get("id")

    if not content_type:
        return jsonify({"success": False, "message": "Kein Inhaltstyp übermittelt."}), 400

    try:
        object_id_int = int(object_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Ungültige Inhalts-ID."}), 400

    success = record_user_view_event(content_type, object_id_int)
    status_code = 200 if success else 404
    message = "Aufruf wurde gespeichert." if success else "Inhalt konnte nicht gespeichert werden."
    return jsonify({"success": success, "message": message}), status_code


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

    movie_genres = fetch_movie_genre_stats()
    raw_selected_genre = (request.args.get("genre") or "").strip()
    selected_genre = None
    if raw_selected_genre:
        lookup = {genre["name"].lower(): genre["name"] for genre in movie_genres}
        selected_genre = lookup.get(raw_selected_genre.lower())

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
        movie_genres=movie_genres,
        selected_genre=selected_genre,
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
        provider.name: _build_scraper_provider_settings(provider)
        for provider in providers
    }
    host_options_payload: dict[str, list[dict[str, object]]] = {}
    for provider in providers:
        options = _get_scraper_stream_host_options(provider)
        if not options:
            continue
        selection = _resolve_scraper_host_selection(provider, options)
        host_options_payload[provider.name] = [
            {
                "key": key,
                "label": data.get("label") or key,
                "description": data.get("description") or "",
                "selected": key in selection,
            }
            for key, data in sorted(
                options.items(), key=lambda item: item[1].get("label", item[0]).lower()
            )
        ]
    context["scraper_host_options"] = host_options_payload
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
            provider.name: _build_scraper_provider_settings(provider)
            for provider in providers
        },
    )


@app.route("/api/movies")
def api_movies():
    movies = (
        Movie.query.options(
            selectinload(Movie.genres),
            selectinload(Movie.streaming_links),
        )
        .filter(movie_has_valid_streaming_link())
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

    if tmdb_details:
        _update_movie_genres_from_tmdb(movie, tmdb_details)

    stored_genres = [
        {
            "id": genre.tmdb_id,
            "name": genre.name,
        }
        for genre in movie.genres
        if genre.name
    ]

    detail_genres_payload = []
    if isinstance(tmdb_details, dict):
        raw_genres = tmdb_details.get("genres")
        if isinstance(raw_genres, list):
            detail_genres_payload = [
                entry
                for entry in raw_genres
                if isinstance(entry, dict) and _normalize_genre_name(entry.get("name"))
            ]

    combined_genres: list[dict[str, Optional[str]]] = []
    seen_genres: set[tuple[Optional[int], str]] = set()

    def _append_genre(entry: dict) -> None:
        name = _normalize_genre_name(entry.get("name"))
        tmdb_id_value = entry.get("id")
        try:
            tmdb_id = int(tmdb_id_value) if tmdb_id_value is not None else None
        except (TypeError, ValueError):
            tmdb_id = None
        if not name:
            return
        key = (tmdb_id, name.lower())
        if key in seen_genres:
            return
        seen_genres.add(key)
        combined_genres.append({"id": tmdb_id, "name": name})

    for entry in detail_genres_payload:
        _append_genre(entry)
    for entry in stored_genres:
        _append_genre(entry)

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
        "genres": combined_genres,
        "tagline": tmdb_details.get("tagline"),
        "cast": tmdb_details.get("cast", []),
        "streaming_links": serialize_streaming_links(movie.streaming_links),
        "trailer": tmdb_details.get("trailer"),
    }

    db.session.commit()

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
            raw_links = [
                link for link in episode.streaming_links if (link.url or "").strip()
            ]
            links = serialize_streaming_links(raw_links)
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
            provider.name: _build_scraper_provider_settings(provider)
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
            scraper = SCRAPER_MANAGER.get_scraper(provider)
            if scraper is None:
                continue
            provider_updates: dict[str, object] = {}
            payload = values or {}

            next_page_value = payload.get("next_page")
            if next_page_value is not None:
                try:
                    next_page = int(next_page_value)
                except (TypeError, ValueError):
                    errors[f"{provider}_next_page"] = "Ungültige Zahl."
                else:
                    if next_page < 1:
                        errors[f"{provider}_next_page"] = "Wert muss größer oder gleich 1 sein."
                    else:
                        set_scraper_setting(provider, "next_page", next_page)
                        set_scraper_setting(provider, "last_page", max(0, next_page - 1))
                        provider_updates["next_page"] = next_page
                        provider_updates["last_page"] = max(0, next_page - 1)

            if "hosts" in payload:
                normalized_hosts, error = _normalize_scraper_host_values(
                    provider, payload.get("hosts")
                )
                if error:
                    errors[f"{provider}_hosts"] = error
                elif normalized_hosts is not None:
                    set_scraper_host_setting(provider, normalized_hosts)
                    _apply_scraper_host_preferences(scraper)
                    provider_updates["hosts"] = normalized_hosts

            if provider_updates:
                scraper_updates[provider] = provider_updates

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
        for provider, values in scraper_updates.items():
            scraper = SCRAPER_MANAGER.get_scraper(provider)
            if scraper is None:
                continue
            if "hosts" not in values:
                continue
            _apply_scraper_host_preferences(scraper)

    if scraper_updates:
        response_payload["scrapers"] = scraper_updates

    return jsonify({"success": True, "settings": response_payload})


@app.route("/api/stream-providers", methods=["GET", "POST"])
def api_stream_providers():
    if request.method == "GET":
        providers = collect_stream_provider_stats()
        return jsonify({"success": True, "providers": providers})

    data = request.get_json(silent=True) or {}
    action = data.get("action")
    provider_keys = data.get("provider_keys")
    if provider_keys is not None and not isinstance(provider_keys, list):
        return (
            jsonify({"success": False, "error": "provider_keys muss eine Liste sein."}),
            400,
        )

    result, error, status_code = apply_stream_provider_action(
        action, provider_keys or []
    )
    if status_code != 200:
        return jsonify({"success": False, "error": error or "Unbekannter Fehler."}), status_code

    providers = collect_stream_provider_stats()
    payload = {"success": True, "providers": providers}
    payload.update(result)
    return jsonify(payload)


ensure_database()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
