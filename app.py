import os
from datetime import datetime
from typing import List, Optional

import requests
from flask import Flask, jsonify, render_template, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from sqlalchemy.engine import make_url

from scrapers.kinox import scrape_detail as scrape_kinox_detail
from scrapers.kinox import scrape_page as scrape_kinox_page


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "database", "mediahub.db")
DATABASE_URL = os.environ.get("DATABASE_URL", f"sqlite:///{DEFAULT_DB_PATH}")
_ENV_TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)


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


def attach_streaming_link(movie_title: str, streaming_url: str, mirror_info: Optional[str] = None) -> StreamingLink:
    """Create or update a streaming link for a movie based on its title."""
    movie = Movie.query.filter_by(title=movie_title).first()
    if movie is None:
        movie = Movie(tmdb_id=_generate_placeholder_tmdb_id(), title=movie_title)
        db.session.add(movie)
        db.session.flush()

    link = StreamingLink.query.filter_by(movie_id=movie.id, url=streaming_url).first()
    if link is None:
        link = StreamingLink(movie=movie, url=streaming_url, source_name="Kinox", mirror_info=mirror_info)
    else:
        link.source_name = "Kinox"
        link.mirror_info = mirror_info
    db.session.add(link)
    db.session.commit()
    return link


@app.route("/")
def index():
    categories = {
        "Beliebt": Movie.query.order_by(Movie.rating.desc().nullslast()).limit(20).all(),
        "Neu hinzugefügt": Movie.query.order_by(Movie.created_at.desc()).limit(20).all(),
    }
    scraped = StreamingLink.query.order_by(StreamingLink.id.desc()).limit(25).all()
    return render_template("index.html", categories=categories, scraped=scraped)


@app.route("/api/movies")
def api_movies():
    movies = Movie.query.order_by(Movie.rating.desc().nullslast()).all()
    return jsonify([movie.to_dict() for movie in movies])


@app.route("/api/scrape/kinox", methods=["POST"])
def api_scrape_kinox():
    data = request.get_json(silent=True) or {}
    start_page = int(
        data.get("start_page")
        or get_int_setting("kinox_start_page", 1)
    )
    end_page = int(
        data.get("end_page")
        or get_int_setting("kinox_end_page", start_page)
    )

    if end_page < start_page:
        start_page, end_page = end_page, start_page

    collected: List[dict] = []
    for page in range(start_page, end_page + 1):
        entries = scrape_kinox_page(page)
        for entry in entries:
            link = attach_streaming_link(entry["title"], entry["streaming_url"], entry.get("mirror"))
            collected.append(link.to_dict())
    return jsonify({"success": True, "links": collected})


@app.route("/api/tmdb/<category>")
def api_tmdb(category: str):
    page = int(request.args.get("page", 1))
    tmdb_movies = fetch_tmdb_movies(category=category, page=page)
    movies = upsert_movies(tmdb_movies)
    return jsonify({"success": True, "count": len(movies)})


@app.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    if request.method == "GET":
        return jsonify(
            {
                "tmdb_api_key": get_tmdb_api_key(),
                "kinox_start_page": get_int_setting("kinox_start_page", 1),
                "kinox_end_page": get_int_setting("kinox_end_page", 1),
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

    for key in ("kinox_start_page", "kinox_end_page"):
        if key in data:
            try:
                value = int(data[key])
            except (TypeError, ValueError):
                errors[key] = "Ungültige Zahl."
                continue
            if value < 1:
                errors[key] = "Wert muss größer oder gleich 1 sein."
                continue
            set_setting(key, str(value))
            response_payload[key] = value

    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    return jsonify({"success": True, "settings": response_payload})


ensure_database()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
