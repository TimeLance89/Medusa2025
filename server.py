import os
from datetime import datetime
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request, send_from_directory
from flask_sqlalchemy import SQLAlchemy


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "medusa.db")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DB_PATH}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False


db = SQLAlchemy(app)


class Movie(db.Model):
    __tablename__ = "movies"

    id = db.Column(db.Integer, primary_key=True)
    tmdb_id = db.Column(db.Integer, unique=True, index=True, nullable=False)
    title = db.Column(db.String(255), nullable=False)
    overview = db.Column(db.Text)
    poster_url = db.Column(db.String(512))
    backdrop_url = db.Column(db.String(512))
    release_date = db.Column(db.String(32))
    rating = db.Column(db.Float)
    genres = db.Column(db.String(255))
    runtime = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "tmdb_id": self.tmdb_id,
            "title": self.title,
            "overview": self.overview,
            "poster_url": self.poster_url,
            "backdrop_url": self.backdrop_url,
            "release_date": self.release_date,
            "rating": self.rating,
            "genres": self.genres.split(",") if self.genres else [],
            "runtime": self.runtime,
            "created_at": self.created_at.isoformat(),
        }


class ScrapedLink(db.Model):
    __tablename__ = "scraped_links"
    __table_args__ = (
        db.UniqueConstraint("movie_title", "embed_url", name="uq_movie_embed"),
    )

    id = db.Column(db.Integer, primary_key=True)
    movie_title = db.Column(db.String(255), nullable=False)
    detail_url = db.Column(db.String(512), nullable=False)
    embed_url = db.Column(db.String(512), nullable=False)
    provider = db.Column(db.String(128))
    mirror_info = db.Column(db.String(128))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "movie_title": self.movie_title,
            "detail_url": self.detail_url,
            "embed_url": self.embed_url,
            "provider": self.provider,
            "mirror_info": self.mirror_info,
            "created_at": self.created_at.isoformat(),
        }


def create_tables() -> None:
    with app.app_context():
        db.create_all()


create_tables()


@app.route("/")
def root() -> str:
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/<path:path>")
def serve_static(path: str):
    return send_from_directory(BASE_DIR, path)


@app.route("/api/health")
def health_check():
    return {"status": "ok"}


@app.route("/api/movies", methods=["GET", "POST"])
def movies():
    if request.method == "GET":
        query = request.args.get("q", "").strip()
        stmt = Movie.query
        if query:
            like = f"%{query.lower()}%"
            stmt = stmt.filter(db.func.lower(Movie.title).like(like))
        movies_data = [movie.to_dict() for movie in stmt.order_by(Movie.created_at.desc()).all()]
        return jsonify(movies_data)

    payload = request.get_json(force=True)
    tmdb_id = payload.get("tmdb_id")
    query = payload.get("query")

    if not tmdb_id and not query:
        return jsonify({"error": "'query' oder 'tmdb_id' muss angegeben werden."}), 400

    try:
        movie_data = fetch_tmdb_movie(tmdb_id=tmdb_id, query=query)
    except TMDBConfigurationError as exc:
        return jsonify({"error": str(exc)}), 500
    except TMDBNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    except requests.RequestException as exc:
        return jsonify({"error": f"TMDB Anfrage fehlgeschlagen: {exc}"}), 502

    movie = Movie.query.filter_by(tmdb_id=movie_data["tmdb_id"]).first()
    if movie:
        for key, value in movie_data.items():
            if key == "genres":
                movie.genres = ",".join(value)
            elif hasattr(movie, key):
                setattr(movie, key, value)
    else:
        movie = Movie(
            tmdb_id=movie_data["tmdb_id"],
            title=movie_data["title"],
            overview=movie_data.get("overview"),
            poster_url=movie_data.get("poster_url"),
            backdrop_url=movie_data.get("backdrop_url"),
            release_date=movie_data.get("release_date"),
            rating=movie_data.get("rating"),
            genres=",".join(movie_data.get("genres", [])),
            runtime=movie_data.get("runtime"),
        )
        db.session.add(movie)

    db.session.commit()
    return jsonify(movie.to_dict()), 201


@app.route("/api/scraped-links", methods=["GET"])
def list_scraped_links():
    query = request.args.get("q", "").strip()
    stmt = ScrapedLink.query
    if query:
        like = f"%{query.lower()}%"
        stmt = stmt.filter(db.func.lower(ScrapedLink.movie_title).like(like))
    links = [link.to_dict() for link in stmt.order_by(ScrapedLink.created_at.desc()).all()]
    return jsonify(links)


@app.route("/api/scrape", methods=["POST"])
def scrape_endpoint():
    payload = request.get_json(force=True)
    base_url = payload.get("base_url")
    start_page = int(payload.get("start_page", 1))
    end_page = int(payload.get("end_page", start_page))

    if not base_url:
        return jsonify({"error": "'base_url' ist erforderlich."}), 400

    try:
        results = scrape_kinox(base_url, start_page, end_page)
    except requests.RequestException as exc:
        return jsonify({"error": f"Scraping fehlgeschlagen: {exc}"}), 502

    created = 0
    for entry in results:
        link = ScrapedLink.query.filter_by(
            movie_title=entry["movie_title"], embed_url=entry["embed_url"]
        ).first()
        if link:
            continue
        link = ScrapedLink(
            movie_title=entry["movie_title"],
            detail_url=entry["detail_url"],
            embed_url=entry["embed_url"],
            provider=entry.get("provider"),
            mirror_info=entry.get("mirror_info"),
        )
        db.session.add(link)
        created += 1

    db.session.commit()
    return jsonify({"created": created, "total_found": len(results)})


class TMDBConfigurationError(RuntimeError):
    pass


class TMDBNotFoundError(RuntimeError):
    pass


def fetch_tmdb_movie(*, tmdb_id: Optional[int] = None, query: Optional[str] = None) -> Dict:
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise TMDBConfigurationError(
            "TMDB_API_KEY ist nicht gesetzt. Bitte legen Sie einen API-Schlüssel als Umgebungsvariable fest."
        )

    session = requests.Session()
    session.params = {"api_key": api_key, "language": "de-DE"}

    if tmdb_id is None:
        search_resp = session.get(
            "https://api.themoviedb.org/3/search/movie",
            params={"query": query, "include_adult": "false"},
            timeout=15,
        )
        search_resp.raise_for_status()
        results = search_resp.json().get("results", [])
        if not results:
            raise TMDBNotFoundError(f"Kein Film für '{query}' gefunden.")
        tmdb_id = results[0]["id"]

    detail_resp = session.get(
        f"https://api.themoviedb.org/3/movie/{tmdb_id}",
        params={"append_to_response": "credits"},
        timeout=15,
    )
    if detail_resp.status_code == 404:
        raise TMDBNotFoundError(f"Film mit TMDB ID {tmdb_id} wurde nicht gefunden.")
    detail_resp.raise_for_status()
    data = detail_resp.json()

    poster_url = (
        f"https://image.tmdb.org/t/p/w500{data['poster_path']}"
        if data.get("poster_path")
        else None
    )
    backdrop_url = (
        f"https://image.tmdb.org/t/p/original{data['backdrop_path']}"
        if data.get("backdrop_path")
        else None
    )

    genres = [genre["name"] for genre in data.get("genres", [])]

    return {
        "tmdb_id": data["id"],
        "title": data.get("title") or data.get("name"),
        "overview": data.get("overview"),
        "poster_url": poster_url,
        "backdrop_url": backdrop_url,
        "release_date": data.get("release_date"),
        "rating": data.get("vote_average"),
        "genres": genres,
        "runtime": data.get("runtime"),
    }


def normalise_page_url(base_url: str, page: int) -> str:
    if "{page}" in base_url:
        return base_url.format(page=page)
    base_url = base_url.rstrip("/")
    if base_url.endswith(str(page)):
        return base_url
    return f"{base_url}/page/{page}/"


def scrape_kinox(base_url: str, start_page: int, end_page: int) -> List[Dict]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
    )

    collected: List[Dict] = []
    for page in range(start_page, end_page + 1):
        page_url = normalise_page_url(base_url, page)
        resp = session.get(page_url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for anchor in soup.select(".short-entry-title a"):
            title = anchor.get_text(strip=True)
            detail_url = urljoin(page_url, anchor.get("href"))
            for embed in scrape_detail_page(session, detail_url):
                collected.append(
                    {
                        "movie_title": title,
                        "detail_url": detail_url,
                        "embed_url": embed["embed_url"],
                        "provider": embed.get("provider"),
                        "mirror_info": embed.get("mirror_info"),
                    }
                )
    return collected


def scrape_detail_page(session: requests.Session, detail_url: str) -> Iterable[Dict]:
    resp = session.get(detail_url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for item in soup.select("li[data-link]"):
        embed_url = item.get("data-link")
        if not embed_url:
            continue
        provider_el = item.select_one(".Named")
        data_el = item.select_one(".Data")
        yield {
            "embed_url": embed_url,
            "provider": provider_el.get_text(strip=True) if provider_el else None,
            "mirror_info": data_el.get_text(strip=True) if data_el else None,
        }


if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    app.run(host=host, port=port, debug=True)
