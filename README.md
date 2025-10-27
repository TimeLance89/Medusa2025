# Medusa Media Server

Medusa ist ein leichtgewichtiger Python-Medienserver, der optisch an Plex angelehnt ist und Inhalte über die TMDB-API sowie konfigurierbare Scraper (z. B. Kinox) verwaltet. Die Anwendung kombiniert einen Flask-Backend-Server mit einem modernen Frontend auf Basis von HTML, CSS und JavaScript.

## Features

- **TMDB-Integration** – Filme können aus beliebigen Kategorien (standardmäßig `popular`) synchronisiert und dauerhaft in einer Datenbank gespeichert werden.
- **Kinox-Scraper** – Beispiel-Scraper, der Filmnamen samt Streaming-Links aus `kinox.farm` extrahiert und in der Datenbank ablegt.
- **Plex-inspiriertes UI** – Responsive Oberfläche mit modernen Netflix/Plex-Anmutungen.
- **SQLite by default** – Nutzt SQLite out-of-the-box, kann aber über `DATABASE_URL` auf andere SQL-Datenbanken (z. B. MySQL, PostgreSQL) umgestellt werden.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate  # oder .venv\Scripts\activate auf Windows
pip install -r requirements.txt
```

## Konfiguration

Setze die Umgebungsvariablen, bevor du den Server startest:

```bash
export TMDB_API_KEY="dein_tmdb_api_key"
# optional, Standard ist sqlite:///database/mediahub.db
export DATABASE_URL="sqlite:///database/mediahub.db"
```

## Datenbank initialisieren und Server starten

```bash
python app.py
```

Der Server läuft standardmäßig unter `http://localhost:8000`.

## Endpunkte

- `GET /` – Rendert die Medienübersicht.
- `GET /api/movies` – Gibt alle gespeicherten Filme als JSON zurück.
- `GET /api/tmdb/<category>?page=<n>` – Synchronisiert Inhalte aus der angegebenen TMDB-Kategorie.
- `POST /api/scrape/kinox` – Führt den Beispiel-Scraper aus (`start_page`, `end_page`).

## Hinweis zum Scraping

Die Kinox-Integration dient als Beispiel. Bitte prüfe vor dem Einsatz die rechtliche Lage und die Nutzungsbedingungen der Zielseiten. Passe den Scraper bei Bedarf in `app.py` oder in separaten Modulen unter `scrapers/` an.
