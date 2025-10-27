# Medusa Media Server

Ein schlanker Medienserver mit Plex-ähnlicher Oberfläche, TMDB-Integration und Scraper für externe Mirror-Links.

## Features

- **Plex-inspirierte UI** in HTML/CSS mit dunklem Design und Hero-Bereich.
- **REST API** auf Basis von Flask zur Verwaltung der Bibliothek.
- **TMDB-Anbindung**: Importiert Filmdaten inklusive Poster, Backdrop, Beschreibung und Genres.
- **Scraper** für Seiten wie `kinox.farm`, die Mirror-Links erfasst und speichert.
- **SQLite-Datenbank** (lokale Datei `medusa.db`) über SQLAlchemy verwaltet.

## Voraussetzungen

- Python 3.10+
- Abhängigkeiten aus `requirements.txt`
- TMDB API Key als Umgebungsvariable `TMDB_API_KEY`

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export TMDB_API_KEY="<dein_api_key>"
python server.py
```

Der Server läuft anschließend standardmäßig auf `http://127.0.0.1:5000`.

## API Endpunkte

| Methode | Pfad                | Beschreibung                              |
|---------|--------------------|--------------------------------------------|
| GET     | `/api/movies`      | Liste aller gespeicherten Filme            |
| POST    | `/api/movies`      | Import eines Films via `query` oder `tmdb_id` |
| GET     | `/api/scraped-links` | Liste aller gespeicherten Mirror-Links    |
| POST    | `/api/scrape`      | Startet den Scraper für eine Basis-URL     |

## Scraper verwenden

Das Steuerpanel (⚙️) enthält ein Formular zur Konfiguration:

- **Basis-URL**: z. B. `https://kinox.farm/kinofilme-online/page/{page}/`
- **Start-/Endseite**: numerischer Bereich der zu durchsuchenden Seiten.

Der Scraper erfasst den Filmtitel, die Detailseite sowie jeden `data-link` Mirror-Eintrag inklusive Anbieterinformationen.

## Hinweise

- Die SQLite-Datenbank wird beim ersten Start automatisch angelegt.
- Das Frontend lädt Bibliothek und Scraper-Ergebnisse beim Start automatisch.
- Tests werden nicht automatisch ausgeführt; der Nutzer kann diese selbst übernehmen.

Viel Spaß mit deinem individuellen Mediensystem! 🜁
