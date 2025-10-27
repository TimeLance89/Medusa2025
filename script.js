const state = {
    movies: [],
    filteredMovies: [],
    scrapedLinks: [],
};

const elements = {
    hero: document.getElementById('hero'),
    heroTitle: document.getElementById('heroTitle'),
    heroDescription: document.getElementById('heroDescription'),
    libraryCarousel: document.getElementById('libraryCarousel'),
    libraryCount: document.getElementById('libraryCount'),
    scrapeTableBody: document.getElementById('scrapeTableBody'),
    scrapeCount: document.getElementById('scrapeCount'),
    searchInput: document.getElementById('movieSearch'),
    tmdbForm: document.getElementById('tmdbForm'),
    scraperForm: document.getElementById('scraperForm'),
    statusLog: document.getElementById('statusLog'),
    controlPanel: document.getElementById('controlPanel'),
    openControlPanel: document.getElementById('openControlPanel'),
    closeControlPanel: document.getElementById('closeControlPanel'),
    panelBackdrop: document.getElementById('panelBackdrop'),
    playHero: document.getElementById('playHero'),
    moreInfoHero: document.getElementById('moreInfoHero'),
};

async function fetchJSON(url, options = {}) {
    const response = await fetch(url, {
        headers: {
            'Content-Type': 'application/json',
        },
        ...options,
    });

    if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `HTTP ${response.status}`);
    }
    return response.json();
}

function setHero(movie) {
    if (!movie) {
        elements.hero.style.backgroundImage = '';
        elements.heroTitle.textContent = 'Deine Mediensammlung';
        elements.heroDescription.textContent = 'Synchronisiere Filme von TMDB, sammle Mirror-Links aus dem Netz und erlebe deine persönliche Plex-ähnliche Oberfläche direkt im Browser.';
        return;
    }

    if (movie.backdrop_url) {
        elements.hero.style.backgroundImage = `url(${movie.backdrop_url})`;
    } else if (movie.poster_url) {
        elements.hero.style.backgroundImage = `url(${movie.poster_url})`;
    } else {
        elements.hero.style.backgroundImage = '';
    }

    elements.heroTitle.textContent = movie.title;
    const releaseYear = movie.release_date ? ` (${movie.release_date.slice(0, 4)})` : '';
    const rating = movie.rating ? ` • Bewertung ${movie.rating.toFixed(1)}` : '';
    elements.heroDescription.textContent = `${movie.overview || 'Keine Beschreibung verfügbar.'}${releaseYear}${rating}`;

    elements.playHero.onclick = () => {
        if (movie.trailer_url) {
            window.open(movie.trailer_url, '_blank');
        } else {
            logStatus(`Kein Trailer-Link für ${movie.title} vorhanden.`, 'warning');
        }
    };

    elements.moreInfoHero.onclick = () => {
        const message = [
            `Titel: ${movie.title}`,
            movie.genres?.length ? `Genres: ${movie.genres.join(', ')}` : null,
            movie.runtime ? `Laufzeit: ${movie.runtime} Minuten` : null,
            movie.overview || null,
        ]
            .filter(Boolean)
            .join('\n');
        alert(message || 'Keine zusätzlichen Informationen verfügbar.');
    };
}

function renderLibrary() {
    elements.libraryCarousel.innerHTML = '';
    const movies = state.filteredMovies;

    if (!movies.length) {
        const empty = document.createElement('div');
        empty.className = 'empty-state';
        empty.textContent = 'Noch keine Filme importiert. Verwende den TMDB-Import im Steuerpanel.';
        elements.libraryCarousel.appendChild(empty);
        setHero(null);
        elements.libraryCount.textContent = '0 Titel';
        return;
    }

    movies.forEach((movie) => {
        const card = document.createElement('article');
        card.className = 'media-card';
        card.style.setProperty('--poster', movie.poster_url ? `url(${movie.poster_url})` : 'none');

        const poster = document.createElement('div');
        poster.className = 'poster';
        if (movie.poster_url) {
            poster.style.backgroundImage = `url(${movie.poster_url})`;
        }

        const info = document.createElement('div');
        info.className = 'info';

        const title = document.createElement('h3');
        title.textContent = movie.title;

        const meta = document.createElement('p');
        meta.className = 'meta';
        const year = movie.release_date ? movie.release_date.slice(0, 4) : 'Unbekannt';
        const genres = movie.genres && movie.genres.length ? movie.genres.slice(0, 3).join(' • ') : 'Keine Genres';
        meta.textContent = `${year} • ${genres}`;

        const rating = document.createElement('span');
        rating.className = 'rating';
        rating.textContent = movie.rating ? movie.rating.toFixed(1) : 'N/A';

        info.appendChild(title);
        info.appendChild(meta);
        info.appendChild(rating);

        card.appendChild(poster);
        card.appendChild(info);

        card.addEventListener('click', () => setHero(movie));
        elements.libraryCarousel.appendChild(card);
    });

    setHero(movies[0]);
    elements.libraryCount.textContent = `${movies.length} ${movies.length === 1 ? 'Titel' : 'Titel'}`;
}

function renderScraperTable() {
    elements.scrapeTableBody.innerHTML = '';

    if (!state.scrapedLinks.length) {
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = 5;
        cell.textContent = 'Noch keine Scraper-Ergebnisse vorhanden.';
        row.appendChild(cell);
        elements.scrapeTableBody.appendChild(row);
        elements.scrapeCount.textContent = '0 Links';
        return;
    }

    state.scrapedLinks.forEach((link) => {
        const row = document.createElement('tr');

        const titleCell = document.createElement('td');
        titleCell.textContent = link.movie_title;

        const providerCell = document.createElement('td');
        providerCell.textContent = link.provider || '–';

        const mirrorCell = document.createElement('td');
        mirrorCell.textContent = link.mirror_info || '–';

        const embedCell = document.createElement('td');
        const embedLink = document.createElement('a');
        embedLink.href = link.embed_url;
        embedLink.target = '_blank';
        embedLink.rel = 'noopener noreferrer';
        embedLink.textContent = 'Öffnen';
        embedCell.appendChild(embedLink);

        const sourceCell = document.createElement('td');
        const sourceLink = document.createElement('a');
        sourceLink.href = link.detail_url;
        sourceLink.target = '_blank';
        sourceLink.rel = 'noopener noreferrer';
        sourceLink.textContent = 'Quelle';
        sourceCell.appendChild(sourceLink);

        row.appendChild(titleCell);
        row.appendChild(providerCell);
        row.appendChild(mirrorCell);
        row.appendChild(embedCell);
        row.appendChild(sourceCell);

        elements.scrapeTableBody.appendChild(row);
    });

    elements.scrapeCount.textContent = `${state.scrapedLinks.length} ${state.scrapedLinks.length === 1 ? 'Link' : 'Links'}`;
}

function applySearchFilter() {
    const query = elements.searchInput.value.trim().toLowerCase();
    if (!query) {
        state.filteredMovies = [...state.movies];
        renderLibrary();
        return;
    }

    state.filteredMovies = state.movies.filter((movie) =>
        movie.title.toLowerCase().includes(query) ||
        movie.genres?.some((genre) => genre.toLowerCase().includes(query)),
    );
    renderLibrary();
}

function logStatus(message, level = 'info') {
    if (!elements.statusLog) return;
    const item = document.createElement('li');
    item.className = `status-${level}`;
    const timestamp = new Date().toLocaleTimeString();
    item.textContent = `[${timestamp}] ${message}`;
    elements.statusLog.prepend(item);
}

async function loadMovies() {
    try {
        const movies = await fetchJSON('/api/movies');
        state.movies = movies;
        state.filteredMovies = [...movies];
        renderLibrary();
        logStatus('Bibliothek erfolgreich geladen.');
    } catch (error) {
        console.error(error);
        logStatus(`Fehler beim Laden der Bibliothek: ${error.message}`, 'error');
    }
}

async function loadScrapedLinks() {
    try {
        const links = await fetchJSON('/api/scraped-links');
        state.scrapedLinks = links;
        renderScraperTable();
        logStatus('Scraper-Einträge aktualisiert.');
    } catch (error) {
        console.error(error);
        logStatus(`Fehler beim Laden der Scraper-Einträge: ${error.message}`, 'error');
    }
}

function handlePanel(open) {
    if (open) {
        elements.controlPanel.classList.add('open');
        elements.panelBackdrop.classList.add('visible');
    } else {
        elements.controlPanel.classList.remove('open');
        elements.panelBackdrop.classList.remove('visible');
    }
}

function initEventListeners() {
    if (elements.searchInput) {
        elements.searchInput.addEventListener('input', applySearchFilter);
    }

    if (elements.tmdbForm) {
        elements.tmdbForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            const value = document.getElementById('tmdbQuery').value.trim();
            if (!value) {
                return;
            }

            const payload = {};
            if (/^\d+$/.test(value)) {
                payload.tmdb_id = parseInt(value, 10);
            } else {
                payload.query = value;
            }

            logStatus(`Importiere "${value}" von TMDB...`);
            try {
                const movie = await fetchJSON('/api/movies', {
                    method: 'POST',
                    body: JSON.stringify(payload),
                });
                document.getElementById('tmdbQuery').value = '';
                logStatus(`"${movie.title}" wurde zur Bibliothek hinzugefügt.`, 'success');
                await loadMovies();
            } catch (error) {
                console.error(error);
                logStatus(`Import fehlgeschlagen: ${error.message}`, 'error');
            }
        });
    }

    if (elements.scraperForm) {
        elements.scraperForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            const baseUrl = document.getElementById('scraperBaseUrl').value.trim();
            const startPage = parseInt(document.getElementById('scraperStart').value, 10) || 1;
            const endPage = parseInt(document.getElementById('scraperEnd').value, 10) || startPage;

            logStatus(`Starte Scraping von ${baseUrl} (Seiten ${startPage}-${endPage})...`);
            try {
                const result = await fetchJSON('/api/scrape', {
                    method: 'POST',
                    body: JSON.stringify({ base_url: baseUrl, start_page: startPage, end_page: endPage }),
                });
                logStatus(`Scraping abgeschlossen. ${result.created} neue Links gespeichert (insgesamt ${result.total_found}).`, 'success');
                await loadScrapedLinks();
            } catch (error) {
                console.error(error);
                logStatus(`Scraping fehlgeschlagen: ${error.message}`, 'error');
            }
        });
    }

    if (elements.openControlPanel) {
        elements.openControlPanel.addEventListener('click', () => handlePanel(true));
    }

    if (elements.closeControlPanel) {
        elements.closeControlPanel.addEventListener('click', () => handlePanel(false));
    }

    if (elements.panelBackdrop) {
        elements.panelBackdrop.addEventListener('click', () => handlePanel(false));
    }
}

function init() {
    initEventListeners();
    loadMovies();
    loadScrapedLinks();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
