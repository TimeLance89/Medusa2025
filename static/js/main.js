const toast = document.getElementById('toast');
const detailPanel = document.querySelector('[data-section="detail"]');
const detailBackdrop = document.getElementById('detailBackdrop');
const detailPoster = document.getElementById('detailPoster');
const detailTitle = document.getElementById('detailTitle');
const detailTagline = document.getElementById('detailTagline');
const detailMeta = document.getElementById('detailMeta');
const detailOverview = document.getElementById('detailOverview');
const detailOverviewSecondary = document.getElementById('detailOverviewSecondary');
const detailCast = document.getElementById('detailCast');
const detailStreaming = document.getElementById('detailStreaming');
const detailTrailer = document.getElementById('detailTrailer');
const detailBackButton = detailPanel?.querySelector('.detail-back');

let changeSection = () => {};
let currentSectionName = 'start';
let previousSectionName = 'start';
let currentSettings = {
  tmdb_api_key: '',
  kinox_start_page: 1,
  kinox_end_page: 1,
};

function showToast(message, variant = 'info') {
  toast.textContent = message;
  toast.dataset.variant = variant;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 4000);
}

async function callApi(url, options = {}) {
  try {
    const response = await fetch(url, options);
    if (!response.ok) {
      let errorMessage = `Serverfehler: ${response.status}`;
      try {
        const errorData = await response.clone().json();
        if (errorData?.errors) {
          errorMessage = Object.values(errorData.errors).join(' ');
        } else if (errorData?.message) {
          errorMessage = errorData.message;
        }
      } catch (_) {
        // ignore json parse errors
      }
      throw new Error(errorMessage);
    }
    return await response.json();
  } catch (error) {
    console.error(error);
    showToast(error.message, 'error');
    throw error;
  }
}

async function syncTmdb() {
  try {
    showToast('TMDB Synchronisation gestartet...');
    const { success, count } = await callApi('/api/tmdb/popular');
    if (success) {
      showToast(`${count} Filme aktualisiert.`, 'success');
      window.location.reload();
    }
  } catch (_) {
    // Fehler bereits behandelt
  }
}

async function scrapeKinox() {
  try {
    showToast('Kinox Scraper läuft...');
    const startPage = Number(currentSettings.kinox_start_page) || 1;
    const endPage = Number(currentSettings.kinox_end_page) || startPage;
    const { success, links } = await callApi('/api/scrape/kinox', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start_page: startPage, end_page: endPage }),
    });
    if (success) {
      showToast(`${links.length} Links gespeichert.`, 'success');
      window.location.reload();
    }
  } catch (_) {
    // Fehler bereits behandelt
  }
}

async function resetScrapedContent() {
  if (!window.confirm('Möchtest du wirklich alle gescrapten Inhalte löschen?')) {
    return;
  }
  try {
    showToast('Gescrapte Inhalte werden gelöscht...');
    const { success, removed_links } = await callApi('/api/reset/scraped', {
      method: 'POST',
    });
    if (success) {
      showToast(`${removed_links} gescrapte Links gelöscht.`, 'success');
      window.location.reload();
    }
  } catch (_) {
    // Fehler bereits behandelt
  }
}

function initNavigation() {
  const menuItems = Array.from(document.querySelectorAll('.menu-item[data-target]'));
  const panels = Array.from(document.querySelectorAll('[data-section]'));

  function setSection(name) {
    panels.forEach((panel) => {
      const isActive = panel.dataset.section === name;
      panel.classList.toggle('active', isActive);
    });
    menuItems.forEach((item) => {
      item.classList.toggle('active', item.dataset.target === name);
    });
    window.scrollTo({ top: 0, behavior: 'smooth' });
    currentSectionName = name;
  }

  changeSection = setSection;

  menuItems.forEach((item) => {
    item.addEventListener('click', (event) => {
      event.preventDefault();
      const target = item.dataset.target;
      if (target) {
        setSection(target);
      }
    });
  });

  document.querySelectorAll('[data-target]').forEach((element) => {
    if (element.classList.contains('menu-item')) return;
    element.addEventListener('click', (event) => {
      event.preventDefault();
      const target = element.dataset.target;
      if (target) {
        setSection(target);
      }
    });
  });

  setSection('start');
}

function initHero() {
  const hero = document.getElementById('hero');
  const cards = Array.from(document.querySelectorAll('[data-section="start"] .card[data-movie-id]'));
  if (!hero || !cards.length) return;

  const heroTitle = hero.querySelector('h2');
  const heroDescription = hero.querySelector('p');

  function setHero(card) {
    const title = card.dataset.title || 'Unbekannter Titel';
    const overview = card.dataset.overview || 'Keine Beschreibung verfügbar.';
    const backdrop = card.dataset.backdrop || card.dataset.poster || '';
    const text = overview.length > 280 ? `${overview.slice(0, 277)}…` : overview;

    hero.style.setProperty('--hero-image', backdrop ? `url('${backdrop}')` : "url('')");
    heroTitle.textContent = title;
    heroDescription.textContent = text;
  }

  document.getElementById('refreshHero')?.addEventListener('click', () => {
    const randomCard = cards[Math.floor(Math.random() * cards.length)];
    if (randomCard) {
      setHero(randomCard);
    }
  });

  setHero(cards[0]);
}

function bindButtons() {
  const syncButton = document.getElementById('syncTmdb');
  const scrapeButton = document.getElementById('scrapeKinox');
  const settingsForm = document.getElementById('settingsForm');
  const resetButton = document.getElementById('resetScraped');

  syncButton?.addEventListener('click', syncTmdb);
  scrapeButton?.addEventListener('click', scrapeKinox);
  resetButton?.addEventListener('click', resetScrapedContent);

  if (settingsForm) {
    settingsForm.addEventListener('submit', saveSettings);
  }
}

async function loadSettings() {
  try {
    const settings = await callApi('/api/settings');
    currentSettings = { ...currentSettings, ...settings };
    const form = document.getElementById('settingsForm');
    if (!form) return;

    form.tmdb_api_key.value = settings.tmdb_api_key || '';
    form.kinox_start_page.value = settings.kinox_start_page ?? '';
    form.kinox_end_page.value = settings.kinox_end_page ?? '';
  } catch (_) {
    // Fehler bereits angezeigt
  }
}

async function saveSettings(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const payload = {
    tmdb_api_key: form.tmdb_api_key.value.trim(),
  };

  if (form.kinox_start_page.value) {
    payload.kinox_start_page = form.kinox_start_page.value;
  }

  if (form.kinox_end_page.value) {
    payload.kinox_end_page = form.kinox_end_page.value;
  }

  try {
    showToast('Einstellungen werden gespeichert...');
    const { success, settings } = await callApi('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (success) {
      currentSettings = { ...currentSettings, ...settings };
      showToast('Einstellungen gespeichert.', 'success');
    }
  } catch (_) {
    // Fehler bereits behandelt
  }
}

function bindContentCards() {
  const triggers = document.querySelectorAll('.card[data-movie-id], .view-detail[data-movie-id], .scraper-card[data-movie-id]');

  triggers.forEach((element) => {
    if (element.dataset.detailBound === 'true') return;
    element.dataset.detailBound = 'true';

    const activate = (event) => {
      if (event) {
        event.preventDefault();
        if (typeof event.stopPropagation === 'function') {
          event.stopPropagation();
        }
      }
      const movieId = element.dataset.movieId;
      if (movieId) {
        openMovieDetail(movieId);
      }
    };

    element.addEventListener('click', activate);

    if (element.classList.contains('card') || element.classList.contains('scraper-card')) {
      element.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          activate();
        }
      });
    }
  });
}

function resetDetailView() {
  if (detailBackdrop) {
    detailBackdrop.style.backgroundImage = 'none';
  }
  if (detailPoster) {
    detailPoster.src = detailPoster.dataset.placeholder || '';
    detailPoster.alt = 'Poster';
  }
  if (detailTitle) {
    detailTitle.textContent = '';
  }
  if (detailOverview) {
    detailOverview.textContent = '';
  }
  if (detailOverviewSecondary) {
    detailOverviewSecondary.textContent = '';
  }
  if (detailMeta) {
    detailMeta.innerHTML = '';
  }
  if (detailCast) {
    detailCast.innerHTML = '';
  }
  if (detailStreaming) {
    detailStreaming.innerHTML = '';
  }
  if (detailTrailer) {
    detailTrailer.innerHTML = '';
  }
  if (detailTagline) {
    detailTagline.textContent = '';
    detailTagline.style.display = 'none';
  }
}

function closeDetail() {
  if (!detailPanel) return;
  resetDetailView();
  changeSection(previousSectionName || 'start');
  previousSectionName = currentSectionName;
}

function showDetailLoadingState() {
  resetDetailView();
  if (detailTitle) {
    detailTitle.textContent = 'Lädt…';
  }
  if (detailOverview) {
    detailOverview.textContent = 'Details werden geladen…';
  }
  if (detailTrailer) {
    const loadingMessage = document.createElement('p');
    loadingMessage.classList.add('empty');
    loadingMessage.textContent = 'Trailer wird geladen…';
    detailTrailer.appendChild(loadingMessage);
  }
}

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && currentSectionName === 'detail') {
    closeDetail();
  }
});

detailBackButton?.addEventListener('click', closeDetail);

async function openMovieDetail(movieId) {
  try {
    if (currentSectionName !== 'detail') {
      previousSectionName = currentSectionName;
    }
    showDetailLoadingState();
    changeSection('detail');
    const { success, movie } = await callApi(`/api/movies/${movieId}`);
    if (!success) return;
    populateDetail(movie);
  } catch (_) {
    closeDetail();
  }
}

function populateDetail(movie) {
  if (!detailPanel || !detailPoster || !detailBackdrop || !detailTitle || !detailMeta || !detailCast || !detailStreaming || !detailOverview || !detailTagline) {
    return;
  }

  const posterPlaceholder = detailPoster.dataset.placeholder || '';
  const posterUrl = movie.poster_url || posterPlaceholder;
  const backdropUrl = movie.backdrop_url || posterUrl;

  detailPoster.src = posterUrl;
  detailPoster.alt = movie.title || 'Unbekannter Titel';
  detailBackdrop.style.backgroundImage = backdropUrl ? `url('${backdropUrl}')` : 'none';
  detailTitle.textContent = movie.title || 'Unbekannter Titel';

  if (movie.tagline) {
    detailTagline.textContent = movie.tagline;
    detailTagline.style.display = '';
  } else {
    detailTagline.textContent = '';
    detailTagline.style.display = 'none';
  }

  const overviewText = movie.overview || 'Keine Beschreibung verfügbar.';
  const overviewShort = overviewText.length > 220 ? `${overviewText.slice(0, 217)}…` : overviewText;
  detailOverview.textContent = overviewShort;
  if (detailOverviewSecondary) {
    detailOverviewSecondary.textContent = overviewText;
  }

  detailMeta.innerHTML = '';
  const metaEntries = [];
  const releaseYear = movie.release_date ? movie.release_date.split('-')[0] : '';
  if (releaseYear) {
    metaEntries.push(releaseYear);
  }
  if (Number.isFinite(movie.runtime) && movie.runtime > 0) {
    metaEntries.push(`${movie.runtime} Min.`);
  }
  if (typeof movie.rating === 'number' && movie.rating > 0) {
    metaEntries.push(`⭐ ${movie.rating.toFixed(1)}`);
  }
  if (Array.isArray(movie.genres)) {
    metaEntries.push(...movie.genres);
  }
  if (!metaEntries.length) {
    metaEntries.push('Keine zusätzlichen Infos');
  }
  metaEntries.forEach((entry) => {
    const span = document.createElement('span');
    span.textContent = entry;
    detailMeta.appendChild(span);
  });

  detailCast.innerHTML = '';
  if (Array.isArray(movie.cast) && movie.cast.length) {
    movie.cast.forEach((actor) => {
      const li = document.createElement('li');
      li.textContent = actor;
      detailCast.appendChild(li);
    });
  } else {
    const li = document.createElement('li');
    li.textContent = 'Keine Besetzung verfügbar.';
    li.classList.add('empty');
    detailCast.appendChild(li);
  }

  detailStreaming.innerHTML = '';
  if (Array.isArray(movie.streaming_links) && movie.streaming_links.length) {
    movie.streaming_links.forEach((link, index) => {
      const wrapper = document.createElement('div');
      wrapper.classList.add('stream-embed');

      const header = document.createElement('div');
      header.classList.add('stream-embed__header');
      header.textContent = `${link.source_name || 'Stream'}${
        link.mirror_info ? ' · ' + link.mirror_info : ''
      }`;

      const frame = document.createElement('iframe');
      frame.classList.add('stream-embed__frame');
      frame.src = link.url;
      frame.loading = 'lazy';
      frame.allowFullscreen = true;
      frame.referrerPolicy = 'no-referrer';
      frame.title = `${header.textContent} – Stream ${index + 1}`;
      frame.setAttribute('allow', 'fullscreen; picture-in-picture');

      const fallback = document.createElement('a');
      fallback.classList.add('stream-embed__link');
      fallback.href = link.url;
      fallback.target = '_blank';
      fallback.rel = 'noopener';
      fallback.textContent = 'Im neuen Tab öffnen';

      wrapper.appendChild(header);
      wrapper.appendChild(frame);
      wrapper.appendChild(fallback);
      detailStreaming.appendChild(wrapper);
    });
  } else {
    const empty = document.createElement('p');
    empty.classList.add('empty');
    empty.textContent = 'Keine Streams verfügbar.';
    detailStreaming.appendChild(empty);
  }

  if (detailTrailer) {
    detailTrailer.innerHTML = '';
    const trailer = movie.trailer;
    if (trailer?.site === 'YouTube' && trailer?.key) {
      const iframe = document.createElement('iframe');
      iframe.src = `https://www.youtube.com/embed/${trailer.key}?rel=0`;
      iframe.allowFullscreen = true;
      iframe.loading = 'lazy';
      iframe.referrerPolicy = 'no-referrer';
      iframe.title = `${movie.title || 'Trailer'} – ${trailer.name || 'YouTube'}`;
      iframe.setAttribute('allow', 'fullscreen; picture-in-picture; encrypted-media');
      detailTrailer.appendChild(iframe);
    } else {
      const emptyTrailer = document.createElement('p');
      emptyTrailer.classList.add('empty');
      emptyTrailer.textContent = 'Kein Trailer verfügbar.';
      detailTrailer.appendChild(emptyTrailer);
    }
  }
}

bindButtons();
initNavigation();
bindContentCards();
initHero();
loadSettings();
