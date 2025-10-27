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
const detailFacts = document.getElementById('detailFacts');
const detailBackButton = detailPanel?.querySelector('.detail-back');
const watchButton = document.getElementById('detailWatch');
const trailerButton = document.getElementById('detailTrailerButton');
const trailerModal = document.getElementById('trailerModal');
const trailerModalFrame = document.getElementById('trailerModalFrame');
const trailerModalTitle = document.getElementById('trailerModalTitle');
const trailerModalClose = trailerModal?.querySelector('.modal-close');
const trailerModalBackdrop = trailerModal?.querySelector('.modal-backdrop');
const watchButtonLabel = watchButton?.querySelector('span:last-child');
const watchButtonDefaultLabel = watchButtonLabel?.textContent || 'Ansehen';

let changeSection = () => {};
let currentSectionName = 'start';
let previousSectionName = 'start';
let currentSettings = {
  tmdb_api_key: '',
  kinox_start_page: 1,
  kinox_end_page: 1,
};
let currentTrailer = null;
let currentStreamingLinks = [];
let currentMovieTitle = '';

function showToast(message, variant = 'info') {
  toast.textContent = message;
  toast.dataset.variant = variant;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 4000);
}

function formatDate(value) {
  if (!value) return '';
  const [year, month, day] = String(value).split('-');
  if (year && month && day) {
    return `${day.padStart(2, '0')}.${month.padStart(2, '0')}.${year}`;
  }
  if (year && month) {
    return `${month.padStart(2, '0')}.${year}`;
  }
  return value;
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

function closeTrailerModal() {
  if (!trailerModal) return;
  trailerModal.classList.remove('show');
  trailerModal.setAttribute('aria-hidden', 'true');
  if (trailerModalFrame) {
    trailerModalFrame.src = '';
  }
}

function openTrailerModal() {
  if (!trailerModal || !currentTrailer) return;
  const embedUrl = `https://www.youtube.com/embed/${currentTrailer.key}?rel=0&autoplay=1`;
  trailerModal.classList.add('show');
  trailerModal.setAttribute('aria-hidden', 'false');
  if (trailerModalFrame) {
    trailerModalFrame.src = embedUrl;
  }
  if (trailerModalTitle) {
    const trailerName = currentTrailer.name || 'Trailer';
    const title = currentMovieTitle ? `${currentMovieTitle} – ${trailerName}` : trailerName;
    trailerModalTitle.textContent = title;
  }
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
  if (detailFacts) {
    detailFacts.innerHTML = '';
  }
  if (detailTagline) {
    detailTagline.textContent = '';
    detailTagline.style.display = 'none';
  }
  if (watchButton) {
    watchButton.disabled = true;
    watchButton.removeAttribute('data-stream-url');
    watchButton.removeAttribute('data-stream-name');
  }
  if (watchButtonLabel) {
    watchButtonLabel.textContent = watchButtonDefaultLabel;
  }
  if (trailerButton) {
    trailerButton.disabled = true;
  }
  currentTrailer = null;
  currentStreamingLinks = [];
  currentMovieTitle = '';
  closeTrailerModal();
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
  if (detailFacts) {
    const loadingFact = document.createElement('li');
    loadingFact.classList.add('empty');
    loadingFact.textContent = 'Details werden geladen…';
    detailFacts.appendChild(loadingFact);
  }
}

document.addEventListener('keydown', (event) => {
  if (event.key !== 'Escape') {
    return;
  }
  if (trailerModal?.classList.contains('show')) {
    closeTrailerModal();
    return;
  }
  if (currentSectionName === 'detail') {
    closeDetail();
  }
});

detailBackButton?.addEventListener('click', closeDetail);
watchButton?.addEventListener('click', () => {
  if (watchButton.disabled) {
    return;
  }
  const streamUrl = watchButton.dataset.streamUrl;
  if (streamUrl) {
    window.open(streamUrl, '_blank', 'noopener');
  } else if (detailStreaming) {
    detailStreaming.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
});
trailerButton?.addEventListener('click', openTrailerModal);
trailerModalClose?.addEventListener('click', closeTrailerModal);
trailerModalBackdrop?.addEventListener('click', closeTrailerModal);

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
  if (
    !detailPanel ||
    !detailPoster ||
    !detailBackdrop ||
    !detailTitle ||
    !detailMeta ||
    !detailCast ||
    !detailStreaming ||
    !detailOverview ||
    !detailTagline ||
    !detailFacts
  ) {
    return;
  }

  const posterPlaceholder = detailPoster.dataset.placeholder || '';
  const posterUrl = movie.poster_url || posterPlaceholder;
  const backdropUrl = movie.backdrop_url || posterUrl;

  currentMovieTitle = movie.title || 'Unbekannter Titel';

  detailPoster.src = posterUrl;
  detailPoster.alt = currentMovieTitle;
  detailBackdrop.style.backgroundImage = backdropUrl ? `url('${backdropUrl}')` : 'none';
  detailTitle.textContent = currentMovieTitle;

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
  if (Array.isArray(movie.genres) && movie.genres.length) {
    metaEntries.push(...movie.genres.slice(0, 3));
  }
  if (!metaEntries.length) {
    metaEntries.push('Keine zusätzlichen Infos');
  }
  metaEntries.forEach((entry) => {
    const span = document.createElement('span');
    span.textContent = entry;
    detailMeta.appendChild(span);
  });

  const streamingLinks = Array.isArray(movie.streaming_links)
    ? movie.streaming_links.filter((link) => link && link.url)
    : [];
  currentStreamingLinks = streamingLinks;

  if (watchButton) {
    if (streamingLinks.length) {
      const primary = streamingLinks[0];
      watchButton.disabled = false;
      watchButton.dataset.streamUrl = primary.url;
      watchButton.dataset.streamName = primary.source_name || '';
      if (watchButtonLabel) {
        const streamLabel = primary.source_name ? `Stream öffnen (${primary.source_name})` : 'Stream öffnen';
        watchButtonLabel.textContent = streamLabel;
      }
    } else {
      watchButton.disabled = true;
      watchButton.removeAttribute('data-stream-url');
      watchButton.removeAttribute('data-stream-name');
      if (watchButtonLabel) {
        watchButtonLabel.textContent = watchButtonDefaultLabel;
      }
    }
  }

  detailFacts.innerHTML = '';
  const facts = [];
  const formattedDate = formatDate(movie.release_date);
  if (formattedDate) {
    facts.push({ label: 'Veröffentlichung', value: formattedDate });
  }
  if (Number.isFinite(movie.runtime) && movie.runtime > 0) {
    facts.push({ label: 'Laufzeit', value: `${movie.runtime} Minuten` });
  }
  if (typeof movie.rating === 'number' && movie.rating > 0) {
    facts.push({ label: 'Bewertung', value: `${movie.rating.toFixed(1)} / 10` });
  }
  if (Array.isArray(movie.genres) && movie.genres.length) {
    facts.push({ label: movie.genres.length === 1 ? 'Genre' : 'Genres', value: movie.genres.join(', ') });
  }
  const streamCount = streamingLinks.length;
  facts.push({ label: 'Streams', value: streamCount ? `${streamCount} Quelle${streamCount === 1 ? '' : 'n'}` : 'Keine Quellen' });

  if (facts.length) {
    facts.forEach((fact) => {
      const li = document.createElement('li');
      const labelSpan = document.createElement('span');
      labelSpan.classList.add('detail-facts__label');
      labelSpan.textContent = fact.label;
      const valueSpan = document.createElement('span');
      valueSpan.classList.add('detail-facts__value');
      valueSpan.textContent = fact.value;
      li.appendChild(labelSpan);
      li.appendChild(valueSpan);
      detailFacts.appendChild(li);
    });
  } else {
    const emptyFact = document.createElement('li');
    emptyFact.classList.add('empty');
    emptyFact.textContent = 'Keine zusätzlichen Details.';
    detailFacts.appendChild(emptyFact);
  }

  detailCast.innerHTML = '';
  const castPlaceholder = detailCast.dataset.placeholder || '';
  const castEntries = Array.isArray(movie.cast) ? movie.cast.filter(Boolean) : [];

  if (castEntries.length) {
    castEntries.forEach((entry) => {
      const castItem = typeof entry === 'string' ? { name: entry } : entry || {};
      const name = (typeof castItem.name === 'string' && castItem.name.trim()) ||
        (typeof entry === 'string' ? entry : '');
      if (!name) {
        return;
      }

      const character = typeof castItem.character === 'string' ? castItem.character.trim() : '';
      const profilePath = castItem.profile_path || castItem.profilePath || '';
      const avatarUrl = profilePath ? `https://image.tmdb.org/t/p/w185${profilePath}` : castPlaceholder;

      const li = document.createElement('li');
      li.classList.add('cast-card');

      const avatar = document.createElement('div');
      avatar.classList.add('cast-card__avatar');

      if (avatarUrl) {
        const img = document.createElement('img');
        img.src = avatarUrl;
        img.alt = name;
        img.loading = 'lazy';
        avatar.appendChild(img);
      } else {
        avatar.classList.add('cast-card__avatar--initial');
        avatar.textContent = name.slice(0, 2).toUpperCase();
      }

      const body = document.createElement('div');
      body.classList.add('cast-card__body');

      const nameEl = document.createElement('span');
      nameEl.classList.add('cast-card__name');
      nameEl.textContent = name;
      body.appendChild(nameEl);

      const roleEl = document.createElement('span');
      roleEl.classList.add('cast-card__role');
      if (character) {
        roleEl.textContent = character;
      } else {
        roleEl.textContent = 'Darsteller:in';
        roleEl.classList.add('cast-card__role--muted');
      }
      body.appendChild(roleEl);

      li.appendChild(avatar);
      li.appendChild(body);
      detailCast.appendChild(li);
    });

    if (!detailCast.children.length) {
      const li = document.createElement('li');
      li.textContent = 'Keine Besetzung verfügbar.';
      li.classList.add('empty');
      detailCast.appendChild(li);
    }
  } else {
    const li = document.createElement('li');
    li.textContent = 'Keine Besetzung verfügbar.';
    li.classList.add('empty');
    detailCast.appendChild(li);
  }

  detailStreaming.innerHTML = '';
  if (streamingLinks.length) {
    streamingLinks.forEach((link, index) => {
      const wrapper = document.createElement('div');
      wrapper.classList.add('stream-embed');

      const header = document.createElement('div');
      header.classList.add('stream-embed__header');
      header.textContent = `${link.source_name || 'Stream'}${link.mirror_info ? ' · ' + link.mirror_info : ''}`;

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

  currentTrailer = null;
  if (detailTrailer) {
    detailTrailer.innerHTML = '';
    const trailer = movie.trailer;
    if (trailer?.site === 'YouTube' && trailer?.key) {
      currentTrailer = trailer;
      const thumbnail = `https://img.youtube.com/vi/${trailer.key}/hqdefault.jpg`;
      const card = document.createElement('button');
      card.type = 'button';
      card.classList.add('trailer-card');
      card.innerHTML = `
        <div class="trailer-card__media" style="background-image: url('${thumbnail}')">
          <span class="trailer-card__play">▶</span>
        </div>
        <div class="trailer-card__body">
          <h4>${currentMovieTitle}</h4>
          <p>${trailer.name || 'YouTube'}</p>
        </div>
      `;
      card.addEventListener('click', openTrailerModal);
      detailTrailer.appendChild(card);
    } else {
      const emptyTrailer = document.createElement('p');
      emptyTrailer.classList.add('empty');
      emptyTrailer.textContent = 'Kein Trailer verfügbar.';
      detailTrailer.appendChild(emptyTrailer);
    }
  }

  if (trailerButton) {
    trailerButton.disabled = !currentTrailer;
  }
}

bindButtons();
initNavigation();
bindContentCards();
initHero();
loadSettings();
