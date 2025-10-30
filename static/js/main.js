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
const detailFacts = document.getElementById('detailFacts');
const detailLabel = document.getElementById('detailLabel');
const detailEpisodesSection = document.getElementById('detailEpisodesSection');
const detailSeasonSelect = document.getElementById('detailSeasonSelect');
const detailEpisodeList = document.getElementById('detailEpisodeList');
const detailEpisodeTitle = document.getElementById('detailEpisodeTitle');
const detailEpisodeMeta = document.getElementById('detailEpisodeMeta');
const detailEpisodeOverview = document.getElementById('detailEpisodeOverview');
const detailBackButton = detailPanel?.querySelector('.detail-back');
const watchButton = document.getElementById('detailWatch');
const trailerButton = document.getElementById('detailTrailerButton');
const detailAllMoviesGrid = document.getElementById('detailAllMovies');
const detailAllPageLabels = Array.from(
  document.querySelectorAll('[data-role="detailAllPageLabel"]')
);
const detailAllPrevButtons = Array.from(
  document.querySelectorAll('[data-role="detailAllPrev"]')
);
const detailAllNextButtons = Array.from(
  document.querySelectorAll('[data-role="detailAllNext"]')
);
const trailerModal = document.getElementById('trailerModal');
const trailerModalFrame = document.getElementById('trailerModalFrame');
const trailerModalTitle = document.getElementById('trailerModalTitle');
const trailerModalClose = trailerModal?.querySelector('.modal-close');
const trailerModalBackdrop = trailerModal?.querySelector('.modal-backdrop');
const watchButtonLabel = document.getElementById('detailWatchLabel');
const watchButtonDefaultLabel = watchButtonLabel?.textContent || 'Ansehen';
const heroSection = document.getElementById('hero');
const heroPosterImage = heroSection?.querySelector('.hero__poster-image');
const heroPlayButton = document.getElementById('detailHeroPlay');
const heroMeta = heroSection?.querySelector('.hero__meta');
const heroIndicatorsContainer = heroSection?.querySelector('.hero__indicators');
const heroPrevButton = heroSection?.querySelector('[data-hero-prev]');
const heroNextButton = heroSection?.querySelector('[data-hero-next]');
const heroDataElement = document.getElementById('heroData');
const topbarScrapeButton = document.getElementById('scrapeAllScrapers');
const topbarSearchForm = document.querySelector('.topbar-search');
const topbarSearchInput = topbarSearchForm?.querySelector('input[name="q"]');
const topbarSearchResults = document.getElementById('topbarSearchResults');
const topbarSearchList = document.getElementById('topbarSearchList');
const searchOverlay = document.getElementById('searchOverlay');
const searchOverlayTitle = document.getElementById('searchOverlayTitle');
const searchOverlayMeta = document.getElementById('searchOverlayMeta');
const searchOverlayList = document.getElementById('searchOverlayList');
const searchOverlayEmpty = document.getElementById('searchOverlayEmpty');
const searchOverlayClose = document.getElementById('searchOverlayClose');
const profileFormWrapper = document.querySelector('[data-profile-form]');
const profileEditToggles = Array.from(
  document.querySelectorAll('[data-action="toggle-profile-edit"]')
);
const profileEditCancelButtons = Array.from(
  document.querySelectorAll('[data-action="cancel-profile-edit"]')
);
const scraperStartAllButton = document.getElementById('scraperStartAll');
const scraperPanels = Array.from(document.querySelectorAll('[data-scraper-panel]'));
const scraperControllers = new Map();
const scraperControllersByProvider = new Map();

scraperPanels.forEach((panel) => {
  const provider = panel.dataset.provider;
  if (!provider) {
    return;
  }
  const id = panel.dataset.scraperId || provider;
  const mode = panel.dataset.scraperMode || 'default';
  const startPageValue = Number(panel.dataset.startPage);
  const startPage = Number.isFinite(startPageValue) ? startPageValue : null;
  const controller = {
    id,
    provider,
    mode,
    startPage,
    panel,
    message: panel.querySelector('[data-role="scraper-message"]'),
    state: panel.querySelector('[data-role="scraper-state"]'),
    progressBar: panel.querySelector('[data-role="scraper-progress-bar"]'),
    progressLabel: panel.querySelector('[data-role="scraper-progress-label"]'),
    updated: panel.querySelector('[data-role="scraper-updated"]'),
    pages: panel.querySelector('[data-role="scraper-pages"]'),
    links: panel.querySelector('[data-role="scraper-links"]'),
    title: panel.querySelector('[data-role="scraper-title"]'),
    log: panel.querySelector('[data-role="scraper-log"]'),
    startButton: panel.querySelector('[data-action="start-scraper"]'),
    refreshButton: panel.querySelector('[data-action="refresh-scraper"]'),
  };

  scraperControllers.set(id, controller);
  if (!scraperControllersByProvider.has(provider)) {
    scraperControllersByProvider.set(provider, []);
  }
  scraperControllersByProvider.get(provider).push(controller);
});

const scraperLastPageLabels = new Map();
document.querySelectorAll('.scraper-last-page').forEach((element) => {
  const provider = element.dataset.provider;
  if (provider) {
    scraperLastPageLabels.set(provider, element);
  }
});

const scraperNextInputs = new Map();
document.querySelectorAll('[data-scraper-next]').forEach((input) => {
  const provider = input.dataset.provider;
  if (provider) {
    scraperNextInputs.set(provider, input);
  }
});
const allMoviesSortButtons = document.querySelectorAll('[data-sort-option]');
const ALL_MOVIES_PAGE_SIZE = 100;
const ALL_MOVIES_RUNTIME_CHUNK_SIZE = 25;
const HERO_ROTATION_INTERVAL = 9000;
const titleCollator = new Intl.Collator('de', { sensitivity: 'base', numeric: true });

const STORAGE_KEYS = Object.freeze({
  allMoviesSort: 'medusa.allMovies.sortPreferences',
});

let lastRecordedView = { key: null, timestamp: 0 };

let changeSection = () => {};
let currentSectionName = 'start';
let previousSectionName = 'start';
let currentSettings = {
  tmdb_api_key: '',
  scrapers: {},
};
let currentTrailer = null;
let currentStreamingLinks = [];
let currentMovieTitle = '';
let currentMovieId = null;
let currentContentType = 'movie';
let currentSeriesData = null;
let currentSeriesEpisodes = new Map();
let currentSeriesSelection = { season: null, episode: null };
let allMovies = [];
let allMoviesSorted = [];
let allMoviesPage = 1;
let allMoviesLoaded = false;
let allMoviesLoading = false;
let allMoviesSorting = false;
let allMoviesSort = 'popular';
let allMoviesDirection = 'desc';
let currentScraperStatuses = {};
let scraperStatusTimeout = null;
let searchAbortController = null;
let searchDebounceTimeout = null;
let currentSearchResults = [];
let currentSearchQuery = '';
let searchHighlightedIndex = -1;
let heroSlides = [];
let heroActiveIndex = 0;
let heroRotationTimeout = null;
const allMoviesRuntimeCache = new Map();

function recordContentView(payload) {
  if (!payload || typeof payload !== 'object') {
    return;
  }
  const contentType = String(payload.content_type || payload.type || '').toLowerCase();
  const objectId = Number(payload.object_id ?? payload.id);
  if (!contentType || !Number.isFinite(objectId) || objectId <= 0) {
    return;
  }
  const key = `${contentType}:${objectId}`;
  const now = Date.now();
  if (lastRecordedView.key === key && now - lastRecordedView.timestamp < 3000) {
    return;
  }
  lastRecordedView = { key, timestamp: now };
  try {
    fetch('/api/views', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content_type: contentType, object_id: objectId }),
      keepalive: true,
    }).catch(() => {
      /* Ignorieren: Aufzeichnung ist ein optionaler Komfort. */
    });
  } catch (error) {
    console.warn('Konnte Aufruf nicht speichern.', error);
  }
}

detailSeasonSelect?.addEventListener('change', () => {
  const seasonNumber = Number(detailSeasonSelect.value);
  if (!Number.isFinite(seasonNumber)) {
    return;
  }
  renderSeriesEpisodeList(seasonNumber);
  const episodes = currentSeriesEpisodes.get(seasonNumber) || [];
  if (!episodes.length) {
    renderStreamingLinks([]);
    return;
  }
  const preferred =
    episodes.find(
      (episode) => Array.isArray(episode?.streaming_links) && episode.streaming_links.some((link) => link && link.url)
    ) || episodes[0];
  if (preferred) {
    selectSeriesEpisode(seasonNumber, preferred.episode_number);
  }
});

function loadAllMoviesSortPreference() {
  if (typeof window === 'undefined' || !window.localStorage) {
    return null;
  }
  try {
    const rawValue = window.localStorage.getItem(STORAGE_KEYS.allMoviesSort);
    if (!rawValue) {
      return null;
    }
    const parsed = JSON.parse(rawValue);
    if (parsed && typeof parsed.sort === 'string' && typeof parsed.direction === 'string') {
      return parsed;
    }
  } catch (error) {
    console.warn('Konnte Sortierpräferenz nicht laden.', error);
  }
  return null;
}

function saveAllMoviesSortPreference(sort, direction) {
  if (typeof window === 'undefined' || !window.localStorage) {
    return;
  }
  try {
    const payload = JSON.stringify({ sort, direction });
    window.localStorage.setItem(STORAGE_KEYS.allMoviesSort, payload);
  } catch (error) {
    console.warn('Konnte Sortierpräferenz nicht speichern.', error);
  }
}

const SANDBOX_ATTRIBUTE_VALUE =
  'allow-same-origin allow-scripts allow-forms allow-pointer-lock allow-fullscreen';

function getStreamLabel(link, index) {
  if (!link) {
    return `Stream ${index + 1}`;
  }
  const parts = [];
  if (link.source_name) {
    parts.push(link.source_name);
  }
  if (link.mirror_info && link.mirror_info !== link.source_name) {
    parts.push(link.mirror_info);
  }
  if (!parts.length) {
    parts.push(`Stream ${index + 1}`);
  }
  return parts.join(' · ');
}

function getStreamMeta(link) {
  if (!link) {
    return 'Keine weiteren Informationen verfügbar.';
  }
  const parts = [];
  if (link.mirror_info && link.mirror_info !== link.source_name) {
    parts.push(link.mirror_info);
  }
  if (link.additional_info) {
    parts.push(link.additional_info);
  }
  if (link.quality) {
    parts.push(link.quality);
  }
  try {
    const url = new URL(link.url);
    const host = url.hostname.replace(/^www\./, '');
    if (host && !parts.includes(host)) {
      parts.push(host);
    }
  } catch (_) {
    /* ignore invalid urls */
  }
  return parts.length ? parts.join(' · ') : 'Direkte Quelle';
}

function shouldUseSandboxForLink(link) {
  if (!link) {
    return false;
  }
  const providerName =
    (typeof link.provider === 'string' && link.provider.trim()) ||
    (typeof link.source_name === 'string' && link.source_name.trim()) ||
    '';
  return providerName.toLowerCase() === 'kinox';
}

function applyIframeSandbox(frame, link) {
  if (!frame) {
    return;
  }
  if (shouldUseSandboxForLink(link)) {
    frame.setAttribute('sandbox', SANDBOX_ATTRIBUTE_VALUE);
  } else {
    frame.removeAttribute('sandbox');
  }
}

function applyWatchButtonLink(link, index = 0) {
  if (!watchButton) {
    return;
  }
  if (!link) {
    watchButton.disabled = true;
    watchButton.removeAttribute('data-stream-url');
    watchButton.removeAttribute('data-stream-name');
    if (watchButtonLabel) {
      watchButtonLabel.textContent = watchButtonDefaultLabel;
    }
    return;
  }

  watchButton.disabled = false;
  watchButton.dataset.streamUrl = link.url;
  watchButton.dataset.streamName = link.source_name || '';
  if (watchButtonLabel) {
    const labelText = getStreamLabel(link, index);
    const streamLabel = labelText ? `Stream öffnen (${labelText})` : 'Stream öffnen';
    watchButtonLabel.textContent = streamLabel;
  }
}

function renderStreamingLinks(streamingLinks, { onSelect } = {}) {
  currentStreamingLinks = Array.isArray(streamingLinks)
    ? streamingLinks.filter((link) => link && link.url)
    : [];

  if (detailStreaming) {
    detailStreaming.innerHTML = '';
  }

  if (!detailStreaming) {
    applyWatchButtonLink(null);
    return;
  }

  if (!currentStreamingLinks.length) {
    const empty = document.createElement('div');
    empty.classList.add('stream-area__empty');

    const emptyTitle = document.createElement('strong');
    emptyTitle.textContent = 'Keine Streams verfügbar.';

    const emptyText = document.createElement('span');
    emptyText.textContent = 'Sobald neue Quellen gefunden werden, erscheinen sie automatisch hier.';

    empty.appendChild(emptyTitle);
    empty.appendChild(emptyText);
    detailStreaming.appendChild(empty);
    applyWatchButtonLink(null);
    return;
  }

  const area = document.createElement('div');
  area.classList.add('stream-area');

  const primary = document.createElement('div');
  primary.classList.add('stream-area__primary');

  const preview = document.createElement('article');
  preview.classList.add('stream-preview');

  const previewHead = document.createElement('header');
  previewHead.classList.add('stream-preview__head');

  const previewBadge = document.createElement('span');
  previewBadge.classList.add('stream-preview__badge');
  previewBadge.textContent = 'Aktiver Stream';

  const previewTitle = document.createElement('h4');
  previewTitle.classList.add('stream-preview__title');

  const previewMeta = document.createElement('p');
  previewMeta.classList.add('stream-preview__meta');

  previewHead.appendChild(previewBadge);
  previewHead.appendChild(previewTitle);
  previewHead.appendChild(previewMeta);

  const frameWrapper = document.createElement('div');
  frameWrapper.classList.add('stream-preview__frame');

  const frame = document.createElement('iframe');
  frame.loading = 'lazy';
  frame.allowFullscreen = true;
  frame.referrerPolicy = 'no-referrer';
  frame.setAttribute(
    'allow',
    'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; fullscreen',
  );

  frameWrapper.appendChild(frame);

  const previewFooter = document.createElement('div');
  previewFooter.classList.add('stream-preview__footer');

  const fallback = document.createElement('a');
  fallback.classList.add('stream-preview__external');
  fallback.target = '_blank';
  fallback.rel = 'noopener noreferrer';
  fallback.textContent = 'In neuem Tab öffnen';

  const previewNote = document.createElement('p');
  previewNote.classList.add('stream-preview__note');
  previewNote.textContent = 'Bei Problemen kannst du den Stream auch direkt im Browser starten.';

  previewFooter.appendChild(fallback);
  previewFooter.appendChild(previewNote);

  preview.appendChild(previewHead);
  preview.appendChild(frameWrapper);
  preview.appendChild(previewFooter);

  primary.appendChild(preview);

  const sidebar = document.createElement('aside');
  sidebar.classList.add('stream-area__sidebar');

  const sources = document.createElement('div');
  sources.classList.add('stream-sources');

  const sourcesHead = document.createElement('header');
  sourcesHead.classList.add('stream-sources__head');

  const sourcesEyebrow = document.createElement('span');
  sourcesEyebrow.classList.add('stream-sources__eyebrow');
  sourcesEyebrow.textContent = 'Quellenübersicht';

  const sourcesTitle = document.createElement('h4');
  sourcesTitle.classList.add('stream-sources__title');
  sourcesTitle.textContent = 'Verfügbare Mirrors';

  const sourcesSubtitle = document.createElement('p');
  sourcesSubtitle.classList.add('stream-sources__subtitle');
  sourcesSubtitle.textContent = 'Wähle eine Quelle für den Player oder öffne sie direkt.';

  sourcesHead.appendChild(sourcesEyebrow);
  sourcesHead.appendChild(sourcesTitle);
  sourcesHead.appendChild(sourcesSubtitle);

  const list = document.createElement('ul');
  list.classList.add('stream-sources__list');

  const sourceButtons = [];

  const updateSelection = (index) => {
    const link = currentStreamingLinks[index];
    sourceButtons.forEach((btn, btnIndex) => {
      const isActive = btnIndex === index && Boolean(link);
      btn.classList.toggle('is-active', isActive);
      btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });

    if (!link) {
      previewTitle.textContent = 'Kein Stream verfügbar';
      previewMeta.textContent = 'Bitte wähle eine andere Quelle aus.';
      frame.removeAttribute('src');
      frame.removeAttribute('title');
      fallback.removeAttribute('href');
      fallback.setAttribute('aria-disabled', 'true');
      fallback.setAttribute('tabindex', '-1');
      applyIframeSandbox(frame, null);
      applyWatchButtonLink(null);
      if (typeof onSelect === 'function') {
        onSelect(null, index);
      }
      return;
    }

    const labelText = getStreamLabel(link, index);
    previewTitle.textContent = labelText;
    previewMeta.textContent = getStreamMeta(link);
    if (frame.src !== link.url) {
      frame.src = link.url;
    }
    frame.title = `${labelText} – Stream Player`;
    applyIframeSandbox(frame, link);
    fallback.href = link.url;
    fallback.removeAttribute('aria-disabled');
    fallback.removeAttribute('tabindex');
    fallback.setAttribute('aria-label', `Stream ${labelText} in neuem Tab öffnen`);
    applyWatchButtonLink(link, index);
    if (typeof onSelect === 'function') {
      onSelect(link, index);
    }
  };

  currentStreamingLinks.forEach((link, index) => {
    const item = document.createElement('li');

    const button = document.createElement('button');
    button.type = 'button';
    button.classList.add('stream-source');
    button.setAttribute('aria-pressed', 'false');
    button.dataset.streamIndex = String(index);

    const name = document.createElement('span');
    name.classList.add('stream-source__name');
    name.textContent = link?.source_name || `Stream ${index + 1}`;

    const meta = document.createElement('span');
    meta.classList.add('stream-source__meta');
    meta.textContent = getStreamMeta(link);

    const cta = document.createElement('span');
    cta.classList.add('stream-source__cta');
    cta.textContent = 'Zum Player';

    button.appendChild(name);
    button.appendChild(meta);
    button.appendChild(cta);

    button.addEventListener('click', () => {
      updateSelection(index);
    });

    item.appendChild(button);
    list.appendChild(item);
    sourceButtons.push(button);
  });

  sources.appendChild(sourcesHead);
  sources.appendChild(list);
  sidebar.appendChild(sources);

  area.appendChild(primary);
  area.appendChild(sidebar);

  detailStreaming.appendChild(area);

  updateSelection(0);
}

function resetSeriesDetail() {
  currentSeriesData = null;
  currentSeriesEpisodes = new Map();
  currentSeriesSelection = { season: null, episode: null };
  if (detailEpisodesSection) {
    detailEpisodesSection.hidden = true;
  }
  if (detailSeasonSelect) {
    detailSeasonSelect.innerHTML = '';
  }
  if (detailEpisodeList) {
    detailEpisodeList.innerHTML = '';
  }
  if (detailEpisodeTitle) {
    detailEpisodeTitle.textContent = '';
  }
  if (detailEpisodeMeta) {
    detailEpisodeMeta.textContent = '';
  }
  if (detailEpisodeOverview) {
    detailEpisodeOverview.textContent = '';
  }
}

function getSeriesEpisode(seasonNumber, episodeNumber) {
  const episodes = currentSeriesEpisodes.get(seasonNumber) || [];
  return episodes.find((episode) => Number(episode?.episode_number) === Number(episodeNumber));
}

function updateEpisodeSelectionHighlight() {
  if (!detailEpisodeList) {
    return;
  }
  const { season, episode } = currentSeriesSelection;
  detailEpisodeList.querySelectorAll('[data-season][data-episode]').forEach((button) => {
    const matches =
      Number(button.dataset.season) === Number(season) &&
      Number(button.dataset.episode) === Number(episode);
    button.classList.toggle('is-active', matches);
  });
}

function renderSeriesEpisodeList(seasonNumber) {
  if (!detailEpisodeList) {
    return;
  }

  detailEpisodeList.innerHTML = '';
  const episodes = currentSeriesEpisodes.get(seasonNumber) || [];

  if (!episodes.length) {
    const empty = document.createElement('li');
    empty.classList.add('empty');
    empty.textContent = 'Keine Episoden für diese Staffel verfügbar.';
    detailEpisodeList.appendChild(empty);
    return;
  }

  episodes.forEach((episode) => {
    if (!episode) {
      return;
    }
    const episodeNumber = Number(episode.episode_number);
    if (!Number.isFinite(episodeNumber)) {
      return;
    }

    const li = document.createElement('li');
    const button = document.createElement('button');
    button.type = 'button';
    button.classList.add('episode-item');
    button.dataset.season = String(seasonNumber);
    button.dataset.episode = String(episodeNumber);

    const name = document.createElement('span');
    name.classList.add('episode-item__title');
    const formattedNumber = `S${String(seasonNumber).padStart(2, '0')}E${String(episodeNumber).padStart(2, '0')}`;
    name.textContent = episode.name ? `${formattedNumber} · ${episode.name}` : formattedNumber;

    const streamCount = Array.isArray(episode.streaming_links)
      ? episode.streaming_links.filter((link) => link && link.url).length
      : 0;
    const meta = document.createElement('span');
    meta.classList.add('episode-item__meta');
    meta.textContent = streamCount
      ? `${streamCount} Stream${streamCount === 1 ? '' : 's'}`
      : 'Keine Streams';

    button.appendChild(name);
    button.appendChild(meta);

    button.addEventListener('click', () => {
      selectSeriesEpisode(seasonNumber, episodeNumber, { focusList: false });
    });

    li.appendChild(button);
    detailEpisodeList.appendChild(li);
  });

  updateEpisodeSelectionHighlight();
}

function selectSeriesEpisode(seasonNumber, episodeNumber, { focusList = false } = {}) {
  const seasonValue = Number(seasonNumber);
  const episodeValue = Number(episodeNumber);
  if (!Number.isFinite(seasonValue) || !Number.isFinite(episodeValue)) {
    return;
  }

  if (detailSeasonSelect && detailSeasonSelect.value !== String(seasonValue)) {
    detailSeasonSelect.value = String(seasonValue);
  }

  const episode = getSeriesEpisode(seasonValue, episodeValue);
  currentSeriesSelection = { season: seasonValue, episode: episodeValue };
  updateEpisodeSelectionHighlight();

  if (focusList && episode && Number.isFinite(Number(episode.id))) {
    recordContentView({ content_type: 'episode', object_id: Number(episode.id) });
  }

  if (focusList && detailEpisodeList) {
    const activeButton = detailEpisodeList.querySelector(
      `[data-season="${seasonValue}"][data-episode="${episodeValue}"]`
    );
    if (activeButton) {
      activeButton.focus();
    }
  }

  const formattedNumber = `S${String(seasonValue).padStart(2, '0')}E${String(episodeValue).padStart(2, '0')}`;
  if (detailEpisodeTitle) {
    detailEpisodeTitle.textContent = episode?.name ? `${formattedNumber} · ${episode.name}` : formattedNumber;
  }
  if (detailEpisodeMeta) {
    const date = episode?.air_date ? formatDate(episode.air_date) : '';
    const runtime = Number(episode?.runtime);
    const parts = [];
    if (date) {
      parts.push(date);
    }
    if (Number.isFinite(runtime) && runtime > 0) {
      parts.push(`${runtime} Min.`);
    }
    detailEpisodeMeta.textContent = parts.length ? parts.join(' · ') : 'Keine Episodendetails verfügbar';
  }
  if (detailEpisodeOverview) {
    detailEpisodeOverview.textContent = episode?.overview || 'Keine Beschreibung verfügbar.';
  }

  const links = Array.isArray(episode?.streaming_links) ? episode.streaming_links : [];
  const seriesBaseTitle = currentSeriesData?.title || currentSeriesData?.name || 'Unbekannter Titel';
  const baseTitle = `${seriesBaseTitle} – ${formattedNumber}`;
  currentMovieTitle = baseTitle;
  renderStreamingLinks(links, {
    onSelect: (link, index) => {
      if (link) {
        currentMovieTitle = `${baseTitle} – ${getStreamLabel(link, index)}`;
      } else {
        currentMovieTitle = baseTitle;
      }
    },
  });
}

function setupSeriesEpisodes(series) {
  resetSeriesDetail();
  if (!series || !Array.isArray(series.seasons) || !series.seasons.length) {
    renderStreamingLinks([]);
    return;
  }

  currentSeriesData = series;
  if (detailEpisodesSection) {
    detailEpisodesSection.hidden = false;
  }

  const selectableSeasons = series.seasons.filter(
    (season) => season && Number.isFinite(Number(season.season_number))
  );

  selectableSeasons.sort((a, b) => Number(a.season_number) - Number(b.season_number));

  selectableSeasons.forEach((season) => {
    const seasonNumber = Number(season.season_number);
    if (!Number.isFinite(seasonNumber)) {
      return;
    }
    const episodes = Array.isArray(season.episodes) ? season.episodes : [];
    currentSeriesEpisodes.set(seasonNumber, episodes);

    if (detailSeasonSelect) {
      const option = document.createElement('option');
      option.value = String(seasonNumber);
      option.textContent = season.name || `Staffel ${seasonNumber}`;
      option.dataset.episodeCount = String(season.episode_count || episodes.length || 0);
      detailSeasonSelect.appendChild(option);
    }
  });

  if (detailSeasonSelect) {
    detailSeasonSelect.disabled = detailSeasonSelect.options.length <= 1;
  }

  if (detailSeasonSelect && detailSeasonSelect.options.length) {
    detailSeasonSelect.value = detailSeasonSelect.options[0].value;
    renderSeriesEpisodeList(Number(detailSeasonSelect.value));
  }
}

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
    if (error?.name === 'AbortError') {
      throw error;
    }
    console.error(error);
    showToast(error.message, 'error');
    throw error;
  }
}

function resetSearchHighlight() {
  searchHighlightedIndex = -1;
  if (topbarSearchInput) {
    topbarSearchInput.removeAttribute('aria-activedescendant');
  }
  if (!topbarSearchList) {
    return;
  }
  topbarSearchList.querySelectorAll('.topbar-search__result').forEach((button) => {
    button.classList.remove('is-active');
  });
}

function clearSearchResults() {
  if (topbarSearchList) {
    topbarSearchList.innerHTML = '';
  }
  currentSearchResults = [];
  resetSearchHighlight();
}

function setSearchResultsVisible(visible) {
  if (!topbarSearchResults || !topbarSearchInput) {
    return;
  }
  if (visible) {
    topbarSearchResults.hidden = false;
    topbarSearchResults.setAttribute('aria-hidden', 'false');
    topbarSearchInput.setAttribute('aria-expanded', 'true');
  } else {
    topbarSearchResults.hidden = true;
    topbarSearchResults.setAttribute('aria-hidden', 'true');
    topbarSearchInput.setAttribute('aria-expanded', 'false');
  }
}

function hideSearchResults({ clear = false } = {}) {
  setSearchResultsVisible(false);
  if (clear) {
    clearSearchResults();
  } else {
    resetSearchHighlight();
  }
}

function renderSearchMessage(message) {
  if (!topbarSearchList) {
    return;
  }
  topbarSearchList.innerHTML = '';
  const item = document.createElement('li');
  item.className = 'topbar-search__item topbar-search__item--empty';
  item.textContent = message;
  topbarSearchList.appendChild(item);
  currentSearchResults = [];
  resetSearchHighlight();
}

function applySearchHighlight(index) {
  if (!topbarSearchList) {
    return;
  }
  const buttons = Array.from(topbarSearchList.querySelectorAll('.topbar-search__result'));
  if (!buttons.length) {
    resetSearchHighlight();
    return;
  }
  if (index == null || index < 0) {
    buttons.forEach((button) => button.classList.remove('is-active'));
    resetSearchHighlight();
    return;
  }
  const targetIndex = Math.max(0, Math.min(index, buttons.length - 1));
  buttons.forEach((button, buttonIndex) => {
    if (buttonIndex === targetIndex) {
      button.classList.add('is-active');
      if (!button.id) {
        button.id = `search-result-${buttonIndex}`;
      }
      if (topbarSearchInput) {
        topbarSearchInput.setAttribute('aria-activedescendant', button.id);
      }
      button.scrollIntoView({ block: 'nearest' });
    } else {
      button.classList.remove('is-active');
    }
  });
  searchHighlightedIndex = targetIndex;
}

function renderSearchResults(results) {
  if (!topbarSearchList) {
    return;
  }
  topbarSearchList.innerHTML = '';
  currentSearchResults = Array.isArray(results) ? results : [];
  resetSearchHighlight();

  if (!currentSearchResults.length) {
    renderSearchMessage('Keine Ergebnisse.');
    return;
  }

  currentSearchResults.forEach((movie, index) => {
    const item = document.createElement('li');
    item.className = 'topbar-search__item';

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'topbar-search__result';
    button.dataset.index = String(index);
    button.dataset.movieId = movie.id != null ? String(movie.id) : '';

    const thumb = document.createElement('div');
    thumb.className = 'topbar-search__thumb';
    const posterFallback =
      detailPoster?.dataset?.placeholder ||
      topbarSearchResults?.dataset?.posterPlaceholder ||
      '';
    const posterUrl = getMoviePosterUrl(movie) || posterFallback;
    if (posterUrl) {
      thumb.style.backgroundImage = `url('${posterUrl}')`;
    }
    button.appendChild(thumb);

    const body = document.createElement('div');
    body.className = 'topbar-search__body';

    const title = document.createElement('div');
    title.className = 'topbar-search__title';
    title.textContent = movie.title || 'Unbekannter Titel';
    body.appendChild(title);

    const meta = document.createElement('div');
    meta.className = 'topbar-search__meta';
    const releaseYear = typeof movie.release_date === 'string' && movie.release_date ? movie.release_date.slice(0, 4) : '';
    if (releaseYear) {
      const year = document.createElement('span');
      year.textContent = releaseYear;
      meta.appendChild(year);
    }
    const ratingValue = Number(movie.rating);
    if (Number.isFinite(ratingValue) && ratingValue > 0) {
      const rating = document.createElement('span');
      rating.textContent = `⭐ ${ratingValue.toFixed(1)}`;
      meta.appendChild(rating);
    }
    if (typeof movie.streams === 'number' && movie.streams > 0) {
      const streams = document.createElement('span');
      streams.textContent = movie.streams === 1 ? '1 Stream' : `${movie.streams} Streams`;
      meta.appendChild(streams);
    }
    if (meta.childElementCount) {
      body.appendChild(meta);
    }

    const overview = document.createElement('div');
    overview.className = 'topbar-search__overview';
    const overviewText = typeof movie.overview === 'string' && movie.overview.trim() ? movie.overview.trim() : 'Keine Beschreibung verfügbar.';
    overview.textContent = overviewText.length > 140 ? `${overviewText.slice(0, 137)}…` : overviewText;
    body.appendChild(overview);

    button.appendChild(body);

    button.addEventListener('click', () => {
      const resultIndex = Number(button.dataset.index);
      openSearchResult(Number.isFinite(resultIndex) ? resultIndex : index);
    });
    button.addEventListener('mouseenter', () => {
      applySearchHighlight(index);
    });

    item.appendChild(button);
    topbarSearchList.appendChild(item);
  });
}

function renderSearchOverlayResults(results) {
  if (!searchOverlayList) {
    return;
  }
  searchOverlayList.innerHTML = '';
  if (!Array.isArray(results) || !results.length) {
    searchOverlayList.setAttribute('hidden', 'true');
    return;
  }
  searchOverlayList.removeAttribute('hidden');

  results.forEach((movie) => {
    if (!movie || movie.id == null) {
      return;
    }
    const card = document.createElement('article');
    card.className = 'media-card media-card--grid';
    card.dataset.movieId = String(movie.id);
    card.setAttribute('role', 'button');
    card.tabIndex = 0;

    const poster = document.createElement('div');
    poster.className = 'media-card__poster';
    const posterUrl = getMoviePosterUrl(movie);
    if (posterUrl) {
      poster.style.backgroundImage = `url('${posterUrl}')`;
    }
    card.appendChild(poster);

    const overlay = document.createElement('div');
    overlay.className = 'media-card__overlay';

    const title = document.createElement('h3');
    title.className = 'media-card__title';
    title.textContent = movie.title || 'Unbekannter Titel';
    overlay.appendChild(title);

    const meta = document.createElement('p');
    meta.className = 'media-card__meta';
    const releaseYear =
      typeof movie.release_date === 'string' && movie.release_date ? movie.release_date.slice(0, 4) : '';
    meta.textContent = releaseYear || 'Keine Angaben';
    overlay.appendChild(meta);

    const ratingValue = Number(movie.rating);
    if (Number.isFinite(ratingValue) && ratingValue > 0) {
      const rating = document.createElement('span');
      rating.className = 'media-card__rating';
      rating.textContent = `⭐ ${ratingValue.toFixed(1)}`;
      overlay.appendChild(rating);
    }

    if (typeof movie.streams === 'number' && movie.streams > 0) {
      const badge = document.createElement('span');
      badge.className = 'media-card__badge';
      badge.textContent = movie.streams === 1 ? '1 Stream' : `${movie.streams} Streams`;
      overlay.appendChild(badge);
    }

    card.appendChild(overlay);
    searchOverlayList.appendChild(card);
  });

  bindContentCards();
}

function isSearchOverlayActive() {
  return Boolean(searchOverlay?.classList.contains('is-active'));
}

function hideSearchOverlay() {
  if (!searchOverlay) {
    return;
  }
  searchOverlay.classList.remove('is-active');
  searchOverlay.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('showing-search');
  if (searchOverlayList) {
    searchOverlayList.innerHTML = '';
    searchOverlayList.setAttribute('hidden', 'true');
  }
  if (searchOverlayEmpty) {
    searchOverlayEmpty.hidden = true;
  }
  if (searchOverlayMeta) {
    searchOverlayMeta.textContent = '';
  }
}

function showSearchOverlay({ query = '', results = [], pending = false, message = '', focusClose = false } = {}) {
  if (!searchOverlay) {
    return;
  }

  const trimmedQuery = typeof query === 'string' ? query.trim() : '';
  if (searchOverlayTitle) {
    searchOverlayTitle.textContent = trimmedQuery ? `Ergebnisse für „${trimmedQuery}“` : 'Suchergebnisse';
  }

  let metaText = '';
  let emptyText = '';

  if (pending) {
    metaText = 'Suche läuft…';
    emptyText = 'Suche läuft…';
  } else if (message) {
    metaText = message;
    emptyText = message;
  } else if (!results.length) {
    metaText = 'Keine Ergebnisse gefunden';
    emptyText = trimmedQuery ? `Keine Ergebnisse für „${trimmedQuery}“.` : 'Keine Ergebnisse gefunden.';
  } else {
    metaText = results.length === 1 ? '1 Ergebnis gefunden' : `${results.length} Ergebnisse gefunden`;
  }

  if (searchOverlayMeta) {
    searchOverlayMeta.textContent = metaText;
  }

  if (searchOverlayEmpty) {
    if (emptyText) {
      searchOverlayEmpty.textContent = emptyText;
      searchOverlayEmpty.hidden = false;
    } else {
      searchOverlayEmpty.hidden = true;
    }
  }

  if (!pending && !message && results.length) {
    renderSearchOverlayResults(results);
  } else if (searchOverlayList) {
    searchOverlayList.innerHTML = '';
    searchOverlayList.setAttribute('hidden', 'true');
  }

  searchOverlay.classList.add('is-active');
  searchOverlay.setAttribute('aria-hidden', 'false');
  document.body.classList.add('showing-search');
  hideSearchResults();
  if (focusClose && searchOverlayClose) {
    window.setTimeout(() => {
      searchOverlayClose.focus();
    }, 0);
  }
}

function moveSearchHighlight(offset) {
  if (!currentSearchResults.length) {
    return;
  }
  let nextIndex = searchHighlightedIndex;
  if (nextIndex < 0) {
    nextIndex = offset > 0 ? 0 : currentSearchResults.length - 1;
  } else {
    nextIndex = (nextIndex + offset + currentSearchResults.length) % currentSearchResults.length;
  }
  applySearchHighlight(nextIndex);
}

function openSearchResult(index) {
  if (!currentSearchResults.length) {
    return;
  }
  const boundedIndex = Math.max(0, Math.min(index, currentSearchResults.length - 1));
  const movie = currentSearchResults[boundedIndex];
  if (!movie || movie.id == null) {
    return;
  }
  hideSearchResults({ clear: true });
  if (topbarSearchInput) {
    topbarSearchInput.value = '';
    topbarSearchInput.blur();
  }
  hideSearchOverlay();
  openMovieDetail(movie.id);
}

async function performSearch(query, { showOverlay = false } = {}) {
  const trimmed = query.trim();
  if (trimmed.length < 2) {
    return;
  }

  currentSearchQuery = trimmed;

  if (searchAbortController) {
    searchAbortController.abort();
  }
  const controller = new AbortController();
  searchAbortController = controller;

  try {
    const response = await callApi(`/api/search?q=${encodeURIComponent(trimmed)}`, {
      signal: controller.signal,
    });
    if (controller.signal.aborted) {
      return;
    }
    if (!response?.success) {
      renderSearchMessage('Suche fehlgeschlagen.');
      setSearchResultsVisible(true);
      if (showOverlay) {
        showSearchOverlay({ query: trimmed, message: 'Suche fehlgeschlagen.' });
      }
      return;
    }
    const results = Array.isArray(response.results) ? response.results : [];
    if (!results.length) {
      renderSearchMessage(`Keine Ergebnisse für „${trimmed}“.`);
      setSearchResultsVisible(true);
      if (showOverlay) {
        showSearchOverlay({ query: trimmed, results: [], message: `Keine Ergebnisse für „${trimmed}“.`, focusClose: false });
      }
      return;
    }
    renderSearchResults(results);
    setSearchResultsVisible(true);
    if (showOverlay) {
      showSearchOverlay({ query: trimmed, results, focusClose: false });
    }
  } catch (error) {
    if (error?.name === 'AbortError') {
      return;
    }
    renderSearchMessage('Suche fehlgeschlagen.');
    setSearchResultsVisible(true);
    if (showOverlay) {
      showSearchOverlay({ query: trimmed, message: 'Suche fehlgeschlagen.' });
    }
  } finally {
    if (searchAbortController === controller) {
      searchAbortController = null;
    }
  }
}

function handleSearchInput(event) {
  if (!topbarSearchInput) {
    return;
  }
  const value = event.target.value || '';

  if (isSearchOverlayActive()) {
    hideSearchOverlay();
  }

  if (searchAbortController) {
    searchAbortController.abort();
    searchAbortController = null;
  }
  if (searchDebounceTimeout) {
    clearTimeout(searchDebounceTimeout);
    searchDebounceTimeout = null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    hideSearchResults({ clear: true });
    hideSearchOverlay();
    return;
  }
  if (trimmed.length < 2) {
    renderSearchMessage('Bitte mindestens 2 Zeichen eingeben.');
    setSearchResultsVisible(true);
    hideSearchOverlay();
    return;
  }

  renderSearchMessage('Suche läuft…');
  setSearchResultsVisible(true);
  searchDebounceTimeout = window.setTimeout(() => {
    searchDebounceTimeout = null;
    performSearch(value);
  }, 250);
}

function handleSearchSubmit(event) {
  if (!topbarSearchInput) {
    return;
  }
  event.preventDefault();
  const query = topbarSearchInput.value || '';
  const trimmed = query.trim();

  if (!trimmed) {
    hideSearchOverlay();
    hideSearchResults({ clear: true });
    return;
  }

  if (trimmed.length < 2) {
    renderSearchMessage('Bitte mindestens 2 Zeichen eingeben.');
    setSearchResultsVisible(true);
    hideSearchOverlay();
    return;
  }

  const sameQuery =
    typeof currentSearchQuery === 'string' && currentSearchQuery.toLowerCase() === trimmed.toLowerCase();

  if (currentSearchResults.length && sameQuery) {
    showSearchOverlay({ query: trimmed, results: currentSearchResults, focusClose: true });
    return;
  }

  clearSearchResults();
  showSearchOverlay({ query: trimmed, pending: true, focusClose: true });
  performSearch(query, { showOverlay: true });
}

function handleSearchKeydown(event) {
  if (!topbarSearchInput) {
    return;
  }
  if (event.key === 'ArrowDown') {
    if (currentSearchResults.length) {
      event.preventDefault();
      setSearchResultsVisible(true);
      moveSearchHighlight(1);
    }
  } else if (event.key === 'ArrowUp') {
    if (currentSearchResults.length) {
      event.preventDefault();
      setSearchResultsVisible(true);
      moveSearchHighlight(-1);
    }
  } else if (event.key === 'Escape') {
    hideSearchOverlay();
    if (topbarSearchInput.value) {
      event.preventDefault();
      hideSearchResults({ clear: true });
      topbarSearchInput.value = '';
    } else {
      hideSearchResults();
    }
  }
}

function initSearch() {
  if (!topbarSearchForm || !topbarSearchInput) {
    return;
  }

  if (topbarSearchResults) {
    topbarSearchResults.hidden = true;
    topbarSearchResults.setAttribute('aria-hidden', 'true');
  }

  topbarSearchInput.setAttribute('role', 'combobox');
  topbarSearchInput.setAttribute('aria-autocomplete', 'list');
  if (topbarSearchList?.id) {
    topbarSearchInput.setAttribute('aria-controls', topbarSearchList.id);
  }
  topbarSearchInput.setAttribute('aria-expanded', 'false');

  topbarSearchInput.addEventListener('input', handleSearchInput);
  topbarSearchInput.addEventListener('keydown', handleSearchKeydown);
  topbarSearchForm.addEventListener('submit', handleSearchSubmit);

  topbarSearchInput.addEventListener('focus', () => {
    if (currentSearchResults.length) {
      setSearchResultsVisible(true);
    }
  });

  topbarSearchInput.addEventListener('blur', () => {
    window.setTimeout(() => {
      if (!topbarSearchForm.contains(document.activeElement)) {
        hideSearchResults();
      }
    }, 120);
  });

  document.addEventListener('click', (event) => {
    if (!topbarSearchForm.contains(event.target)) {
      hideSearchResults();
    }
  });
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

function collectScraperStartButtons() {
  return Array.from(scraperControllers.values())
    .map((controller) => controller.startButton)
    .filter(Boolean);
}

function setButtonsDisabled(buttons, disabled) {
  buttons.forEach((button) => {
    if (button) {
      button.disabled = disabled;
    }
  });
}

function normalizeScraperStatus(raw, provider = '') {
  const base = {
    provider,
    provider_label: '',
    running: false,
    start_page: null,
    current_page: null,
    next_page: null,
    last_page: null,
    processed_pages: 0,
    total_pages: 0,
    processed_links: 0,
    last_title: '',
    message: 'Bereit.',
    error: null,
    content_scope: null,
    started_at: null,
    finished_at: null,
    last_update: null,
    progress: 0,
    progress_mode: 'idle',
    log: [],
  };

  const status = { ...base };
  if (raw && typeof raw === 'object') {
    Object.assign(status, raw);
  }

  status.provider = typeof status.provider === 'string' && status.provider ? status.provider : provider;
  status.provider_label =
    typeof status.provider_label === 'string' && status.provider_label ? status.provider_label : status.provider;

  const numericFields = [
    'start_page',
    'current_page',
    'processed_pages',
    'total_pages',
    'processed_links',
    'progress',
    'next_page',
    'last_page',
  ];
  numericFields.forEach((key) => {
    const value = Number(status[key]);
    status[key] = Number.isFinite(value) ? value : base[key];
  });

  status.running = Boolean(status.running);
  status.log = Array.isArray(status.log) ? status.log.filter(Boolean) : [];
  status.message = typeof status.message === 'string' && status.message.trim() ? status.message : base.message;
  status.last_title = typeof status.last_title === 'string' && status.last_title.trim() ? status.last_title : '';
  status.error = typeof status.error === 'string' && status.error.trim() ? status.error : null;
  status.progress_mode = typeof status.progress_mode === 'string' ? status.progress_mode : base.progress_mode;

  return status;
}

function normalizeScraperStatuses(rawStatuses) {
  const normalized = {};
  scraperControllersByProvider.forEach((_, provider) => {
    normalized[provider] = normalizeScraperStatus(rawStatuses?.[provider], provider);
  });
  Object.keys(rawStatuses || {}).forEach((provider) => {
    if (!normalized[provider]) {
      normalized[provider] = normalizeScraperStatus(rawStatuses[provider], provider);
    }
  });
  return normalized;
}

async function startScraper(controllerId) {
  if (!controllerId) return;
  const controller = scraperControllers.get(controllerId);
  if (!controller) return;

  const provider = controller.provider;
  const providerControllers = scraperControllersByProvider.get(provider) || [];
  const providerButtons = providerControllers.map((item) => item.startButton).filter(Boolean);
  const buttonsToDisable = [topbarScrapeButton, scraperStartAllButton, ...providerButtons].filter(Boolean);
  setButtonsDisabled(buttonsToDisable, true);
  try {
    const heading =
      controller?.panel?.querySelector('h3') || controller?.panel?.querySelector('h2');
    const label = heading?.textContent?.trim() || 'Scraper';
    showToast(`${label} wird gestartet...`);
    const payload = {};
    if (controller.mode === 'series') {
      payload.include_series = true;
      const status = currentScraperStatuses[provider];
      const nextPage = Number(status?.next_page);
      if (!Number.isFinite(nextPage) || nextPage <= 1) {
        payload.start_page = 1;
      }
    } else if (Number.isFinite(controller.startPage)) {
      payload.start_page = controller.startPage;
    }
    const response = await callApi(`/api/scrape/${provider}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (response?.success) {
      const anyRunning = updateScraperStatusUI(response.status);
      if (response.started_any) {
        showToast(response.message || 'Scraper läuft im Hintergrund.', 'success');
      } else if (response.message) {
        showToast(response.message, 'info');
      }
      ensureScraperStatusPolling(anyRunning || response.started_any ? 1500 : undefined);
    }
  } catch (_) {
    // Fehler bereits behandelt durch callApi
  } finally {
    if (topbarScrapeButton) {
      topbarScrapeButton.disabled = false;
    }
    if (scraperStartAllButton) {
      scraperStartAllButton.disabled = false;
    }
    updateStartButtonsDisabled();
  }
}

async function startAllScrapers() {
  const buttonsToDisable = [topbarScrapeButton, scraperStartAllButton, ...collectScraperStartButtons()];
  setButtonsDisabled(buttonsToDisable, true);
  try {
    showToast('Alle Scraper werden gestartet...');
    const response = await callApi('/api/scrape/all', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    if (response?.success) {
      const anyRunning = updateScraperStatusUI(response.status);
      if (response.started_any) {
        showToast(response.message || 'Scraper laufen im Hintergrund.', 'success');
      } else if (response.message) {
        showToast(response.message, 'info');
      }
      ensureScraperStatusPolling(anyRunning || response.started_any ? 1500 : undefined);
    }
  } catch (_) {
    // Fehler bereits behandelt durch callApi
  } finally {
    if (topbarScrapeButton) {
      topbarScrapeButton.disabled = false;
    }
    if (scraperStartAllButton) {
      scraperStartAllButton.disabled = false;
    }
    updateStartButtonsDisabled();
  }
}

function formatScraperLogTime(value) {
  if (!value) return '–';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString('de-DE', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function formatScraperUpdated(value) {
  if (!value) return 'Noch keine Aktivität';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return `Aktualisiert: ${value}`;
  }
  const now = new Date();
  const sameDay = date.toDateString() === now.toDateString();
  const timeString = date.toLocaleTimeString('de-DE', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  if (sameDay) {
    return `Aktualisiert: ${timeString}`;
  }
  const dateString = date.toLocaleDateString('de-DE', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
  });
  return `Aktualisiert: ${dateString} ${timeString}`;
}

function renderScraperStatusLog(controller, entries) {
  if (!controller?.log) return;

  const list = controller.log;
  list.innerHTML = '';

  const safeEntries = Array.isArray(entries) ? entries.filter(Boolean) : [];
  if (!safeEntries.length) {
    const empty = document.createElement('li');
    empty.className = 'scraper-status__log-item is-empty';
    empty.textContent = 'Noch keine Aktivitäten.';
    list.appendChild(empty);
    return;
  }

  const visible = safeEntries.slice(-25).reverse();
  visible.forEach((entry) => {
    const item = document.createElement('li');
    item.className = 'scraper-status__log-item';
    if (entry.level) {
      item.dataset.level = entry.level;
    }

    const time = document.createElement('span');
    time.className = 'scraper-status__log-time';
    time.textContent = formatScraperLogTime(entry.timestamp);

    const text = document.createElement('span');
    text.className = 'scraper-status__log-text';
    text.textContent = entry.message || '';

    item.appendChild(time);
    item.appendChild(text);
    list.appendChild(item);
  });
}

function updateStartButtonsDisabled() {
  scraperControllers.forEach((controller) => {
    if (!controller.startButton) return;
    controller.startButton.disabled = Boolean(currentScraperStatuses[controller.provider]?.running);
  });
}

function updateSingleScraperUI(controller, status) {
  if (!controller) return;

  const progressValue = Math.max(0, Math.min(100, Number(status.progress) || 0));
  const progressMode = status.progress_mode || (status.total_pages > 0 ? 'determinate' : status.running ? 'indeterminate' : 'idle');
  const message = status.message || (status.running ? 'Scraper läuft…' : 'Bereit.');
  const state = status.running ? 'running' : status.error ? 'error' : status.processed_links > 0 ? 'done' : 'idle';

  if (controller.panel) {
    controller.panel.dataset.running = status.running ? 'true' : 'false';
    if (status.content_scope) {
      controller.panel.dataset.contentScope = status.content_scope;
    } else {
      delete controller.panel.dataset.contentScope;
    }
  }
  if (controller.message) {
    controller.message.textContent = message;
  }
  if (controller.state) {
    const stateText = state === 'running' ? 'Läuft' : state === 'error' ? 'Fehler' : state === 'done' ? 'Fertig' : 'Bereit';
    controller.state.textContent = stateText;
    controller.state.dataset.state = state;
  }
  if (controller.progressBar) {
    if (progressMode === 'indeterminate') {
      controller.progressBar.classList.add('is-indeterminate');
      controller.progressBar.style.width = '28%';
    } else {
      controller.progressBar.classList.remove('is-indeterminate');
      controller.progressBar.style.width = `${progressValue.toFixed(1)}%`;
    }
  }
  if (controller.progressLabel) {
    controller.progressLabel.textContent = progressMode === 'indeterminate' && status.running
      ? 'Laufend'
      : `${progressValue.toFixed(1)}%`;
  }
  if (controller.updated) {
    controller.updated.textContent = formatScraperUpdated(status.last_update);
  }
  if (controller.pages) {
    let pagesText = 'Noch nicht gestartet';
    const nextPage = Number.isFinite(status.next_page) ? status.next_page : null;
    const lastPage = Number.isFinite(status.last_page) ? status.last_page : null;
    const currentPage = Number.isFinite(status.current_page) ? status.current_page : null;
    const processed = status.processed_pages || 0;

    if (status.running) {
      if (currentPage) {
        pagesText = `Seite ${currentPage}`;
      } else if (nextPage) {
        pagesText = `Bereit ab Seite ${nextPage}`;
      } else {
        pagesText = 'Scraper aktiv';
      }
      if (nextPage && (!currentPage || nextPage !== currentPage)) {
        pagesText += ` · nächste: ${nextPage}`;
      }
      if (processed > 0) {
        pagesText += ` · ${processed} abgeschlossen`;
      }
    } else if (lastPage) {
      pagesText = `Letzte: ${lastPage}`;
      if (nextPage) {
        pagesText += ` · nächste: ${nextPage}`;
      }
    } else if (nextPage) {
      pagesText = `Bereit ab Seite ${nextPage}`;
    }
    controller.pages.textContent = pagesText;
  }
  if (controller.links) {
    controller.links.textContent = String(status.processed_links || 0);
  }
  if (controller.title) {
    controller.title.textContent = status.last_title || '–';
  }
  if (controller.startButton) {
    controller.startButton.disabled = status.running;
  }

  renderScraperStatusLog(controller, status.log);

  if (!status.provider) {
    return;
  }
  if (!currentSettings.scrapers) {
    currentSettings.scrapers = {};
  }
  const providerSettings = currentSettings.scrapers[status.provider] || {};
  if (Number.isFinite(status.next_page)) {
    providerSettings.next_page = status.next_page;
  }
  if (Number.isFinite(status.last_page)) {
    providerSettings.last_page = status.last_page;
  }
  currentSettings.scrapers[status.provider] = providerSettings;
}

function updateScraperStatusUI(statuses) {
  const normalized = normalizeScraperStatuses(statuses);
  currentScraperStatuses = normalized;

  let anyRunning = false;
  scraperControllersByProvider.forEach((controllers, provider) => {
    const status = normalized[provider] || normalizeScraperStatus(null, provider);
    controllers.forEach((controller) => {
      updateSingleScraperUI(controller, status);
    });
    if (status.running) {
      anyRunning = true;
    }
  });

  refreshScraperSettingsUi();
  updateStartButtonsDisabled();
  return anyRunning;
}

function cancelScraperStatusPoll() {
  if (scraperStatusTimeout) {
    clearTimeout(scraperStatusTimeout);
    scraperStatusTimeout = null;
  }
}

function scheduleScraperStatusPoll(delay = 6000) {
  if (!scraperControllers.size) return;
  cancelScraperStatusPoll();
  scraperStatusTimeout = window.setTimeout(async () => {
    await fetchScraperStatus();
    const nextDelay = Object.values(currentScraperStatuses).some((status) => status?.running) ? 2000 : 8000;
    scheduleScraperStatusPoll(nextDelay);
  }, delay);
}

async function fetchScraperStatus({ manual = false } = {}) {
  if (!scraperControllers.size) return;
  try {
    const response = await fetch('/api/scrape/status', { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`Status konnte nicht geladen werden (${response.status}).`);
    }
    const payload = await response.json();
    if (payload?.success && payload.status) {
      const anyRunning = updateScraperStatusUI(payload.status);
      if (!anyRunning) {
        updateStartButtonsDisabled();
      }
    } else if (manual) {
      showToast('Status konnte nicht geladen werden.', 'error');
    }
  } catch (error) {
    console.error(error);
    if (manual) {
      showToast(error.message || 'Status konnte nicht geladen werden.', 'error');
    }
  }
}

function ensureScraperStatusPolling(initialDelay) {
  if (!scraperControllers.size) return;
  const anyRunning = Object.values(currentScraperStatuses).some((status) => status?.running);
  const delay = typeof initialDelay === 'number' ? initialDelay : anyRunning ? 2000 : 8000;
  scheduleScraperStatusPoll(delay);
}

function initScraperStatus() {
  if (!scraperControllers.size) return;

  const initialStatuses = {};
  scraperControllers.forEach((controller) => {
    try {
      const raw = controller.panel.dataset.initialStatus;
      if (raw && !initialStatuses[controller.provider]) {
        initialStatuses[controller.provider] = JSON.parse(raw);
      }
    } catch (error) {
      console.error(error);
    }
    controller.panel.dataset.initialStatus = '';
  });

  const anyRunning = updateScraperStatusUI(initialStatuses);
  ensureScraperStatusPolling(anyRunning ? 2000 : 8000);
}

document.addEventListener('visibilitychange', () => {
  if (!scraperControllers.size) return;
  if (document.hidden) {
    cancelScraperStatusPoll();
    return;
  }
  fetchScraperStatus().finally(() => ensureScraperStatusPolling());
});

window.addEventListener('beforeunload', () => {
  cancelScraperStatusPoll();
});

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
  const initialSection = document.body?.dataset?.currentPage || 'start';
  currentSectionName = initialSection;
  previousSectionName = initialSection;

  changeSection = (name, { scroll = true } = {}) => {
    if (!detailPanel) {
      currentSectionName = name;
      return;
    }

    if (name === 'detail') {
      detailPanel.classList.add('is-active');
      document.body.classList.add('showing-detail');
      if (scroll) {
        detailPanel.scrollTop = 0;
      }
      currentSectionName = 'detail';
      return;
    }

    detailPanel.classList.remove('is-active');
    document.body.classList.remove('showing-detail');
    if (scroll) {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }
    closeTrailerModal();
    currentSectionName = name;
  };
}

function initHero() {
  if (!heroSection || document.body?.dataset?.currentPage !== 'start') {
    return;
  }

  let slides = [];
  if (heroDataElement?.textContent) {
    try {
      slides = JSON.parse(heroDataElement.textContent.trim());
    } catch (error) {
      console.warn('Konnte Hero-Daten nicht verarbeiten.', error);
    }
  }

  if (!Array.isArray(slides) || slides.length === 0) {
    return;
  }

  heroSlides = slides.filter((item) => item && typeof item === 'object');
  if (!heroSlides.length) {
    heroSlides = slides;
  }

  const heroTitle = heroSection.querySelector('.hero__title');
  const heroDescription = heroSection.querySelector('.hero__description');
  const refreshButton = document.getElementById('refreshHero');

  const ensureIndicators = () => {
    if (!heroIndicatorsContainer) {
      return;
    }
    heroIndicatorsContainer.innerHTML = '';
    heroSlides.forEach((_, index) => {
      const indicator = document.createElement('button');
      indicator.type = 'button';
      indicator.className = 'hero__indicator';
      indicator.dataset.index = String(index);
      indicator.addEventListener('click', () => {
        goToSlide(index);
      });
      heroIndicatorsContainer.appendChild(indicator);
    });
  };

  const updateIndicators = (index) => {
    if (!heroIndicatorsContainer) {
      return;
    }
    const indicators = heroIndicatorsContainer.querySelectorAll('.hero__indicator');
    indicators.forEach((indicator) => {
      const indicatorIndex = Number(indicator.dataset.index);
      const isActive = indicatorIndex === index;
      indicator.classList.toggle('is-active', isActive);
      indicator.disabled = isActive;
    });
  };

  const normalizeOverview = (value) => {
    const text = typeof value === 'string' ? value.trim() : '';
    if (!text) {
      return 'Keine Beschreibung verfügbar.';
    }
    return text.length > 280 ? `${text.slice(0, 277)}…` : text;
  };

  const buildMeta = (movie) => {
    if (!heroMeta) {
      return;
    }
    heroMeta.innerHTML = '';
    const entries = [];
    const ratingValue = Number(movie.rating);
    if (Number.isFinite(ratingValue) && ratingValue > 0) {
      entries.push({ icon: 'star', text: ratingValue.toFixed(1) });
    }
    const releaseYear =
      typeof movie.release_date === 'string' && movie.release_date
        ? movie.release_date.slice(0, 4)
        : '';
    if (releaseYear) {
      entries.push({ icon: 'event', text: releaseYear });
    }

    if (!entries.length) {
      const placeholder = document.createElement('span');
      placeholder.textContent = 'Jetzt entdecken';
      heroMeta.appendChild(placeholder);
      return;
    }

    entries.forEach(({ icon, text }) => {
      const chip = document.createElement('span');
      if (icon) {
        const iconElement = document.createElement('span');
        iconElement.className = 'material-symbols-outlined';
        iconElement.setAttribute('aria-hidden', 'true');
        iconElement.textContent = icon;
        chip.appendChild(iconElement);
      }
      const label = document.createElement('span');
      label.textContent = text;
      chip.appendChild(label);
      heroMeta.appendChild(chip);
    });
  };

  const applySlide = (index) => {
    const slide = heroSlides[index];
    if (!slide) {
      return;
    }
    heroActiveIndex = index;
    heroSection.dataset.heroActive = String(index);

    const backdrop = typeof slide.backdrop === 'string' ? slide.backdrop.trim() : '';
    if (backdrop) {
      heroSection.style.setProperty('--hero-image', `url('${backdrop}')`);
    } else {
      heroSection.style.removeProperty('--hero-image');
    }

    if (heroTitle) {
      heroTitle.textContent = slide.title || 'Unbekannter Titel';
    }
    if (heroDescription) {
      heroDescription.textContent = normalizeOverview(slide.overview);
    }
    if (heroPosterImage) {
      const poster = typeof slide.poster === 'string' ? slide.poster.trim() : '';
      heroPosterImage.style.backgroundImage = poster ? `url('${poster}')` : '';
    }
    if (heroPlayButton) {
      if (slide.id != null) {
        heroPlayButton.dataset.movieId = String(slide.id);
        heroPlayButton.disabled = false;
      } else {
        heroPlayButton.dataset.movieId = '';
        heroPlayButton.disabled = true;
      }
    }
    buildMeta(slide);
    updateIndicators(index);
    scheduleRotation();
  };

  function scheduleRotation() {
    if (heroRotationTimeout) {
      clearTimeout(heroRotationTimeout);
    }
    if (heroSlides.length <= 1) {
      heroRotationTimeout = null;
      return;
    }
    heroRotationTimeout = setTimeout(() => {
      goToSlide(heroActiveIndex + 1);
    }, HERO_ROTATION_INTERVAL);
  }

  function pauseRotation() {
    if (heroRotationTimeout) {
      clearTimeout(heroRotationTimeout);
      heroRotationTimeout = null;
    }
  }

  function goToSlide(targetIndex) {
    if (!heroSlides.length) {
      return;
    }
    const nextIndex = ((targetIndex % heroSlides.length) + heroSlides.length) % heroSlides.length;
    applySlide(nextIndex);
  }

  const showNext = () => goToSlide(heroActiveIndex + 1);
  const showPrevious = () => goToSlide(heroActiveIndex - 1);

  ensureIndicators();
  applySlide(0);

  refreshButton?.addEventListener('click', showNext);
  heroNextButton?.addEventListener('click', showNext);
  heroPrevButton?.addEventListener('click', showPrevious);

  heroSection.addEventListener('mouseenter', pauseRotation);
  heroSection.addEventListener('mouseleave', scheduleRotation);
  heroSection.addEventListener('focusin', pauseRotation);
  heroSection.addEventListener('focusout', scheduleRotation);
}

function initMediaRails() {
  const rails = Array.from(document.querySelectorAll('[data-rail]'));
  rails.forEach((rail) => {
    const track = rail.querySelector('[data-rail-track]');
    if (!track) {
      return;
    }

    const prev = rail.querySelector('[data-rail-prev]');
    const next = rail.querySelector('[data-rail-next]');

    const update = () => {
      const maxScroll = track.scrollWidth - track.clientWidth;
      if (prev) {
        prev.disabled = track.scrollLeft <= 4;
      }
      if (next) {
        next.disabled = track.scrollLeft >= maxScroll - 4;
      }
    };

    prev?.addEventListener('click', () => {
      track.scrollBy({ left: -track.clientWidth * 0.8, behavior: 'smooth' });
    });
    next?.addEventListener('click', () => {
      track.scrollBy({ left: track.clientWidth * 0.8, behavior: 'smooth' });
    });

    track.addEventListener('scroll', update, { passive: true });
    update();
  });
}

function refreshScraperSettingsUi() {
  scraperNextInputs.forEach((input, provider) => {
    const settings = currentSettings.scrapers?.[provider] || {};
    const nextValue = Number(settings.next_page);
    if (document.activeElement !== input) {
      input.value = Number.isFinite(nextValue) && nextValue > 0 ? nextValue : '';
    }
  });

  scraperLastPageLabels.forEach((label, provider) => {
    const settings = currentSettings.scrapers?.[provider] || {};
    const lastValue = Number(settings.last_page);
    label.textContent = Number.isFinite(lastValue) && lastValue > 0 ? String(lastValue) : '–';
  });
}

function bindButtons() {
  const syncButton = document.getElementById('syncTmdb');
  const settingsForm = document.getElementById('settingsForm');
  const resetButton = document.getElementById('resetScraped');

  syncButton?.addEventListener('click', syncTmdb);
  topbarScrapeButton?.addEventListener('click', startAllScrapers);
  scraperStartAllButton?.addEventListener('click', startAllScrapers);
  scraperControllers.forEach((controller) => {
    controller.startButton?.addEventListener('click', () => startScraper(controller.id));
    controller.refreshButton?.addEventListener('click', () => {
      fetchScraperStatus({ manual: true }).then(() => ensureScraperStatusPolling());
    });
  });
  resetButton?.addEventListener('click', resetScrapedContent);

  if (settingsForm) {
    settingsForm.addEventListener('submit', saveSettings);
  }
}

async function loadSettings() {
  try {
    const settings = await callApi('/api/settings');
    currentSettings = {
      ...currentSettings,
      tmdb_api_key: settings.tmdb_api_key || '',
      scrapers: settings.scrapers || {},
    };
    const form = document.getElementById('settingsForm');
    if (!form) {
      refreshScraperSettingsUi();
      return;
    }

    form.tmdb_api_key.value = currentSettings.tmdb_api_key || '';
    refreshScraperSettingsUi();
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

  const scraperSettings = {};
  scraperNextInputs.forEach((input, provider) => {
    if (input.value) {
      scraperSettings[provider] = { next_page: input.value };
    }
  });
  if (Object.keys(scraperSettings).length) {
    payload.scrapers = scraperSettings;
  }

  try {
    showToast('Einstellungen werden gespeichert...');
    const { success, settings } = await callApi('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (success) {
      if (settings?.tmdb_api_key !== undefined) {
        currentSettings.tmdb_api_key = settings.tmdb_api_key;
      }
      if (settings?.scrapers) {
        currentSettings.scrapers = {
          ...currentSettings.scrapers,
          ...settings.scrapers,
        };
      }
      refreshScraperSettingsUi();
      showToast('Einstellungen gespeichert.', 'success');
    }
  } catch (_) {
    // Fehler bereits behandelt
  }
}

function bindContentCards() {
  const triggers = document.querySelectorAll(
    '.media-card[data-movie-id], .card[data-movie-id], .view-detail[data-movie-id], .scraper-card[data-movie-id], .scraper-result[data-movie-id], .detail-all-card[data-movie-id], .media-card[data-series-id], .card[data-series-id], .view-detail[data-series-id], .scraper-card[data-series-id], .scraper-result[data-series-id]'
  );

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
      const seriesId = element.dataset.seriesId;
      if (seriesId) {
        hideSearchOverlay();
        openSeriesDetail(seriesId);
        return;
      }
      if (movieId) {
        hideSearchOverlay();
        openMovieDetail(movieId);
      }
    };

    element.addEventListener('click', activate);

    if (
      element.classList.contains('media-card') ||
      element.classList.contains('card') ||
      element.classList.contains('scraper-card') ||
      element.classList.contains('scraper-result') ||
      element.classList.contains('detail-all-card')
    ) {
      element.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          activate();
        }
      });
    }
  });
}

function getMoviePosterUrl(movie) {
  const placeholder = detailPoster?.dataset?.placeholder || '';
  if (!movie) {
    return placeholder;
  }
  const posterUrl = movie.poster_url || movie.poster_path || '';
  if (typeof posterUrl === 'string' && posterUrl.trim()) {
    if (posterUrl.startsWith('http')) {
      return posterUrl;
    }
    return `https://image.tmdb.org/t/p/w342${posterUrl}`;
  }
  return placeholder;
}

function updateAllMoviesActive(movieId) {
  if (!detailAllMoviesGrid) return;
  const targetId = movieId != null ? String(movieId) : null;
  detailAllMoviesGrid.querySelectorAll('.detail-all-card').forEach((card) => {
    const isActive = targetId && card.dataset.movieId === targetId;
    card.classList.toggle('is-active', Boolean(isActive));
  });
}

function getSortedMovies() {
  return allMoviesSorted.length ? allMoviesSorted : allMovies;
}

function getAllMoviesTotalPages() {
  const movies = getSortedMovies();
  if (!movies.length) {
    return 1;
  }
  return Math.max(1, Math.ceil(movies.length / ALL_MOVIES_PAGE_SIZE));
}

function updateAllMoviesControls() {
  const movies = getSortedMovies();
  const totalPages = getAllMoviesTotalPages();

  detailAllPageLabels.forEach((label) => {
    if (allMoviesLoading) {
      label.textContent = 'Lädt…';
    } else if (allMoviesSorting) {
      label.textContent = 'Sortiert…';
    } else if (!movies.length) {
      label.textContent = 'Keine Inhalte';
    } else {
      label.textContent = `Seite ${allMoviesPage} / ${totalPages}`;
    }
  });

  const disableControls =
    allMoviesLoading || allMoviesSorting || !movies.length || totalPages <= 1;

  detailAllPrevButtons.forEach((button) => {
    button.disabled = disableControls || allMoviesPage <= 1;
  });
  detailAllNextButtons.forEach((button) => {
    button.disabled = disableControls || allMoviesPage >= totalPages;
  });
}

function getMovieRuntime(movie) {
  const runtimeValue = Number(movie?.runtime);
  if (Number.isFinite(runtimeValue) && runtimeValue > 0) {
    return runtimeValue;
  }
  const cached = allMoviesRuntimeCache.get(movie?.id);
  const cachedValue = Number(cached);
  if (Number.isFinite(cachedValue) && cachedValue > 0) {
    return cachedValue;
  }
  return null;
}

function compareAllMovies(a, b) {
  const direction = allMoviesDirection === 'asc' ? 1 : -1;

  const compareNumbers = (aValue, bValue) => {
    const first = Number(aValue);
    const second = Number(bValue);
    const firstValid = Number.isFinite(first);
    const secondValid = Number.isFinite(second);
    if (!firstValid && !secondValid) {
      return 0;
    }
    if (!firstValid) {
      return direction === 'asc' ? 1 : -1;
    }
    if (!secondValid) {
      return direction === 'asc' ? -1 : 1;
    }
    if (first === second) {
      return 0;
    }
    return (first - second) * direction;
  };

  const compareDates = (firstValue, secondValue) => {
    const firstDate = firstValue ? new Date(firstValue).getTime() : NaN;
    const secondDate = secondValue ? new Date(secondValue).getTime() : NaN;
    const firstValid = Number.isFinite(firstDate);
    const secondValid = Number.isFinite(secondDate);
    if (!firstValid && !secondValid) {
      return 0;
    }
    if (!firstValid) {
      return direction === 'asc' ? 1 : -1;
    }
    if (!secondValid) {
      return direction === 'asc' ? -1 : 1;
    }
    if (firstDate === secondDate) {
      return 0;
    }
    return (firstDate - secondDate) * direction;
  };

  switch (allMoviesSort) {
    case 'title': {
      const firstTitle = typeof a.title === 'string' ? a.title : '';
      const secondTitle = typeof b.title === 'string' ? b.title : '';
      const result = titleCollator.compare(firstTitle, secondTitle);
      if (result === 0) {
        return 0;
      }
      return result * (allMoviesDirection === 'asc' ? 1 : -1);
    }
    case 'runtime': {
      const runtimeA = getMovieRuntime(a);
      const runtimeB = getMovieRuntime(b);
      const compareResult = compareNumbers(runtimeA, runtimeB);
      if (compareResult !== 0) {
        return compareResult;
      }
      break;
    }
    case 'release': {
      const compareResult = compareDates(a.release_date, b.release_date);
      if (compareResult !== 0) {
        return compareResult;
      }
      break;
    }
    case 'added': {
      const compareResult = compareDates(a.created_at, b.created_at);
      if (compareResult !== 0) {
        return compareResult;
      }
      break;
    }
    default: {
      const compareResult = compareNumbers(a.rating, b.rating);
      if (compareResult !== 0) {
        return compareResult;
      }
      break;
    }
  }

  const fallbackTitleA = typeof a.title === 'string' ? a.title : '';
  const fallbackTitleB = typeof b.title === 'string' ? b.title : '';
  return titleCollator.compare(fallbackTitleA, fallbackTitleB);
}

function renderAllMoviesPage() {
  if (!detailAllMoviesGrid) {
    return;
  }

  detailAllMoviesGrid.innerHTML = '';

  if (allMoviesLoading || allMoviesSorting) {
    const loading = document.createElement('p');
    loading.classList.add('empty');
    loading.textContent = allMoviesLoading
      ? 'Filme werden geladen…'
      : 'Sortierung wird aktualisiert…';
    detailAllMoviesGrid.appendChild(loading);
    updateAllMoviesControls();
    return;
  }

  const movies = getSortedMovies();

  if (!movies.length) {
    const empty = document.createElement('p');
    empty.classList.add('empty');
    empty.textContent = 'Noch keine Filme vorhanden.';
    detailAllMoviesGrid.appendChild(empty);
    updateAllMoviesControls();
    return;
  }

  const totalPages = getAllMoviesTotalPages();
  if (allMoviesPage > totalPages) {
    allMoviesPage = totalPages;
  }
  if (allMoviesPage < 1) {
    allMoviesPage = 1;
  }

  const startIndex = (allMoviesPage - 1) * ALL_MOVIES_PAGE_SIZE;
  const pageItems = movies.slice(startIndex, startIndex + ALL_MOVIES_PAGE_SIZE);

  pageItems.forEach((movie) => {
    const card = document.createElement('article');
    card.classList.add('detail-all-card');
    card.dataset.movieId = String(movie.id);

    const media = document.createElement('div');
    media.className = 'detail-all-card__media';

    const image = document.createElement('div');
    image.className = 'detail-all-card__image';
    const posterUrl = getMoviePosterUrl(movie);
    if (posterUrl) {
      image.style.backgroundImage = `url('${posterUrl}')`;
    }

    const overlay = document.createElement('div');
    overlay.className = 'detail-all-card__overlay';

    const meta = document.createElement('div');
    meta.className = 'detail-all-card__meta';
    const releaseYear = typeof movie.release_date === 'string' && movie.release_date ? movie.release_date.slice(0, 4) : '';
    if (releaseYear) {
      const year = document.createElement('span');
      year.className = 'detail-all-card__year';
      year.textContent = releaseYear;
      meta.appendChild(year);
    }
    const ratingValue = Number(movie.rating);
    if (Number.isFinite(ratingValue) && ratingValue > 0) {
      const rating = document.createElement('span');
      rating.className = 'detail-all-card__rating';
      rating.textContent = ratingValue.toFixed(1);
      meta.appendChild(rating);
    }
    if (meta.childElementCount > 0) {
      overlay.appendChild(meta);
    }

    const title = document.createElement('h3');
    title.className = 'detail-all-card__title';
    title.textContent = movie.title || 'Unbekannt';
    overlay.appendChild(title);

    media.appendChild(image);
    media.appendChild(overlay);
    card.setAttribute('role', 'button');
    card.tabIndex = 0;
    card.appendChild(media);
    detailAllMoviesGrid.appendChild(card);
  });

  bindContentCards();
  updateAllMoviesActive(currentMovieId);
  updateAllMoviesControls();
}

async function ensureRuntimeForMovies(movieIds) {
  const uniqueIds = Array.from(
    new Set(
      movieIds
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value) && value > 0),
    ),
  );
  if (!uniqueIds.length) {
    return;
  }

  for (let index = 0; index < uniqueIds.length; index += ALL_MOVIES_RUNTIME_CHUNK_SIZE) {
    const chunk = uniqueIds.slice(index, index + ALL_MOVIES_RUNTIME_CHUNK_SIZE);
    try {
      const response = await callApi('/api/movies/runtime', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ movie_ids: chunk }),
      });
      if (response?.success && Array.isArray(response.items)) {
        response.items.forEach((item) => {
          const id = Number(item.id);
          const runtimeValue = Number(item.runtime);
          const normalizedRuntime = Number.isFinite(runtimeValue) && runtimeValue > 0 ? runtimeValue : null;
          if (Number.isFinite(id)) {
            allMoviesRuntimeCache.set(id, normalizedRuntime);
            const target = allMovies.find((movie) => movie.id === id);
            if (target) {
              target.runtime = normalizedRuntime;
            }
          }
        });
      }
    } catch (error) {
      console.warn('Konnte Laufzeiten nicht laden.', error);
    }
  }
}

async function applyAllMoviesSort({ resetPage = false } = {}) {
  if (!detailAllMoviesGrid) {
    return;
  }
  if (!allMovies.length) {
    allMoviesSorted = [];
    renderAllMoviesPage();
    return;
  }

  const movies = [...allMovies];

  if (allMoviesSort === 'runtime') {
    const missingRuntimeIds = movies
      .filter((movie) => getMovieRuntime(movie) == null)
      .map((movie) => movie.id);
    if (missingRuntimeIds.length) {
      allMoviesSorting = true;
      renderAllMoviesPage();
      try {
        await ensureRuntimeForMovies(missingRuntimeIds);
      } finally {
        allMoviesSorting = false;
      }
    }
    movies.forEach((movie) => {
      const runtimeValue = getMovieRuntime(movie);
      if (runtimeValue != null) {
        movie.runtime = runtimeValue;
      }
    });
  }

  movies.sort(compareAllMovies);
  allMoviesSorted = movies;

  if (resetPage) {
    allMoviesPage = 1;
  } else if (currentMovieId != null) {
    const index = allMoviesSorted.findIndex((item) => item.id === currentMovieId);
    if (index >= 0) {
      const targetPage = Math.floor(index / ALL_MOVIES_PAGE_SIZE) + 1;
      if (targetPage !== allMoviesPage) {
        allMoviesPage = targetPage;
      }
    }
  }

  renderAllMoviesPage();
}

async function ensureAllMoviesLoaded() {
  if (!detailAllMoviesGrid) {
    return;
  }
  if (allMoviesLoaded || allMoviesLoading) {
    if (allMoviesLoaded) {
      renderAllMoviesPage();
    }
    return;
  }

  allMoviesLoading = true;
  renderAllMoviesPage();

  try {
    const movies = await callApi('/api/movies');
    if (Array.isArray(movies)) {
      allMovies = movies
        .filter((movie) => {
          if (!movie || !Array.isArray(movie.streaming_links)) {
            return false;
          }
          return movie.streaming_links.some((link) => {
            if (!link) {
              return false;
            }
            const url = typeof link.url === 'string' ? link.url.trim() : '';
            return url.length > 0;
          });
        })
        .map((movie) => ({ ...movie }));
      allMoviesLoaded = true;
      if (currentMovieId != null) {
        const index = allMovies.findIndex((item) => item.id === currentMovieId);
        if (index >= 0) {
          allMoviesPage = Math.floor(index / ALL_MOVIES_PAGE_SIZE) + 1;
        } else {
          allMoviesPage = 1;
        }
      } else {
        allMoviesPage = 1;
      }
    } else {
      allMovies = [];
      allMoviesLoaded = true;
      allMoviesPage = 1;
    }
  } catch (error) {
    console.error(error);
    allMovies = [];
    allMoviesLoaded = true;
    allMoviesPage = 1;
  } finally {
    allMoviesLoading = false;
  }

  try {
    await applyAllMoviesSort({ resetPage: true });
  } catch (error) {
    console.error(error);
    allMoviesSorted = [...allMovies];
    renderAllMoviesPage();
  }
}

function changeAllMoviesPage(offset) {
  const movies = getSortedMovies();
  if (!detailAllMoviesGrid || !movies.length || allMoviesLoading || allMoviesSorting) {
    return;
  }
  const totalPages = getAllMoviesTotalPages();
  const nextPage = Math.min(Math.max(allMoviesPage + offset, 1), totalPages);
  if (nextPage === allMoviesPage) {
    return;
  }
  allMoviesPage = nextPage;
  renderAllMoviesPage();
  detailAllMoviesGrid.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function setProfileFormVisibility(visible, options = {}) {
  if (!profileFormWrapper) {
    return;
  }

  const { scroll = true } = options;
  profileFormWrapper.hidden = !visible;
  profileFormWrapper.dataset.open = visible ? 'true' : 'false';

  profileEditToggles.forEach((toggle) => {
    toggle.setAttribute('aria-expanded', visible ? 'true' : 'false');
  });

  if (visible) {
    if (scroll) {
      profileFormWrapper.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
    const firstField = profileFormWrapper.querySelector('input, textarea, select');
    if (firstField) {
      window.requestAnimationFrame(() => {
        firstField.focus();
      });
    }
  }
}

function toggleProfileFormVisibility() {
  if (!profileFormWrapper) {
    return;
  }
  const isOpen = profileFormWrapper.dataset.open === 'true';
  setProfileFormVisibility(!isOpen);
}

function initAllMoviesSort() {
  if (!allMoviesSortButtons?.length) {
    return;
  }

  let storedSortPreference = loadAllMoviesSortPreference();
  if (storedSortPreference) {
    const targetButton = Array.from(allMoviesSortButtons).find((button) => {
      const option = button.dataset.sortOption || 'popular';
      const direction = button.dataset.sortDirection || (option === 'title' ? 'asc' : 'desc');
      return option === storedSortPreference.sort && direction === storedSortPreference.direction;
    });
    if (targetButton) {
      allMoviesSort = storedSortPreference.sort;
      allMoviesDirection = storedSortPreference.direction;
    } else {
      storedSortPreference = null;
    }
  }

  const getButtonSortInfo = (button) => {
    const option = button.dataset.sortOption || 'popular';
    const direction = button.dataset.sortDirection || (option === 'title' ? 'asc' : 'desc');
    return { option, direction };
  };

  const updateActiveState = () => {
    allMoviesSortButtons.forEach((button) => {
      const { option, direction } = getButtonSortInfo(button);
      button.classList.toggle('is-active', option === allMoviesSort && direction === allMoviesDirection);
    });
  };

  updateActiveState();

  allMoviesSortButtons.forEach((button) => {
    const { option, direction } = getButtonSortInfo(button);
    button.addEventListener('click', async () => {
      if (allMoviesLoading || allMoviesSorting) {
        return;
      }
      if (option === allMoviesSort && direction === allMoviesDirection) {
        return;
      }
      allMoviesSort = option;
      allMoviesDirection = direction;
      updateActiveState();
      saveAllMoviesSortPreference(allMoviesSort, allMoviesDirection);
      try {
        await applyAllMoviesSort({ resetPage: true });
      } catch (error) {
        console.error(error);
      }
    });
  });

  if (!storedSortPreference) {
    saveAllMoviesSortPreference(allMoviesSort, allMoviesDirection);
  }
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
  const embedUrl = new URL(`https://www.youtube-nocookie.com/embed/${currentTrailer.key}`);
  embedUrl.searchParams.set('autoplay', '1');
  embedUrl.searchParams.set('rel', '0');
  embedUrl.searchParams.set('modestbranding', '1');
  embedUrl.searchParams.set('playsinline', '1');
  trailerModal.classList.add('show');
  trailerModal.setAttribute('aria-hidden', 'false');
  if (trailerModalFrame) {
    trailerModalFrame.src = embedUrl.toString();
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
  currentMovieId = null;
  currentContentType = 'movie';
  if (detailLabel) {
    detailLabel.textContent = 'Film';
  }
  resetSeriesDetail();
  updateAllMoviesActive(null);
  closeTrailerModal();
}

function closeDetail() {
  if (!detailPanel) return;
  resetDetailView();
  const fallbackSection = document.body?.dataset?.currentPage || 'start';
  const target = previousSectionName && previousSectionName !== 'detail' ? previousSectionName : fallbackSection;
  changeSection(target);
  previousSectionName = target;
  currentSectionName = target;
}

function showDetailLoadingState() {
  resetDetailView();
  if (detailTitle) {
    detailTitle.textContent = 'Lädt…';
  }
  if (detailOverview) {
    detailOverview.textContent = 'Details werden geladen…';
  }
  if (detailFacts) {
    const loadingFact = document.createElement('li');
    loadingFact.classList.add('empty');
    loadingFact.textContent = 'Details werden geladen…';
    detailFacts.appendChild(loadingFact);
  }
  if (detailAllMoviesGrid) {
    detailAllMoviesGrid.innerHTML = '';
    const loading = document.createElement('p');
    loading.classList.add('empty');
    loading.textContent = 'Filme werden geladen…';
    detailAllMoviesGrid.appendChild(loading);
  }
  detailAllPageLabels.forEach((label) => {
    label.textContent = 'Lädt…';
  });
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
    window.open(streamUrl, '_blank', 'noopener,noreferrer');
  } else if (detailStreaming) {
    detailStreaming.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
});
trailerButton?.addEventListener('click', openTrailerModal);
trailerModalClose?.addEventListener('click', closeTrailerModal);
trailerModalBackdrop?.addEventListener('click', closeTrailerModal);

if (profileFormWrapper) {
  const initiallyOpen = profileFormWrapper.dataset.open === 'true';
  setProfileFormVisibility(initiallyOpen, { scroll: false });
}

profileEditToggles.forEach((toggle) => {
  toggle.addEventListener('click', () => {
    toggleProfileFormVisibility();
  });
});

profileEditCancelButtons.forEach((button) => {
  button.addEventListener('click', () => {
    setProfileFormVisibility(false);
    const firstToggle = profileEditToggles[0];
    if (firstToggle) {
      firstToggle.focus();
    }
  });
});

detailAllPrevButtons.forEach((button) => {
  button.addEventListener('click', () => changeAllMoviesPage(-1));
});
detailAllNextButtons.forEach((button) => {
  button.addEventListener('click', () => changeAllMoviesPage(1));
});
heroPlayButton?.addEventListener('click', () => {
  const movieId = heroPlayButton.dataset.movieId;
  if (movieId) {
    openMovieDetail(movieId);
  }
});

async function openMovieDetail(movieId) {
  try {
    if (currentSectionName !== 'detail') {
      previousSectionName = currentSectionName;
    }
    showDetailLoadingState();
    changeSection('detail');
    const moviesPromise = ensureAllMoviesLoaded();
    const { success, movie } = await callApi(`/api/movies/${movieId}`);
    await moviesPromise;
    if (!success) return;
    populateDetail(movie);
  } catch (_) {
    closeDetail();
  }
}

async function openSeriesDetail(seriesId) {
  try {
    if (currentSectionName !== 'detail') {
      previousSectionName = currentSectionName;
    }
    showDetailLoadingState();
    changeSection('detail');
    const { success, series } = await callApi(`/api/series/${seriesId}`);
    if (!success) return;
    populateDetail(series);
  } catch (_) {
    closeDetail();
  }
}


function populateDetail(item) {
  if (
    !detailPanel ||
    !detailPoster ||
    !detailBackdrop ||
    !detailTitle ||
    !detailMeta ||
    !detailCast ||
    !detailStreaming ||
    !detailFacts
  ) {
    return;
  }

  const contentType = (item?.content_type || 'movie').toLowerCase();
  const isSeries = contentType === 'series';
  currentContentType = contentType;

  currentMovieId = isSeries ? null : item?.id ?? null;
  currentMovieTitle = item?.title || item?.name || 'Unbekannter Titel';

  const numericId = Number(item?.id);
  if (Number.isFinite(numericId) && numericId > 0) {
    recordContentView({ content_type: contentType, object_id: numericId });
  }

  if (detailLabel) {
    detailLabel.textContent = isSeries ? 'Serie' : 'Film';
  }

  if (detailPoster) {
    detailPoster.src = getMoviePosterUrl(item);
    detailPoster.alt = currentMovieTitle;
  }

  const backdropUrl =
    (item?.backdrop_url && item.backdrop_url.trim()) ||
    (item?.backdrop_path ? `https://image.tmdb.org/t/p/w1280${item.backdrop_path}` : '') ||
    '';
  if (detailBackdrop) {
    detailBackdrop.style.backgroundImage = backdropUrl ? `url('${backdropUrl}')` : 'none';
  }

  if (detailTitle) {
    detailTitle.textContent = currentMovieTitle;
  }
  if (detailTagline) {
    const tagline = item?.tagline || '';
    detailTagline.textContent = tagline;
    detailTagline.style.display = tagline ? '' : 'none';
  }
  if (detailOverview) {
    detailOverview.textContent = item?.overview || 'Keine Beschreibung verfügbar.';
  }
  if (detailOverviewSecondary) {
    detailOverviewSecondary.textContent = item?.overview || '';
  }

  detailMeta.innerHTML = '';
  const metaEntries = [];
  if (isSeries) {
    if (item?.first_air_date) {
      metaEntries.push(formatDate(item.first_air_date));
    }
    if (item?.last_air_date && item.last_air_date !== item.first_air_date) {
      metaEntries.push(`Letzte Folge ${formatDate(item.last_air_date)}`);
    }
  } else {
    const releaseYear = item?.release_date ? item.release_date.split('-')[0] : '';
    if (releaseYear) {
      metaEntries.push(releaseYear);
    }
    if (Number.isFinite(item?.runtime) && item.runtime > 0) {
      metaEntries.push(`${item.runtime} Min.`);
    }
  }
  if (typeof item?.rating === 'number' && item.rating > 0) {
    metaEntries.push(`⭐ ${item.rating.toFixed(1)}`);
  }
  const genreNames = Array.isArray(item?.genres)
    ? item.genres
        .map((genre) => {
          if (!genre) return null;
          if (typeof genre === 'string') return genre;
          if (typeof genre.name === 'string') return genre.name;
          return null;
        })
        .filter(Boolean)
    : [];
  if (genreNames.length) {
    metaEntries.push(...genreNames.slice(0, 3));
  }
  if (isSeries) {
    if (item?.total_seasons) {
      metaEntries.push(`${item.total_seasons} Staffel${item.total_seasons === 1 ? '' : 'n'}`);
    }
    if (item?.total_episodes) {
      metaEntries.push(`${item.total_episodes} Episode${item.total_episodes === 1 ? '' : 'n'}`);
    }
  }
  if (!metaEntries.length) {
    metaEntries.push('Keine zusätzlichen Infos');
  }
  metaEntries.forEach((entry) => {
    const span = document.createElement('span');
    span.textContent = entry;
    detailMeta.appendChild(span);
  });

  detailFacts.innerHTML = '';
  const facts = [];
  if (isSeries) {
    const firstAir = formatDate(item?.first_air_date);
    const lastAir = formatDate(item?.last_air_date);
    if (firstAir) {
      facts.push({ label: 'Erstausstrahlung', value: firstAir });
    }
    if (lastAir) {
      facts.push({ label: 'Letzte Ausstrahlung', value: lastAir });
    }
    if (item?.status) {
      facts.push({ label: 'Status', value: item.status });
    }
    if (item?.total_seasons) {
      facts.push({ label: 'Staffeln', value: String(item.total_seasons) });
    }
    if (item?.total_episodes) {
      facts.push({ label: 'Episoden', value: String(item.total_episodes) });
    }
    if (Array.isArray(item?.episode_run_time) && item.episode_run_time.length) {
      const runtimeValue = item.episode_run_time.find((value) => Number.isFinite(Number(value)));
      if (runtimeValue) {
        facts.push({ label: 'Laufzeit', value: `${runtimeValue} Minuten` });
      }
    }
    if (genreNames.length) {
      const label = genreNames.length === 1 ? 'Genre' : 'Genres';
      facts.push({ label, value: genreNames.join(', ') });
    }
  } else {
    const formattedDate = formatDate(item?.release_date);
    if (formattedDate) {
      facts.push({ label: 'Veröffentlichung', value: formattedDate });
    }
    if (Number.isFinite(item?.runtime) && item.runtime > 0) {
      facts.push({ label: 'Laufzeit', value: `${item.runtime} Minuten` });
    }
    if (typeof item?.rating === 'number' && item.rating > 0) {
      facts.push({ label: 'Bewertung', value: `${item.rating.toFixed(1)} / 10` });
    }
    if (genreNames.length) {
      const label = genreNames.length === 1 ? 'Genre' : 'Genres';
      facts.push({ label, value: genreNames.join(', ') });
    }
    const streamCount = Array.isArray(item?.streaming_links)
      ? item.streaming_links.filter((link) => link && link.url).length
      : 0;
    facts.push({
      label: 'Streams',
      value: streamCount ? `${streamCount} Quelle${streamCount === 1 ? '' : 'n'}` : 'Keine Quellen',
    });
  }

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
  const castEntries = Array.isArray(item?.cast) ? item.cast.filter(Boolean) : [];

  if (castEntries.length) {
    castEntries.forEach((entry) => {
      const castItem = typeof entry === 'string' ? { name: entry } : entry || {};
      const name =
        (typeof castItem.name === 'string' && castItem.name.trim()) ||
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

  if (isSeries) {
    setupSeriesEpisodes(item);
    const defaultEpisode = item?.default_episode;
    if (defaultEpisode && Number.isFinite(Number(defaultEpisode.season_number))) {
      selectSeriesEpisode(defaultEpisode.season_number, defaultEpisode.episode_number);
    } else {
      const seasonNumbers = Array.from(currentSeriesEpisodes.keys()).sort((a, b) => a - b);
      if (seasonNumbers.length) {
        const firstSeason = seasonNumbers[0];
        const episodes = currentSeriesEpisodes.get(firstSeason) || [];
        const preferredEpisode =
          episodes.find(
            (episode) =>
              Array.isArray(episode?.streaming_links) && episode.streaming_links.some((link) => link && link.url)
          ) || episodes[0];
        if (preferredEpisode) {
          selectSeriesEpisode(firstSeason, preferredEpisode.episode_number);
        } else {
          renderStreamingLinks([]);
        }
      } else {
        renderStreamingLinks([]);
      }
    }
  } else {
    if (detailEpisodesSection) {
      detailEpisodesSection.hidden = true;
    }
    renderStreamingLinks(item?.streaming_links || []);
  }

  const trailer = item?.trailer;
  if (trailer?.site === 'YouTube' && trailer?.key) {
    currentTrailer = trailer;
  } else {
    currentTrailer = null;
  }

  if (trailerButton) {
    trailerButton.disabled = !currentTrailer;
  }

  if (!isSeries && allMoviesLoaded) {
    if (currentMovieId != null) {
      const index = allMovies.findIndex((entry) => entry.id === currentMovieId);
      if (index >= 0) {
        const targetPage = Math.floor(index / ALL_MOVIES_PAGE_SIZE) + 1;
        if (targetPage !== allMoviesPage) {
          allMoviesPage = targetPage;
          renderAllMoviesPage();
        } else {
          updateAllMoviesActive(currentMovieId);
        }
      } else {
        updateAllMoviesActive(currentMovieId);
      }
    } else {
      updateAllMoviesActive(null);
    }
  } else if (!isSeries) {
    updateAllMoviesActive(currentMovieId);
  } else {
    updateAllMoviesActive(null);
  }
}
searchOverlayClose?.addEventListener('click', () => {
  hideSearchOverlay();
  if (topbarSearchInput) {
    topbarSearchInput.focus();
  }
});

searchOverlay?.addEventListener('click', (event) => {
  if (event.target === searchOverlay) {
    hideSearchOverlay();
    if (topbarSearchInput) {
      topbarSearchInput.focus();
    }
  }
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && isSearchOverlayActive()) {
    hideSearchOverlay();
  }
});

initSearch();
bindButtons();
initNavigation();
bindContentCards();
initAllMoviesSort();
initMediaRails();
initHero();
initScraperStatus();
loadSettings();
if (document.body?.dataset?.loadAllMovies === 'true') {
  ensureAllMoviesLoaded();
}
