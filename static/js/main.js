const toast = document.getElementById('toast');
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

function initHero() {
  const cards = Array.from(document.querySelectorAll('.card'));
  const hero = document.getElementById('hero');
  if (!cards.length || !hero) return;

  function setHero(card) {
    const title = card.dataset.title;
    const image = card.querySelector('.card-poster').style.backgroundImage;
    hero.style.setProperty('--hero-image', image || "url('')");
    hero.querySelector('h2').textContent = title;
  }

  document.getElementById('refreshHero').addEventListener('click', () => {
    const randomCard = cards[Math.floor(Math.random() * cards.length)];
    setHero(randomCard);
  });

  setHero(cards[0]);
}

function bindButtons() {
  const syncButton = document.getElementById('syncTmdb');
  const scrapeButton = document.getElementById('scrapeKinox');
  const settingsForm = document.getElementById('settingsForm');

  if (syncButton) {
    syncButton.addEventListener('click', syncTmdb);
  }

  if (scrapeButton) {
    scrapeButton.addEventListener('click', scrapeKinox);
  }

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

bindButtons();
initHero();
loadSettings();
