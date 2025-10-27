const toast = document.getElementById('toast');

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
      throw new Error(`Serverfehler: ${response.status}`);
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
    const { success, links } = await callApi('/api/scrape/kinox', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start_page: 1, end_page: 1 }),
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

  if (syncButton) {
    syncButton.addEventListener('click', syncTmdb);
  }

  if (scrapeButton) {
    scrapeButton.addEventListener('click', scrapeKinox);
  }
}

bindButtons();
initHero();
