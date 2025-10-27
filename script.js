// ===== DATA MANAGEMENT =====
class BlogManager {
    constructor() {
        this.posts = this.loadPosts();
        this.categories = this.loadCategories();
        this.tags = this.loadTags();
        this.settings = this.loadSettings();
        this.isLoggedIn = false;
        this.editingPostId = null;
        this.editingCategoryId = null;
        this.editingTagId = null;
        this.currentPage = 1;
        this.postsPerPage = this.settings.postsPerPage || 6;
        this.currentFilter = { category: '', search: '' };
    }

    // ===== POSTS =====
    loadPosts() {
        const stored = localStorage.getItem('blogPosts');
        if (stored) {
            return JSON.parse(stored);
        }
        // Default demo posts
        return [
            {
                id: this.generateId(),
                title: 'Ted Bundy',
                alias: 'Der Campus-Killer',
                author: 'Redaktion',
                years: '1974-1978',
                location: 'USA (mehrere Bundesstaaten)',
                victims: 30,
                status: 'Verstorben',
                category: 'Organisierte Täter',
                tags: ['USA', '1970er', 'Organisiert', 'Serienmord'],
                image: 'https://images.unsplash.com/photo-1568602471122-7832951cc4c5?w=800',
                excerpt: 'Theodore Robert Bundy war einer der bekanntesten Serienmörder der USA. Er galt als charmant und intelligent, was ihm half, das Vertrauen seiner Opfer zu gewinnen.',
                content: 'Theodore Robert Bundy war einer der bekanntesten Serienmörder der USA. Er galt als charmant und intelligent, was ihm half, das Vertrauen seiner Opfer zu gewinnen. Seine Verbrechen erstreckten sich über mehrere Bundesstaaten und erschütterten die amerikanische Gesellschaft der 1970er Jahre.',
                mo: 'Bundy lockte seine Opfer oft mit vorgetäuschten Verletzungen oder der Bitte um Hilfe. Er nutzte seine Attraktivität und sein gepflegtes Auftreten, um Vertrauen zu schaffen. Seine Opfer waren meist junge Frauen mit langen, dunklen Haaren.',
                investigation: 'Bundy wurde 1975 erstmals verhaftet und konnte zweimal aus der Haft fliehen. Seine endgültige Festnahme erfolgte 1978 in Florida. Er wurde 1989 auf dem elektrischen Stuhl hingerichtet.',
                date: new Date('2024-01-15').toISOString(),
                readTime: 8
            },
            {
                id: this.generateId(),
                title: 'Jeffrey Dahmer',
                alias: 'Der Milwaukee Cannibal',
                author: 'Redaktion',
                years: '1978-1991',
                location: 'Milwaukee, Wisconsin, USA',
                victims: 17,
                status: 'Verstorben',
                category: 'Desorganisierte Täter',
                tags: ['USA', '1980er', '1990er', 'Kannibalismus'],
                image: 'https://images.unsplash.com/photo-1589829085413-56de8ae18c73?w=800',
                excerpt: 'Jeffrey Lionel Dahmer beging zwischen 1978 und 1991 eine Serie grausamer Morde. Seine Verbrechen umfassten Nekrophilie und Kannibalismus.',
                content: 'Jeffrey Lionel Dahmer beging zwischen 1978 und 1991 eine Serie grausamer Morde. Seine Verbrechen umfassten Nekrophilie und Kannibalismus. Er wurde 1991 verhaftet und 1994 im Gefängnis ermordet.',
                mo: 'Dahmer lockte seine Opfer meist mit dem Versprechen von Geld oder Alkohol in seine Wohnung. Seine Taten waren von extremer Brutalität geprägt. Er versuchte, seine Opfer zu "Zombies" zu machen.',
                investigation: 'Dahmers Verbrechen wurden 1991 aufgedeckt, als eines seiner Opfer fliehen konnte. In seiner Wohnung fanden Ermittler menschliche Überreste. Er wurde zu 15 lebenslangen Haftstrafen verurteilt.',
                date: new Date('2024-02-10').toISOString(),
                readTime: 10
            },
            {
                id: this.generateId(),
                title: 'Jack the Ripper',
                alias: 'Jack the Ripper',
                author: 'Redaktion',
                years: '1888',
                location: 'Whitechapel, London, England',
                victims: 5,
                status: 'Ungeklärt',
                category: 'Ungeklärte Fälle',
                tags: ['England', '1880er', 'Ungeklärt', 'Historisch'],
                image: 'https://images.unsplash.com/photo-1513002749550-c59d786b8e6c?w=800',
                excerpt: 'Jack the Ripper ist der bekannteste ungelöste Fall der Kriminalgeschichte. Im Herbst 1888 wurden im Londoner Stadtteil Whitechapel mindestens fünf Frauen brutal ermordet.',
                content: 'Jack the Ripper ist der bekannteste ungelöste Fall der Kriminalgeschichte. Im Herbst 1888 wurden im Londoner Stadtteil Whitechapel mindestens fünf Frauen brutal ermordet. Die Identität des Täters wurde nie geklärt.',
                mo: 'Die Opfer waren meist Prostituierte, die in den frühen Morgenstunden angegriffen wurden. Die Morde waren von extremer Brutalität und chirurgischer Präzision geprägt.',
                investigation: 'Trotz intensiver Ermittlungen und zahlreicher Theorien bleibt die Identität von Jack the Ripper bis heute ungeklärt. Der Fall inspirierte unzählige Bücher, Filme und Theorien.',
                date: new Date('2024-03-05').toISOString(),
                readTime: 12
            }
        ];
    }

    savePosts() {
        localStorage.setItem('blogPosts', JSON.stringify(this.posts));
    }

    addPost(postData) {
        const newPost = {
            id: this.generateId(),
            ...postData,
            date: new Date().toISOString(),
            readTime: this.calculateReadTime(postData.content)
        };
        this.posts.unshift(newPost);
        this.savePosts();
        return newPost;
    }

    updatePost(id, postData) {
        const index = this.posts.findIndex(p => p.id === id);
        if (index !== -1) {
            this.posts[index] = {
                ...this.posts[index],
                ...postData,
                readTime: this.calculateReadTime(postData.content)
            };
            this.savePosts();
            return true;
        }
        return false;
    }

    deletePost(id) {
        const index = this.posts.findIndex(p => p.id === id);
        if (index !== -1) {
            this.posts.splice(index, 1);
            this.savePosts();
            return true;
        }
        return false;
    }

    getPostById(id) {
        return this.posts.find(p => p.id === id);
    }

    calculateReadTime(content) {
        const wordsPerMinute = 200;
        const words = content.split(/\s+/).length;
        return Math.ceil(words / wordsPerMinute);
    }

    // ===== CATEGORIES =====
    loadCategories() {
        const stored = localStorage.getItem('blogCategories');
        if (stored) {
            return JSON.parse(stored);
        }
        return [
            { id: this.generateId(), name: 'Organisierte Täter', description: 'Geplante und methodische Vorgehensweise' },
            { id: this.generateId(), name: 'Desorganisierte Täter', description: 'Spontane und chaotische Taten' },
            { id: this.generateId(), name: 'Ungeklärte Fälle', description: 'Fälle ohne Täteridentifikation' },
            { id: this.generateId(), name: 'Historische Fälle', description: 'Fälle aus vergangenen Jahrhunderten' }
        ];
    }

    saveCategories() {
        localStorage.setItem('blogCategories', JSON.stringify(this.categories));
    }

    addCategory(categoryData) {
        const newCategory = {
            id: this.generateId(),
            ...categoryData
        };
        this.categories.push(newCategory);
        this.saveCategories();
        return newCategory;
    }

    updateCategory(id, categoryData) {
        const index = this.categories.findIndex(c => c.id === id);
        if (index !== -1) {
            this.categories[index] = { ...this.categories[index], ...categoryData };
            this.saveCategories();
            return true;
        }
        return false;
    }

    deleteCategory(id) {
        const index = this.categories.findIndex(c => c.id === id);
        if (index !== -1) {
            this.categories.splice(index, 1);
            this.saveCategories();
            return true;
        }
        return false;
    }

    // ===== TAGS =====
    loadTags() {
        const stored = localStorage.getItem('blogTags');
        if (stored) {
            return JSON.parse(stored);
        }
        return [
            { id: this.generateId(), name: 'USA' },
            { id: this.generateId(), name: 'England' },
            { id: this.generateId(), name: '1970er' },
            { id: this.generateId(), name: '1980er' },
            { id: this.generateId(), name: 'Organisiert' },
            { id: this.generateId(), name: 'Ungeklärt' }
        ];
    }

    saveTags() {
        localStorage.setItem('blogTags', JSON.stringify(this.tags));
    }

    addTag(tagData) {
        const newTag = {
            id: this.generateId(),
            ...tagData
        };
        this.tags.push(newTag);
        this.saveTags();
        return newTag;
    }

    updateTag(id, tagData) {
        const index = this.tags.findIndex(t => t.id === id);
        if (index !== -1) {
            this.tags[index] = { ...this.tags[index], ...tagData };
            this.saveTags();
            return true;
        }
        return false;
    }

    deleteTag(id) {
        const index = this.tags.findIndex(t => t.id === id);
        if (index !== -1) {
            this.tags.splice(index, 1);
            this.saveTags();
            return true;
        }
        return false;
    }

    // ===== SETTINGS =====
    loadSettings() {
        const stored = localStorage.getItem('blogSettings');
        if (stored) {
            return JSON.parse(stored);
        }
        return {
            siteName: 'Archiv der Dunkelheit',
            siteDescription: 'Kriminologie Blog',
            postsPerPage: 6
        };
    }

    saveSettings() {
        localStorage.setItem('blogSettings', JSON.stringify(this.settings));
    }

    // ===== FILTERING & SEARCH =====
    getFilteredPosts() {
        let filtered = [...this.posts];

        if (this.currentFilter.category) {
            filtered = filtered.filter(p => p.category === this.currentFilter.category);
        }

        if (this.currentFilter.search) {
            const search = this.currentFilter.search.toLowerCase();
            filtered = filtered.filter(p =>
                p.title.toLowerCase().includes(search) ||
                p.excerpt.toLowerCase().includes(search) ||
                p.content.toLowerCase().includes(search) ||
                p.tags.some(t => t.toLowerCase().includes(search))
            );
        }

        return filtered;
    }

    getPaginatedPosts() {
        const filtered = this.getFilteredPosts();
        const start = (this.currentPage - 1) * this.postsPerPage;
        const end = start + this.postsPerPage;
        return filtered.slice(start, end);
    }

    getTotalPages() {
        const filtered = this.getFilteredPosts();
        return Math.ceil(filtered.length / this.postsPerPage);
    }

    // ===== UTILITIES =====
    generateId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2);
    }

    formatDate(dateString) {
        const date = new Date(dateString);
        return date.toLocaleDateString('de-DE', { year: 'numeric', month: 'long', day: 'numeric' });
    }

    exportData() {
        const data = {
            posts: this.posts,
            categories: this.categories,
            tags: this.tags,
            settings: this.settings,
            exportDate: new Date().toISOString()
        };
        return JSON.stringify(data, null, 2);
    }

    importData(jsonString) {
        try {
            const data = JSON.parse(jsonString);
            if (data.posts) this.posts = data.posts;
            if (data.categories) this.categories = data.categories;
            if (data.tags) this.tags = data.tags;
            if (data.settings) this.settings = data.settings;
            this.savePosts();
            this.saveCategories();
            this.saveTags();
            this.saveSettings();
            return true;
        } catch (e) {
            return false;
        }
    }

    clearAllData() {
        localStorage.removeItem('blogPosts');
        localStorage.removeItem('blogCategories');
        localStorage.removeItem('blogTags');
        localStorage.removeItem('blogSettings');
        this.posts = [];
        this.categories = [];
        this.tags = [];
        this.settings = this.loadSettings();
    }
}

// ===== INITIALIZE =====
const blogManager = new BlogManager();

// ===== NAVIGATION =====
function initNavigation() {
    const navLinks = document.querySelectorAll('.nav-link');
    const sections = document.querySelectorAll('.section');

    navLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const targetId = link.getAttribute('href').substring(1);

            navLinks.forEach(l => l.classList.remove('active'));
            link.classList.add('active');

            sections.forEach(s => s.classList.remove('active'));
            document.getElementById(targetId).classList.add('active');

            if (targetId === 'home') {
                renderBlog();
            } else if (targetId === 'archive') {
                renderArchive();
            } else if (targetId === 'admin') {
                if (!blogManager.isLoggedIn) {
                    document.getElementById('adminLogin').style.display = 'block';
                    document.getElementById('adminPanel').style.display = 'none';
                }
            }
        });
    });
}

// ===== BLOG RENDERING =====
function renderBlog() {
    renderHeroPost();
    renderBlogPosts();
    renderSidebar();
    renderPagination();
}

function renderHeroPost() {
    const heroPost = document.getElementById('heroPost');
    if (!heroPost || blogManager.posts.length === 0) return;

    const post = blogManager.posts[0];
    heroPost.innerHTML = `
        <img src="${post.image}" alt="${post.title}" class="hero-post-image" onerror="this.src='https://images.unsplash.com/photo-1568602471122-7832951cc4c5?w=800'">
        <div class="hero-post-overlay">
            <span class="hero-post-category">${post.category}</span>
            <h2 class="hero-post-title">${post.title}</h2>
            <p class="hero-post-excerpt">${post.excerpt}</p>
            <div class="hero-post-meta">
                <span>👤 ${post.author}</span>
                <span>📅 ${blogManager.formatDate(post.date)}</span>
                <span>⏱️ ${post.readTime} Min. Lesezeit</span>
            </div>
        </div>
    `;
    heroPost.onclick = () => showSinglePost(post.id);
}

function renderBlogPosts() {
    const blogPosts = document.getElementById('blogPosts');
    if (!blogPosts) return;

    const posts = blogManager.getPaginatedPosts();

    if (posts.length === 0) {
        blogPosts.innerHTML = `
            <div class="empty-state" style="grid-column: 1 / -1;">
                <div class="empty-state-icon">📝</div>
                <p class="empty-state-text">Keine Artikel gefunden</p>
            </div>
        `;
        return;
    }

    blogPosts.innerHTML = posts.map(post => `
        <div class="blog-post-card" onclick="showSinglePost('${post.id}')">
            <img src="${post.image}" alt="${post.title}" class="blog-post-image" onerror="this.src='https://images.unsplash.com/photo-1568602471122-7832951cc4c5?w=800'">
            <div class="blog-post-content">
                <span class="blog-post-category">${post.category}</span>
                <h3 class="blog-post-title">${post.title}</h3>
                <p class="blog-post-excerpt">${post.excerpt}</p>
                <div class="blog-post-meta">
                    <div class="blog-post-author">
                        <span>👤 ${post.author}</span>
                    </div>
                    <span class="blog-post-date">${blogManager.formatDate(post.date)}</span>
                </div>
            </div>
        </div>
    `).join('');
}

function renderSidebar() {
    // Categories
    const sidebarCategories = document.getElementById('sidebarCategories');
    if (sidebarCategories) {
        sidebarCategories.innerHTML = blogManager.categories.map(cat => {
            const count = blogManager.posts.filter(p => p.category === cat.name).length;
            return `
                <li>
                    <a href="#" onclick="filterByCategory('${cat.name}'); return false;">
                        <span>${cat.name}</span>
                        <span class="category-count">${count}</span>
                    </a>
                </li>
            `;
        }).join('');
    }

    // Tags
    const tagCloud = document.getElementById('tagCloud');
    if (tagCloud) {
        const allTags = new Set();
        blogManager.posts.forEach(post => {
            post.tags.forEach(tag => allTags.add(tag));
        });
        tagCloud.innerHTML = Array.from(allTags).map(tag => `
            <span class="tag" onclick="filterBySearch('${tag}')">${tag}</span>
        `).join('');
    }

    // Archive
    const archiveList = document.getElementById('archiveList');
    if (archiveList) {
        const months = {};
        blogManager.posts.forEach(post => {
            const date = new Date(post.date);
            const key = `${date.getFullYear()}-${date.getMonth()}`;
            const label = date.toLocaleDateString('de-DE', { year: 'numeric', month: 'long' });
            months[key] = { label, count: (months[key]?.count || 0) + 1 };
        });

        archiveList.innerHTML = Object.values(months).map(month => `
            <li>
                <a href="#">
                    <span>${month.label}</span>
                    <span class="archive-count">${month.count}</span>
                </a>
            </li>
        `).join('');
    }

    // Stats
    const totalPosts = document.getElementById('totalPosts');
    const totalCategories = document.getElementById('totalCategories');
    if (totalPosts) totalPosts.textContent = blogManager.posts.length;
    if (totalCategories) totalCategories.textContent = blogManager.categories.length;
}

function renderPagination() {
    const pagination = document.getElementById('pagination');
    if (!pagination) return;

    const totalPages = blogManager.getTotalPages();
    if (totalPages <= 1) {
        pagination.innerHTML = '';
        return;
    }

    let html = `
        <button onclick="changePage(${blogManager.currentPage - 1})" ${blogManager.currentPage === 1 ? 'disabled' : ''}>
            ← Zurück
        </button>
    `;

    for (let i = 1; i <= totalPages; i++) {
        html += `
            <button onclick="changePage(${i})" class="${i === blogManager.currentPage ? 'active' : ''}">
                ${i}
            </button>
        `;
    }

    html += `
        <button onclick="changePage(${blogManager.currentPage + 1})" ${blogManager.currentPage === totalPages ? 'disabled' : ''}>
            Weiter →
        </button>
    `;

    pagination.innerHTML = html;
}

function changePage(page) {
    const totalPages = blogManager.getTotalPages();
    if (page < 1 || page > totalPages) return;
    blogManager.currentPage = page;
    renderBlogPosts();
    renderPagination();
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function filterByCategory(category) {
    blogManager.currentFilter.category = category;
    blogManager.currentPage = 1;
    renderBlogPosts();
    renderPagination();
}

function filterBySearch(search) {
    blogManager.currentFilter.search = search;
    blogManager.currentPage = 1;
    renderBlogPosts();
    renderPagination();
}

// ===== SINGLE POST =====
function showSinglePost(id) {
    const post = blogManager.getPostById(id);
    if (!post) return;

    const singlePostContent = document.getElementById('singlePostContent');
    singlePostContent.innerHTML = `
        <div class="single-post-header">
            <img src="${post.image}" alt="${post.title}" class="single-post-image" onerror="this.src='https://images.unsplash.com/photo-1568602471122-7832951cc4c5?w=800'">
            <div class="single-post-header-overlay">
                <span class="single-post-category">${post.category}</span>
                <h1 class="single-post-title">${post.title}</h1>
                <div class="single-post-meta">
                    <span>👤 ${post.author}</span>
                    <span>📅 ${blogManager.formatDate(post.date)}</span>
                    <span>⏱️ ${post.readTime} Min. Lesezeit</span>
                </div>
            </div>
        </div>
        <div class="single-post-body">
            <p><em>"${post.alias}"</em></p>
            
            <div class="post-info-grid">
                <div class="post-info-item">
                    <span class="post-info-label">Zeitraum</span>
                    <span class="post-info-value">${post.years}</span>
                </div>
                <div class="post-info-item">
                    <span class="post-info-label">Ort</span>
                    <span class="post-info-value">${post.location}</span>
                </div>
                <div class="post-info-item">
                    <span class="post-info-label">Opfer</span>
                    <span class="post-info-value">${post.victims}</span>
                </div>
                <div class="post-info-item">
                    <span class="post-info-label">Status</span>
                    <span class="post-info-value">${post.status}</span>
                </div>
            </div>

            <h3>Übersicht</h3>
            <p>${post.content}</p>

            ${post.mo ? `
                <h3>Modus Operandi</h3>
                <p>${post.mo}</p>
            ` : ''}

            ${post.investigation ? `
                <h3>Ermittlungen & Aufklärung</h3>
                <p>${post.investigation}</p>
            ` : ''}

            <div style="margin-top: 2rem;">
                <strong>Tags:</strong> ${post.tags.map(tag => `<span class="tag">${tag}</span>`).join(' ')}
            </div>

            <a href="#" onclick="backToBlog(); return false;" class="back-to-blog">← Zurück zum Blog</a>
        </div>
    `;

    // Render related posts
    renderRelatedPosts(post);

    // Switch to single post view
    document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
    document.getElementById('singlePost').classList.add('active');
}

function renderRelatedPosts(currentPost) {
    const relatedPosts = document.getElementById('relatedPosts');
    if (!relatedPosts) return;

    const related = blogManager.posts
        .filter(p => p.id !== currentPost.id && p.category === currentPost.category)
        .slice(0, 3);

    if (related.length === 0) {
        relatedPosts.innerHTML = '<p style="color: var(--text-gray);">Keine ähnlichen Artikel gefunden</p>';
        return;
    }

    relatedPosts.innerHTML = related.map(post => `
        <div class="related-post" onclick="showSinglePost('${post.id}')">
            <img src="${post.image}" alt="${post.title}" class="related-post-image" onerror="this.src='https://images.unsplash.com/photo-1568602471122-7832951cc4c5?w=800'">
            <div class="related-post-info">
                <div class="related-post-title">${post.title}</div>
                <div class="related-post-date">${blogManager.formatDate(post.date)}</div>
            </div>
        </div>
    `).join('');
}

function backToBlog() {
    document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
    document.getElementById('home').classList.add('active');
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    document.querySelector('[href="#home"]').classList.add('active');
}

// ===== ARCHIVE =====
function renderArchive() {
    const archiveGrid = document.getElementById('archiveGrid');
    if (!archiveGrid) return;

    archiveGrid.innerHTML = blogManager.posts.map(post => `
        <div class="blog-post-card" onclick="showSinglePost('${post.id}')">
            <img src="${post.image}" alt="${post.title}" class="blog-post-image" onerror="this.src='https://images.unsplash.com/photo-1568602471122-7832951cc4c5?w=800'">
            <div class="blog-post-content">
                <span class="blog-post-category">${post.category}</span>
                <h3 class="blog-post-title">${post.title}</h3>
                <p class="blog-post-excerpt">${post.excerpt}</p>
                <div class="blog-post-meta">
                    <div class="blog-post-author">
                        <span>👤 ${post.author}</span>
                    </div>
                    <span class="blog-post-date">${blogManager.formatDate(post.date)}</span>
                </div>
            </div>
        </div>
    `).join('');
}

// ===== ADMIN =====
function initAdmin() {
    const loginBtn = document.getElementById('loginBtn');
    const logoutBtn = document.getElementById('logoutBtn');
    const passwordInput = document.getElementById('adminPassword');

    loginBtn.addEventListener('click', () => {
        const password = passwordInput.value;
        if (password === 'admin123') {
            blogManager.isLoggedIn = true;
            document.getElementById('adminLogin').style.display = 'none';
            document.getElementById('adminPanel').style.display = 'block';
            renderAdminDashboard();
            passwordInput.value = '';
        } else {
            alert('Falsches Passwort!');
        }
    });

    passwordInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            loginBtn.click();
        }
    });

    logoutBtn.addEventListener('click', () => {
        blogManager.isLoggedIn = false;
        document.getElementById('adminLogin').style.display = 'block';
        document.getElementById('adminPanel').style.display = 'none';
    });

    // Admin navigation
    document.querySelectorAll('.admin-nav-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const section = btn.dataset.section;
            document.querySelectorAll('.admin-nav-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            document.querySelectorAll('.admin-section').forEach(s => s.classList.remove('active'));
            document.getElementById(`admin${section.charAt(0).toUpperCase() + section.slice(1)}`).classList.add('active');

            if (section === 'dashboard') renderAdminDashboard();
            else if (section === 'posts') renderAdminPosts();
            else if (section === 'categories') renderAdminCategories();
            else if (section === 'tags') renderAdminTags();
        });
    });

    initPostEditor();
    initCategoryEditor();
    initTagEditor();
    initSettings();
}

function renderAdminDashboard() {
    document.getElementById('dashTotalPosts').textContent = blogManager.posts.length;
    document.getElementById('dashTotalCategories').textContent = blogManager.categories.length;
    document.getElementById('dashTotalTags').textContent = blogManager.tags.length;
    
    const lastPost = blogManager.posts[0];
    document.getElementById('dashLastUpdate').textContent = lastPost ? blogManager.formatDate(lastPost.date) : '-';

    const recentActivity = document.getElementById('recentActivity');
    if (blogManager.posts.length === 0) {
        recentActivity.innerHTML = '<p class="activity-item">Noch keine Aktivitäten</p>';
    } else {
        recentActivity.innerHTML = blogManager.posts.slice(0, 5).map(post => `
            <div class="activity-item">
                Artikel "${post.title}" erstellt am ${blogManager.formatDate(post.date)}
            </div>
        `).join('');
    }
}

function initPostEditor() {
    const addNewPost = document.getElementById('addNewPost');
    const postEditor = document.getElementById('postEditor');
    const postForm = document.getElementById('postForm');
    const cancelPost = document.getElementById('cancelPost');
    const previewPost = document.getElementById('previewPost');

    addNewPost.addEventListener('click', () => {
        blogManager.editingPostId = null;
        postForm.reset();
        document.getElementById('editorTitle').textContent = 'Neuen Artikel erstellen';
        postEditor.style.display = 'block';
        renderCategoryOptions();
    });

    cancelPost.addEventListener('click', () => {
        postEditor.style.display = 'none';
        postForm.reset();
        blogManager.editingPostId = null;
    });

    previewPost.addEventListener('click', () => {
        const postData = getPostFormData();
        if (!postData) return;
        
        // Create temporary post for preview
        const tempPost = {
            id: 'preview',
            ...postData,
            date: new Date().toISOString(),
            readTime: blogManager.calculateReadTime(postData.content)
        };
        
        showSinglePost(tempPost.id);
        // Temporarily add to posts for preview
        const originalPosts = [...blogManager.posts];
        blogManager.posts.unshift(tempPost);
        showSinglePost(tempPost.id);
        blogManager.posts = originalPosts;
    });

    postForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const postData = getPostFormData();
        if (!postData) return;

        if (blogManager.editingPostId) {
            blogManager.updatePost(blogManager.editingPostId, postData);
            alert('Artikel erfolgreich aktualisiert!');
        } else {
            blogManager.addPost(postData);
            alert('Artikel erfolgreich erstellt!');
        }

        postEditor.style.display = 'none';
        postForm.reset();
        blogManager.editingPostId = null;
        renderAdminPosts();
        renderBlog();
    });
}

function getPostFormData() {
    const title = document.getElementById('postTitle').value;
    const alias = document.getElementById('postAlias').value;
    const author = document.getElementById('postAuthor').value;
    const years = document.getElementById('postYears').value;
    const location = document.getElementById('postLocation').value;
    const victims = parseInt(document.getElementById('postVictims').value);
    const status = document.getElementById('postStatus').value;
    const category = document.getElementById('postCategory').value;
    const tags = document.getElementById('postTags').value.split(',').map(t => t.trim()).filter(t => t);
    const image = document.getElementById('postImage').value || 'https://images.unsplash.com/photo-1568602471122-7832951cc4c5?w=800';
    const excerpt = document.getElementById('postExcerpt').value;
    const content = document.getElementById('postContent').value;
    const mo = document.getElementById('postMO').value;
    const investigation = document.getElementById('postInvestigation').value;

    if (!title || !years || !location || !victims || !category || !excerpt || !content) {
        alert('Bitte füllen Sie alle Pflichtfelder aus!');
        return null;
    }

    return {
        title, alias, author, years, location, victims, status,
        category, tags, image, excerpt, content, mo, investigation
    };
}

function renderCategoryOptions() {
    const categorySelect = document.getElementById('postCategory');
    categorySelect.innerHTML = blogManager.categories.map(cat => `
        <option value="${cat.name}">${cat.name}</option>
    `).join('');
}

function renderAdminPosts() {
    const postsTable = document.getElementById('postsTable');
    
    if (blogManager.posts.length === 0) {
        postsTable.innerHTML = '<p class="empty-state-text">Noch keine Artikel vorhanden</p>';
        return;
    }

    postsTable.innerHTML = `
        <table class="admin-table">
            <thead>
                <tr>
                    <th>Titel</th>
                    <th>Kategorie</th>
                    <th>Datum</th>
                    <th>Aktionen</th>
                </tr>
            </thead>
            <tbody>
                ${blogManager.posts.map(post => `
                    <tr>
                        <td>${post.title}</td>
                        <td>${post.category}</td>
                        <td>${blogManager.formatDate(post.date)}</td>
                        <td class="admin-table-actions">
                            <button class="btn btn-secondary btn-small" onclick="editPost('${post.id}')">Bearbeiten</button>
                            <button class="btn btn-danger btn-small" onclick="deletePost('${post.id}')">Löschen</button>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

function editPost(id) {
    const post = blogManager.getPostById(id);
    if (!post) return;

    blogManager.editingPostId = id;
    document.getElementById('editorTitle').textContent = 'Artikel bearbeiten';
    document.getElementById('postTitle').value = post.title;
    document.getElementById('postAlias').value = post.alias;
    document.getElementById('postAuthor').value = post.author;
    document.getElementById('postYears').value = post.years;
    document.getElementById('postLocation').value = post.location;
    document.getElementById('postVictims').value = post.victims;
    document.getElementById('postStatus').value = post.status;
    renderCategoryOptions();
    document.getElementById('postCategory').value = post.category;
    document.getElementById('postTags').value = post.tags.join(', ');
    document.getElementById('postImage').value = post.image;
    document.getElementById('postExcerpt').value = post.excerpt;
    document.getElementById('postContent').value = post.content;
    document.getElementById('postMO').value = post.mo || '';
    document.getElementById('postInvestigation').value = post.investigation || '';

    document.getElementById('postEditor').style.display = 'block';
    document.getElementById('postEditor').scrollIntoView({ behavior: 'smooth' });
}

function deletePost(id) {
    if (confirm('Möchten Sie diesen Artikel wirklich löschen?')) {
        blogManager.deletePost(id);
        renderAdminPosts();
        renderBlog();
        alert('Artikel erfolgreich gelöscht!');
    }
}

function initCategoryEditor() {
    const addNewCategory = document.getElementById('addNewCategory');
    const categoryEditor = document.getElementById('categoryEditor');
    const categoryForm = document.getElementById('categoryForm');
    const cancelCategory = document.getElementById('cancelCategory');

    addNewCategory.addEventListener('click', () => {
        blogManager.editingCategoryId = null;
        categoryForm.reset();
        categoryEditor.style.display = 'block';
    });

    cancelCategory.addEventListener('click', () => {
        categoryEditor.style.display = 'none';
        categoryForm.reset();
        blogManager.editingCategoryId = null;
    });

    categoryForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const name = document.getElementById('categoryName').value;
        const description = document.getElementById('categoryDescription').value;

        if (blogManager.editingCategoryId) {
            blogManager.updateCategory(blogManager.editingCategoryId, { name, description });
            alert('Kategorie erfolgreich aktualisiert!');
        } else {
            blogManager.addCategory({ name, description });
            alert('Kategorie erfolgreich erstellt!');
        }

        categoryEditor.style.display = 'none';
        categoryForm.reset();
        blogManager.editingCategoryId = null;
        renderAdminCategories();
        renderSidebar();
    });
}

function renderAdminCategories() {
    const categoriesList = document.getElementById('categoriesList');
    
    if (blogManager.categories.length === 0) {
        categoriesList.innerHTML = '<p class="empty-state-text">Noch keine Kategorien vorhanden</p>';
        return;
    }

    categoriesList.innerHTML = blogManager.categories.map(cat => `
        <div class="category-item">
            <div>
                <strong>${cat.name}</strong>
                <p style="color: var(--text-gray); font-size: 0.9rem;">${cat.description || ''}</p>
            </div>
            <div class="item-actions">
                <button class="btn btn-secondary btn-small" onclick="editCategory('${cat.id}')">Bearbeiten</button>
                <button class="btn btn-danger btn-small" onclick="deleteCategory('${cat.id}')">Löschen</button>
            </div>
        </div>
    `).join('');
}

function editCategory(id) {
    const category = blogManager.categories.find(c => c.id === id);
    if (!category) return;

    blogManager.editingCategoryId = id;
    document.getElementById('categoryName').value = category.name;
    document.getElementById('categoryDescription').value = category.description || '';
    document.getElementById('categoryEditor').style.display = 'block';
}

function deleteCategory(id) {
    if (confirm('Möchten Sie diese Kategorie wirklich löschen?')) {
        blogManager.deleteCategory(id);
        renderAdminCategories();
        renderSidebar();
        alert('Kategorie erfolgreich gelöscht!');
    }
}

function initTagEditor() {
    const addNewTag = document.getElementById('addNewTag');
    const tagEditor = document.getElementById('tagEditor');
    const tagForm = document.getElementById('tagForm');
    const cancelTag = document.getElementById('cancelTag');

    addNewTag.addEventListener('click', () => {
        blogManager.editingTagId = null;
        tagForm.reset();
        tagEditor.style.display = 'block';
    });

    cancelTag.addEventListener('click', () => {
        tagEditor.style.display = 'none';
        tagForm.reset();
        blogManager.editingTagId = null;
    });

    tagForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const name = document.getElementById('tagName').value;

        if (blogManager.editingTagId) {
            blogManager.updateTag(blogManager.editingTagId, { name });
            alert('Tag erfolgreich aktualisiert!');
        } else {
            blogManager.addTag({ name });
            alert('Tag erfolgreich erstellt!');
        }

        tagEditor.style.display = 'none';
        tagForm.reset();
        blogManager.editingTagId = null;
        renderAdminTags();
        renderSidebar();
    });
}

function renderAdminTags() {
    const tagsList = document.getElementById('tagsList');
    
    if (blogManager.tags.length === 0) {
        tagsList.innerHTML = '<p class="empty-state-text">Noch keine Tags vorhanden</p>';
        return;
    }

    tagsList.innerHTML = blogManager.tags.map(tag => `
        <div class="tag-item">
            <strong>${tag.name}</strong>
            <div class="item-actions">
                <button class="btn btn-secondary btn-small" onclick="editTag('${tag.id}')">Bearbeiten</button>
                <button class="btn btn-danger btn-small" onclick="deleteTag('${tag.id}')">Löschen</button>
            </div>
        </div>
    `).join('');
}

function editTag(id) {
    const tag = blogManager.tags.find(t => t.id === id);
    if (!tag) return;

    blogManager.editingTagId = id;
    document.getElementById('tagName').value = tag.name;
    document.getElementById('tagEditor').style.display = 'block';
}

function deleteTag(id) {
    if (confirm('Möchten Sie diesen Tag wirklich löschen?')) {
        blogManager.deleteTag(id);
        renderAdminTags();
        renderSidebar();
        alert('Tag erfolgreich gelöscht!');
    }
}

function initSettings() {
    const settingsForm = document.getElementById('settingsForm');
    const exportData = document.getElementById('exportData');
    const importData = document.getElementById('importData');
    const clearData = document.getElementById('clearData');

    settingsForm.addEventListener('submit', (e) => {
        e.preventDefault();
        blogManager.settings.siteName = document.getElementById('siteName').value;
        blogManager.settings.siteDescription = document.getElementById('siteDescription').value;
        blogManager.settings.postsPerPage = parseInt(document.getElementById('postsPerPage').value);
        blogManager.saveSettings();
        blogManager.postsPerPage = blogManager.settings.postsPerPage;
        alert('Einstellungen erfolgreich gespeichert!');
        renderBlog();
    });

    exportData.addEventListener('click', () => {
        const data = blogManager.exportData();
        const blob = new Blob([data], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `blog-backup-${new Date().toISOString().split('T')[0]}.json`;
        a.click();
        URL.revokeObjectURL(url);
    });

    importData.addEventListener('click', () => {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = 'application/json';
        input.onchange = (e) => {
            const file = e.target.files[0];
            if (file) {
                const reader = new FileReader();
                reader.onload = (event) => {
                    if (blogManager.importData(event.target.result)) {
                        alert('Daten erfolgreich importiert!');
                        renderBlog();
                        renderAdminDashboard();
                    } else {
                        alert('Fehler beim Importieren der Daten!');
                    }
                };
                reader.readAsText(file);
            }
        };
        input.click();
    });

    clearData.addEventListener('click', () => {
        if (confirm('Möchten Sie wirklich ALLE Daten löschen? Diese Aktion kann nicht rückgängig gemacht werden!')) {
            if (confirm('Sind Sie sicher? Alle Artikel, Kategorien und Tags werden gelöscht!')) {
                blogManager.clearAllData();
                alert('Alle Daten wurden gelöscht!');
                location.reload();
            }
        }
    });
}

// ===== FILTERS =====
function initFilters() {
    const categoryFilter = document.getElementById('categoryFilter');
    const blogSearch = document.getElementById('blogSearch');

    if (categoryFilter) {
        categoryFilter.innerHTML = '<option value="">Alle Kategorien</option>' +
            blogManager.categories.map(cat => `<option value="${cat.name}">${cat.name}</option>`).join('');

        categoryFilter.addEventListener('change', (e) => {
            blogManager.currentFilter.category = e.target.value;
            blogManager.currentPage = 1;
            renderBlogPosts();
            renderPagination();
        });
    }

    if (blogSearch) {
        blogSearch.addEventListener('input', (e) => {
            blogManager.currentFilter.search = e.target.value;
            blogManager.currentPage = 1;
            renderBlogPosts();
            renderPagination();
        });
    }
}

// ===== INITIALIZATION =====
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initAdmin();
    initFilters();
    renderBlog();
});

// ===== GLOBAL FUNCTIONS =====
window.showSinglePost = showSinglePost;
window.backToBlog = backToBlog;
window.changePage = changePage;
window.filterByCategory = filterByCategory;
window.filterBySearch = filterBySearch;
window.editPost = editPost;
window.deletePost = deletePost;
window.editCategory = editCategory;
window.deleteCategory = deleteCategory;
window.editTag = editTag;
window.deleteTag = deleteTag;

