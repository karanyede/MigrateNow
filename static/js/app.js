/**
 * MigrateNow — Client-side helpers
 *
 * Modules:
 *  - initToasts()          — flash messages → auto-dismiss toast notifications
 *  - initStatCounters()    — animated count-up for dashboard stat cards
 *  - initSidebarState()    — highlight the active nav item from URL
 *  - initTableSearch()     — live AJAX table/object search on Tables page
 *  - initFieldFilter()     — field name filter on Fields page
 *  - initMappingAutoCheck()— auto-check checkbox when a mapping is selected
 *  - initFormGuards()      — prevent double-submit on forms
 */

document.addEventListener('DOMContentLoaded', () => {
    initToasts();
    initStatCounters();
    initSidebarState();
    initTableSearch();
    initFieldFilter();
    initMappingAutoCheck();
    initFormGuards();
});

// ─── Toasts ──────────────────────────────────────────────────────
function initToasts() {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    // Read flash data elements injected by the template
    document.querySelectorAll('.flash-data').forEach(el => {
        const category = el.dataset.category || 'info';
        const message  = el.dataset.message  || '';
        showToast(message, category, container);
        el.remove();
    });
}

function showToast(message, category, container) {
    if (!container) container = document.getElementById('toastContainer');
    if (!container) return;

    const iconMap = {
        error:   `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m9-.75a9 9 0 1 1-18 0 9 9 0 0 1 18 0Zm-9 3.75h.008v.008H12v-.008Z"/></svg>`,
        success: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75 11.25 15 15 9.75M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z"/></svg>`,
        info:    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="m11.25 11.25.041-.02a.75.75 0 0 1 1.063.852l-.708 2.836a.75.75 0 0 0 1.063.853l.041-.021M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Zm-9-3.75h.008v.008H12V8.25Z"/></svg>`,
    };

    const toast = document.createElement('div');
    toast.className = `toast toast--${category}`;
    toast.setAttribute('role', 'alert');
    toast.innerHTML = (iconMap[category] || iconMap.info) + `<span>${message}</span>`;
    container.appendChild(toast);

    // Auto-dismiss after 4s
    setTimeout(() => {
        toast.classList.add('toast--dismiss');
        toast.addEventListener('animationend', () => toast.remove(), { once: true });
    }, 4000);
}

// ─── Stat Count-Up Animation ─────────────────────────────────────
function initStatCounters() {
    const cards = document.querySelectorAll('[data-count]');
    if (!cards.length) return;

    function easeOutCubic(t) { return 1 - Math.pow(1 - t, 3); }

    function animateCounter(el, target, suffix) {
        const duration = 1000;
        const start = Date.now();

        function tick() {
            const elapsed  = Date.now() - start;
            const progress = Math.min(elapsed / duration, 1);
            const value    = Math.round(target * easeOutCubic(progress));
            el.textContent = value.toLocaleString() + (suffix || '');
            if (progress < 1) requestAnimationFrame(tick);
        }
        requestAnimationFrame(tick);
    }

    // Use IntersectionObserver to trigger on-scroll (or immediately if visible)
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const el     = entry.target;
                const target = parseInt(el.dataset.count, 10) || 0;
                const suffix = el.dataset.suffix || '';
                if (target > 0) animateCounter(el, target, suffix);
                observer.unobserve(el);
            }
        });
    }, { threshold: 0.3 });

    cards.forEach(card => observer.observe(card));
}

// ─── Sidebar Active State ─────────────────────────────────────────
function initSidebarState() {
    const path = window.location.pathname;
    const navMap = {
        '/': 'nav-dashboard',
        '/history': 'nav-history',
    };

    const activeId = navMap[path];
    if (activeId) {
        const el = document.getElementById(activeId);
        if (el) el.classList.add('active');
    }
}

// ─── Live Table / Object Search (Tables page) ────────────────────
// Note: The enhanced dropdown UI is implemented inline in tables.html.
// This handles the legacy <select>-based search for any fallback cases.
function initTableSearch() {
    document.querySelectorAll('#sourceSearch, #targetSearch').forEach(input => {
        // If the input already has a search-dropdown sibling (new UI), skip
        if (input.nextElementSibling && input.nextElementSibling.classList.contains('search-dropdown')) {
            return; // handled by inline script in tables.html
        }

        const selectId   = input.dataset.select;
        const instance   = input.dataset.instance;
        const searchType = input.dataset.searchType || 'sn';
        const select     = document.getElementById(selectId);
        if (!select) return;

        let timer = null;

        input.addEventListener('input', () => {
            clearTimeout(timer);
            const q = input.value.trim();

            if (q.length < 2) {
                select.innerHTML = '<option value="">— Type at least 2 characters —</option>';
                return;
            }

            select.innerHTML = '<option value="">Searching…</option>';

            timer = setTimeout(() => {
                const apiUrl = searchType === 'sf'
                    ? `/api/search_objects?q=${encodeURIComponent(q)}&instance=${instance}`
                    : `/api/search_tables?q=${encodeURIComponent(q)}&instance=${instance}`;

                fetch(apiUrl)
                    .then(res => res.json())
                    .then(tables => {
                        if (tables.error) {
                            select.innerHTML = `<option value="">Error: ${tables.error}</option>`;
                            return;
                        }
                        if (tables.length === 0) {
                            select.innerHTML = `<option value="">No ${searchType === 'sf' ? 'objects' : 'tables'} found</option>`;
                            return;
                        }
                        const lowerQ = q.toLowerCase();
                        tables.sort((a, b) => {
                            const aEx = a.name.toLowerCase() === lowerQ ? 0 : 1;
                            const bEx = b.name.toLowerCase() === lowerQ ? 0 : 1;
                            if (aEx !== bEx) return aEx - bEx;
                            return a.name.localeCompare(b.name);
                        });

                        let html = `<option value="">— ${tables.length} result(s) —</option>`;
                        tables.forEach(t => {
                            const label = t.label && t.label !== t.name ? `${t.name} — ${t.label}` : t.name;
                            html += `<option value="${t.name}">${label}</option>`;
                        });
                        select.innerHTML = html;

                        const exact = tables.find(t => t.name.toLowerCase() === lowerQ);
                        if (exact) select.value = exact.name;
                    })
                    .catch(() => {
                        select.innerHTML = '<option value="">Search failed</option>';
                    });
            }, 350);
        });
    });
}

// ─── Field Filter (Fields page) ───────────────────────────────────
function initFieldFilter() {
    // The field filter is now handled inline in fields.html for richer logic.
    // This is kept as a fallback for any standalone usage.
    const fieldFilter = document.getElementById('fieldFilter');
    if (!fieldFilter) return;

    // Check if the inline script has already attached a listener
    if (fieldFilter.dataset.initialized) return;
    fieldFilter.dataset.initialized = 'true';

    const rows = document.querySelectorAll('#mappingTable tbody tr');
    fieldFilter.addEventListener('input', () => {
        const q = fieldFilter.value.toLowerCase().trim();
        rows.forEach(row => {
            const name  = row.dataset.sourceField || '';
            const label = row.querySelector('.field-label')?.textContent || '';
            row.style.display = (!q || name.toLowerCase().includes(q) || label.toLowerCase().includes(q)) ? '' : 'none';
        });
    });
}

// ─── Auto-check Include on Mapping Select ───────────────────────
function initMappingAutoCheck() {
    document.querySelectorAll('.mapping-select').forEach(sel => {
        if (sel.dataset.autoCheckInit) return;
        sel.dataset.autoCheckInit = 'true';

        sel.addEventListener('change', () => {
            const row   = sel.closest('tr');
            if (!row) return;
            const cb    = row.querySelector('.field-include-cb');
            const badge = row.querySelector('.mapping-badge');
            if (sel.value) {
                if (cb) cb.checked = true;
                if (badge && !badge.classList.contains('mapping-badge--auto')) {
                    badge.className = 'mapping-badge mapping-badge--manual';
                    badge.textContent = 'Manual';
                }
            } else {
                if (badge && !badge.classList.contains('mapping-badge--auto')) {
                    badge.className = 'mapping-badge mapping-badge--unmapped';
                    badge.textContent = 'Unmapped';
                }
            }
        });
    });
}

// ─── Form Submission Guards ──────────────────────────────────────
function initFormGuards() {
    document.querySelectorAll('form').forEach(form => {
        form.addEventListener('submit', () => {
            const btn = form.querySelector('button[type="submit"]');
            if (btn && !btn.dataset.guardApplied) {
                btn.dataset.guardApplied = 'true';
                btn.disabled = true;
                const originalContent = btn.innerHTML;
                btn.innerHTML = '<span class="spinner"></span> Processing…';
                // Re-enable after 30s as a safety net
                setTimeout(() => {
                    btn.disabled = false;
                    btn.innerHTML = originalContent;
                    delete btn.dataset.guardApplied;
                }, 30000);
            }
        });
    });
}
