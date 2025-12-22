const FILTERS_STORAGE_KEY = 'drivers-scout-filters';

const auth = document.getElementById('auth')
const licenseInput = document.getElementById('license-input');
const licenseCheck = document.getElementById('license-check');
const licenseStatus = document.getElementById('license-status');
const dashboard = document.getElementById('dashboard');
const runQuery = document.getElementById('run-query');
const tableBody = document.querySelector('#table tbody');
const results = document.getElementById('results');
const LICENSE_STORAGE_KEY = 'ARMS-License-Key';
const LICENSE_DB_NAME = 'drivers-scout';
const LICENSE_STORE = 'settings';
let licenseKey = null;
let chart = null;
const rangeHeader = document.getElementById('rangeHeader');
const rangeText = document.getElementById('rangeText');
const exportBtn = document.getElementById('export-csv');
const paginationPrev = document.getElementById('pagination-prev');
const paginationNext = document.getElementById('pagination-next');
const paginationStatus = document.getElementById('pagination-status');
const pageSizeSelect = document.getElementById('page-size');

let lastRows = [];
let displayRows = [];
let sortState = { key: null, dir: 'desc' };
let currentPage = 1;
let pageSize = 20;
let paginationRowsRef = null;

function openLicenseDb() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(LICENSE_DB_NAME, 1);
        request.onupgradeneeded = event => {
            const db = event.target.result;
            if (!db.objectStoreNames.contains(LICENSE_STORE)) {
                db.createObjectStore(LICENSE_STORE);
            }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
}

async function getStoredLicenseKey() {
    const db = await openLicenseDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(LICENSE_STORE, 'readonly');
        const store = tx.objectStore(LICENSE_STORE);
        const request = store.get(LICENSE_STORAGE_KEY);
        request.onsuccess = () => resolve(request.result || null);
        request.onerror = () => reject(request.error);
        tx.oncomplete = () => db.close();
        tx.onerror = () => reject(tx.error);
    });
}

async function setStoredLicenseKey(key) {
    const db = await openLicenseDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(LICENSE_STORE, 'readwrite');
        const store = tx.objectStore(LICENSE_STORE);
        const request = store.put(key, LICENSE_STORAGE_KEY);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
        tx.oncomplete = () => db.close();
        tx.onerror = () => reject(tx.error);
    });
}

function sortRows(rows, key, dir) {
    const factor = dir === 'asc' ? 1 : -1;

    return [...rows].sort((a, b) => {
        let va, vb;

        if (key === 'rank') {
            va = rows.indexOf(a);
            vb = rows.indexOf(b);
        } else {
            va = a[key];
            vb = b[key];
        }

        if (va == null) return 1;
        if (vb == null) return -1;

        if (typeof va === 'number' && typeof vb === 'number') {
            return (va - vb) * factor;
        }

        return String(va).localeCompare(String(vb)) * factor;
    });
}

function formatIsoDateHuman(isoDate) {
    if (!isoDate) return null;
    // "YYYY-MM-DD" -> force UTC to avoid timezone shifting
    const d = new Date(`${isoDate}T00:00:00Z`);
    if (Number.isNaN(d.getTime())) return null;
    return new Intl.DateTimeFormat(navigator.language || 'en-US', {
        year: 'numeric', month: 'long', day: '2-digit', timeZone: 'UTC'
    }).format(d);
}

function renderTimeRange(startIso, endIso) {
    const start = formatIsoDateHuman(startIso);
    const end = formatIsoDateHuman(endIso);

    if (!start && !end) {
        rangeHeader.hidden = true;
        rangeText.textContent = '';
        return;
    }

    rangeHeader.hidden = false;
    rangeText.textContent = start && end ? `${start} to ${end}` : (start || end);
}

function setStatus(msg, ok = false) {
    licenseStatus.textContent = msg;
    licenseStatus.className = 'status ' + (ok ? 'ok' : 'err');
}

async function checkLicense(key) {
    if (!key) {
        setStatus('Enter a license key');
        return;
    }
    setStatus('Checking...');
    try {
        const res = await fetch(`/drivers-scout/api/licenses/${encodeURIComponent(key)}/status`);

        if (res.status === 502) {
            setStatus('Server not reachable. Try again later.');
            return;
        }

        if (!res.ok) {
            setStatus('Could not verify license.');
            return;
        }

        const data = await res.json();
        if (data.valid && data.active) {
            licenseKey = key;
            dashboard.classList.remove('hidden');
            setStatus('License active. Dashboard unlocked.', true);
            await setStoredLicenseKey(key);
            auth.hidden = true;
        } else {
            dashboard.classList.add('hidden');
            setStatus('License invalid or inactive.');
        }
    } catch (err) {
        console.error(err);
        setStatus('Could not verify license.');
    }
}

function setLoading(isLoading) {
    runQuery.disabled = isLoading
    runQuery.innerHTML = isLoading ? "Loading..." : "Load Gainers"
}

function getPagedRows(rows) {
    const start = (currentPage - 1) * pageSize;
    return rows.slice(start, start + pageSize);
}

function renderPagination(rows) {
    if (paginationRowsRef !== rows) {
        currentPage = 1;
        paginationRowsRef = rows;
    }

    const totalPages = rows.length ? Math.ceil(rows.length / pageSize) : 1;
    if (currentPage > totalPages) currentPage = totalPages;

    const displayPage = rows.length ? currentPage : 0;
    const displayTotal = rows.length ? totalPages : 0;
    paginationStatus.textContent = `Page ${displayPage} of ${displayTotal}`;

    paginationPrev.disabled = currentPage <= 1 || rows.length === 0;
    paginationNext.disabled = currentPage >= totalPages || rows.length === 0;
    pageSizeSelect.value = String(pageSize);
}

async function loadGainers() {
    if (!licenseKey) {
        setStatus('Validate a license first');
        return;
    }
    setLoading(true)
    const params = new URLSearchParams();
    params.set('category', document.getElementById('category').value || 'sports_car');
    params.set('days', document.getElementById('days').value || 30);
    params.set('limit', document.getElementById('limit').value || 20);
    const minIr = document.getElementById('minIr').value;
    if (minIr) params.set('min_current_irating', minIr);

    const res = await fetch(`/drivers-scout/api/leaders/growers?${params.toString()}`, {
        headers: {'X-License-Key': licenseKey}
    });
    if (!res.ok) {
        setLoading(false)
        alert('Request failed');
        return;
    }
    const data = await res.json();
    lastRows = data.results || []
    renderTimeRange(data.start_date_used, data.end_date_used);
    const rows = data.results || [];
    displayRows = rows;
    exportBtn.onclick = () => {
        exportTableToCSV('drivers_scout_gainers.csv', rows);
    };
    if (rows.length) {
        results.hidden = false;
    }
    renderChart(rows);
    renderPagination(displayRows);
    renderTable(displayRows);
    setLoading(false)
}

function renderChart(rows) {
    const ctx = document.getElementById('chart');
    const points = rows.map(r => ({
        x: r.end_value,
        y: r.delta,
        label: r.driver || `#${r.cust_id}`,
        data: r
    }));
    if (chart) chart.destroy();
    chart = new Chart(ctx, {
        type: 'scatter',
        data: {datasets: [{label: 'Gainers', data: points, backgroundColor: '#38bdf8'}]},
        options: {
            scales: {
                x: {title: {display: true, text: 'Current iRating'}},
                y: {title: {display: true, text: 'Δ iRating'}}
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: ctx => {
                            const d = ctx.raw.data;
                            return [
                                `Driver: ${d.driver || 'N/A'}`,
                                `Current iRating: ${d.end_value}`,
                                `Δ iRating: ${d.delta}`,
                                `Starts: ${d.starts ?? 'N/A'}`,
                                `Wins: ${d.wins ?? 'N/A'}`
                            ];
                        }
                    }
                }
            },
            hover: {mode: 'nearest', intersect: true}
        }
    });
}

function renderTable(rows) {
    tableBody.innerHTML = '';

    const smurfRegex = /\d/; // any digit anywhere in the name
    const pagedRows = getPagedRows(rows);
    const startIndex = (currentPage - 1) * pageSize;

    pagedRows.forEach((r, idx) => {
        const tr = document.createElement('tr');

        // Podium classes (only if not smurf; smurf should win visually)
        if (idx === 0) tr.classList.add('podium-1');
        if (idx === 1) tr.classList.add('podium-2');
        if (idx === 2) tr.classList.add('podium-3');

        const name = (r.driver || '').trim();
        const isSmurf = name && smurfRegex.test(name);

        if (isSmurf) {
            tr.classList.add('smurf');
            tr.title = 'Potential Smurf';
            // optional: remove podium styling when smurf
            tr.classList.remove('podium-1', 'podium-2', 'podium-3');
        }

        const flag = r.location.toLowerCase()

        tr.innerHTML = `
      <td>${startIndex + idx + 1}</td>
      <td><div class="driver"><img class="flag"
          src="https://flagcdn.com/20x15/${flag}.png"
          srcset="https://flagcdn.com/40x30/${flag}.png 2x,
          https://flagcdn.com/60x45/${flag}.png 3x"
          height="15" width="20" alt=""/>${r.driver || 'N/A'}</div></td>
      <td>${r.end_value}</td>
      <td>${r.delta}</td>
      <td>${r.percent_change ? r.percent_change.toFixed(2) + '%' : '—'}</td>
      <td>${r.starts ?? '—'}</td>
      <td>${r.wins ?? '—'}</td>`;

        tableBody.appendChild(tr);
    });
}

function exportTableToCSV(filename, rows) {
    if (!rows || !rows.length) return;

    const headers = [
        '#',
        'Driver',
        'Current iRating',
        'Delta iRating',
        'Percent Change',
        'Starts',
        'Wins',
        'Location'
    ];

    const csvRows = [];
    csvRows.push(headers.join(','));

    rows.forEach((r, idx) => {
        csvRows.push([
            idx + 1,
            `"${r.driver || 'N/A'}"`,
            r.end_value,
            r.delta,
            r.percent_change ? r.percent_change.toFixed(2) : '',
            r.starts ?? '',
            r.wins ?? '',
            `"${r.location || ''}"`
        ].join(','));
    });

    const blob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);

    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();

    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}

function isoToFlagEmoji(isoCode) {
    if (!isoCode || isoCode.length !== 2) return '';
    return isoCode
        .toUpperCase()
        .replace(/./g, char =>
            String.fromCodePoint(127397 + char.charCodeAt())
        );
}

function saveFiltersToSession() {
    const filters = {
        category: document.getElementById('category').value,
        days: document.getElementById('days').value,
        limit: document.getElementById('limit').value,
        minIr: document.getElementById('minIr').value
    };
    localStorage.setItem(FILTERS_STORAGE_KEY, JSON.stringify(filters));
}

function loadFiltersFromSession() {
    const raw = localStorage.getItem(FILTERS_STORAGE_KEY);
    if (!raw) return;

    try {
        const filters = JSON.parse(raw);
        if (filters.category) document.getElementById('category').value = filters.category;
        if (filters.days) document.getElementById('days').value = filters.days;
        if (filters.limit) document.getElementById('limit').value = filters.limit;
        if (filters.minIr !== undefined) document.getElementById('minIr').value = filters.minIr;
    } catch (e) {
        console.warn('Failed to load filters from session storage', e);
    }
}

function getNextTuesdayUtc(fromDate = new Date()) {
    const utcDay = fromDate.getUTCDay();
    const isTuesday = utcDay === 2;
    const pastMidnight =
        fromDate.getUTCHours() > 0 ||
        fromDate.getUTCMinutes() > 0 ||
        fromDate.getUTCSeconds() > 0 ||
        fromDate.getUTCMilliseconds() > 0;
    let daysUntil = (2 - utcDay + 7) % 7;

    if (isTuesday && pastMidnight) {
        daysUntil = 7;
    }

    return new Date(Date.UTC(
        fromDate.getUTCFullYear(),
        fromDate.getUTCMonth(),
        fromDate.getUTCDate() + daysUntil,
        0, 0, 0, 0
    ));
}

function startNextTuesdayCountdown() {
    const countdown = document.getElementById('next-tuesday-countdown');
    if (!countdown) return;

    const valueNodes = {
        days: countdown.querySelector('[data-unit="days"]'),
        hours: countdown.querySelector('[data-unit="hours"]'),
        minutes: countdown.querySelector('[data-unit="minutes"]'),
        seconds: countdown.querySelector('[data-unit="seconds"]')
    };

    if (!valueNodes.days || !valueNodes.hours || !valueNodes.minutes || !valueNodes.seconds) {
        return;
    }

    let nextTarget = getNextTuesdayUtc();

    const render = deltaMs => {
        const totalSeconds = Math.max(0, Math.floor(deltaMs / 1000));
        const days = Math.floor(totalSeconds / 86400);
        const hours = Math.floor((totalSeconds % 86400) / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;

        valueNodes.days.textContent = String(days);
        valueNodes.hours.textContent = String(hours).padStart(2, '0');
        valueNodes.minutes.textContent = String(minutes).padStart(2, '0');
        valueNodes.seconds.textContent = String(seconds).padStart(2, '0');
    };

    const tick = () => {
        const now = new Date();
        const delta = nextTarget.getTime() - now.getTime();

        if (delta <= 0) {
            render(0);
            nextTarget = getNextTuesdayUtc(new Date(now.getTime() + 1000));
            return;
        }

        render(delta);
    };

    tick();
    setInterval(tick, 1000);
}

const TAB_KEY = 'drivers-scout-active-tab';

function setActiveTab(tabId) {
    localStorage.setItem(TAB_KEY, tabId);

    document.querySelectorAll('.tab-panel').forEach(p => p.classList.add('hidden'));
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));

    document.getElementById(tabId).classList.remove('hidden');
    document.querySelector(`.tab-btn[data-tab="${tabId}"]`).classList.add('active');
}

setActiveTab(localStorage.getItem(TAB_KEY) || 'tab-search');
startNextTuesdayCountdown();

document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => setActiveTab(btn.dataset.tab));
});

licenseCheck.addEventListener('click', () => checkLicense(licenseInput.value.trim()));
runQuery.addEventListener('click', () => {
    saveFiltersToSession()
    loadGainers()
});

document.querySelectorAll('th.sortable').forEach(th => {
    th.addEventListener('click', () => {
        const key = th.dataset.key;

        // toggle direction
        sortState.dir =
            sortState.key === key && sortState.dir === 'desc' ? 'asc' : 'desc';
        sortState.key = key;

        // update header classes
        document.querySelectorAll('th.sortable').forEach(h => {
            h.classList.remove('asc', 'desc');
        });
        th.classList.add(sortState.dir);

        const sorted = sortRows(lastRows, key, sortState.dir);
        displayRows = sorted;
        renderPagination(sorted);
        renderTable(sorted);
    });
});

paginationPrev.addEventListener('click', () => {
    if (currentPage > 1) {
        currentPage -= 1;
        renderTable(displayRows);
        renderPagination(displayRows);
    }
});

paginationNext.addEventListener('click', () => {
    const totalPages = displayRows.length ? Math.ceil(displayRows.length / pageSize) : 1;
    if (currentPage < totalPages) {
        currentPage += 1;
        renderTable(displayRows);
        renderPagination(displayRows);
    }
});

pageSizeSelect.addEventListener('change', event => {
    const nextSize = Number(event.target.value);
    if (!Number.isNaN(nextSize) && nextSize > 0) {
        pageSize = nextSize;
        currentPage = 1;
        renderPagination(displayRows);
        renderTable(displayRows);
    }
});

document.getElementById('year').textContent = new Date().getFullYear();

async function initLicense() {
    try {
        licenseKey = await getStoredLicenseKey();
        if (licenseKey) {
            licenseInput.value = licenseKey;
            await checkLicense(licenseKey);
        } else {
            setStatus('Enter a license key');
        }
    } catch (error) {
        console.error('Failed to load stored license key', error);
        setStatus('Enter a license key');
    }
}

loadFiltersFromSession();
initLicense();
