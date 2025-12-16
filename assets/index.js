const auth = document.getElementById('auth')
const licenseInput = document.getElementById('license-input');
const licenseCheck = document.getElementById('license-check');
const licenseStatus = document.getElementById('license-status');
const dashboard = document.getElementById('dashboard');
const runQuery = document.getElementById('run-query');
const tableBody = document.querySelector('#table tbody');
const results = document.getElementById('results');
let licenseKey = sessionStorage.getItem('ARMS-License-Key');
let chart = null;
const rangeHeader = document.getElementById('rangeHeader');
const rangeText = document.getElementById('rangeText');
const exportBtn = document.getElementById('export-csv');


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
            sessionStorage.setItem('ARMS-License-Key', key)
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

async function loadGainers() {
    if (!licenseKey) {
        setStatus('Validate a license first');
        return;
    }
    setLoading(true)
    console.log(document.getElementById('category').value)
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
    renderTimeRange(data.start_date_used, data.end_date_used);
    const rows = data.results || [];
    exportBtn.onclick = () => {
        exportTableToCSV('drivers_scout_gainers.csv', rows);
    };
    if (rows.length) {
        results.hidden = false;
    }
    renderChart(rows);
    renderTable(rows);
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

    rows.slice(0, 20).forEach((r, idx) => {
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

        const flag = isoToFlagEmoji(r.location || '');

        tr.innerHTML = `
      <td>${idx + 1}</td>
      <td>
        ${flag ? `<span class="flag">${flag}</span>` : ''}
        ${r.location || '—'}
      </td>
      <td>${r.driver || 'N/A'}</td>
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

licenseCheck.addEventListener('click', () => checkLicense(licenseInput.value.trim()));
runQuery.addEventListener('click', loadGainers);

document.getElementById('year').textContent = new Date().getFullYear();
checkLicense(licenseKey);

