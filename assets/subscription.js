const subStatus = document.getElementById('sub-status');
const subWebhookUrl = document.getElementById('sub-webhook-url');
const subCategory = document.getElementById('sub-category');
const subMinIr = document.getElementById('sub-min-ir');
const subSave = document.getElementById('sub-save');
const subscriptionTableBody = document.querySelector('#subscription-table tbody');
const subscriptionTableState = document.getElementById('subscription-table-state');
const webhookTabBtn = document.getElementById('tab-webhook-btn');
const webhookTabPanel = document.getElementById('tab-webhook');

function setSubStatus(msg, ok=false) {
    subStatus.textContent = msg;
    subStatus.className = 'status ' + (ok ? 'ok' : 'err');
}

function isValidUrl(value) {
    try { new URL(value); return true; } catch { return false; }
}

function setSubscriptionTableState(message) {
    subscriptionTableState.textContent = message;
}

function renderSubscriptions(rows) {
    subscriptionTableBody.innerHTML = '';

    if (!rows.length) {
        setSubscriptionTableState('No subscriptions found.');
        return;
    }

    setSubscriptionTableState(`Showing ${rows.length} subscription${rows.length === 1 ? '' : 's'}.`);

    rows.forEach(row => {
        const tr = document.createElement('tr');
        const minIr = Number.isFinite(row.min_irating) ? row.min_irating : '—';
        tr.innerHTML = `
            <td>${row.id}</td>
            <td>${row.webhook_url}</td>
            <td>${row.category}</td>
            <td>${minIr}</td>
        `;

        const actionCell = document.createElement('td');
        const deleteBtn = document.createElement('button');
        deleteBtn.type = 'button';
        deleteBtn.textContent = 'Delete';
        deleteBtn.addEventListener('click', async () => {
            if (!licenseKey) { setSubStatus('Validate a license first'); return; }
            deleteBtn.disabled = true;
            deleteBtn.textContent = 'Deleting...';
            try {
                const res = await fetch(`/drivers-scout/api/subscriptions/${row.id}`, {
                    method: 'DELETE',
                    headers: { 'X-License-Key': licenseKey }
                });

                if (!res.ok) {
                    let msg = `Delete failed (${res.status}).`;
                    try {
                        const txt = await res.text();
                        if (txt) msg = `${msg} ${txt}`;
                    } catch {}
                    setSubStatus(msg);
                    return;
                }

                setSubStatus('Subscription deleted.', true);
                await loadSubscriptions();
            } catch (e) {
                console.error(e);
                setSubStatus('Server not reachable. Try again later.');
            } finally {
                deleteBtn.disabled = false;
                deleteBtn.textContent = 'Delete';
            }
        });

        actionCell.appendChild(deleteBtn);
        tr.appendChild(actionCell);
        subscriptionTableBody.appendChild(tr);
    });
}

async function loadSubscriptions() {
    if (!licenseKey) {
        setSubStatus('Validate a license first');
        return;
    }

    setSubscriptionTableState('Loading subscriptions...');
    try {
        const res = await fetch('/drivers-scout/api/subscriptions', {
            headers: {
                'X-License-Key': licenseKey
            }
        });

        if (!res.ok) {
            let msg = `Error (${res.status}).`;
            try {
                const txt = await res.text();
                if (txt) msg = `${msg} ${txt}`;
            } catch {}
            setSubStatus(msg);
            setSubscriptionTableState('Unable to load subscriptions.');
            return;
        }

        const data = await res.json();
        renderSubscriptions(Array.isArray(data) ? data : []);
    } catch (e) {
        console.error(e);
        setSubStatus('Server not reachable. Try again later.');
        setSubscriptionTableState('Unable to load subscriptions.');
    }
}

async function registerSubscription() {
    if (!licenseKey) { setSubStatus('Validate a license first'); return; }

    const webhook_url = subWebhookUrl.value.trim();
    const category = subCategory.value;
    const min_raw = subMinIr.value.trim();

    if (!webhook_url) { setSubStatus('Webhook URL is required'); return; }
    if (!isValidUrl(webhook_url)) { setSubStatus('Webhook URL is not valid'); return; }
    if (category !== 'sports_car' && category !== 'formula_car') {
        setSubStatus('Category must be sports_car or formula_car');
        return;
    }
    if (!min_raw) { setSubStatus('Min iRating is required'); return; }

    const min_irating = Number(min_raw);
    if (!Number.isFinite(min_irating) || min_irating < 0) {
        setSubStatus('Min iRating must be a number ≥ 0');
        return;
    }

    subSave.disabled = true;
    subSave.textContent = 'Saving...';
    setSubStatus('Saving...');

    try {
        const res = await fetch('/drivers-scout/api/subscriptions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-License-Key': licenseKey
            },
            body: JSON.stringify({ webhook_url, category, min_irating })
        });

        if (res.status === 200) {
            setSubStatus('Subscription updated.', true);
            await loadSubscriptions();
            return;
        }
        if (res.status === 201) {
            setSubStatus('Subscription registered.', true);
            await loadSubscriptions();
            return;
        }

        let msg = `Error (${res.status}).`;
        try {
            const txt = await res.text();
            if (txt) msg = `${msg} ${txt}`;
        } catch {}
        setSubStatus(msg);
    } catch (e) {
        console.error(e);
        setSubStatus('Server not reachable. Try again later.');
    } finally {
        subSave.disabled = false;
        subSave.textContent = 'Save subscription';
    }
}

subSave.addEventListener('click', (e) => {
    e.preventDefault();
    registerSubscription();
});

if (webhookTabBtn) {
    webhookTabBtn.addEventListener('click', () => loadSubscriptions());
}

if (webhookTabPanel && !webhookTabPanel.classList.contains('hidden')) {
    loadSubscriptions();
}
