const subStatus = document.getElementById('sub-status');
const subWebhookUrl = document.getElementById('sub-webhook-url');
const subCategory = document.getElementById('sub-category');
const subMinIr = document.getElementById('sub-min-ir');
const subSave = document.getElementById('sub-save');

function setSubStatus(msg, ok=false) {
    subStatus.textContent = msg;
    subStatus.className = 'status ' + (ok ? 'ok' : 'err');
}

function isValidUrl(value) {
    try { new URL(value); return true; } catch { return false; }
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

        if (res.status === 200) { setSubStatus('Subscription updated.', true); return; }
        if (res.status === 201) { setSubStatus('Subscription registered.', true); return; }

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
