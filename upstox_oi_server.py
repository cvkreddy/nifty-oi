async function doFetch() {
    if (!SERVER_URL) { showErr('No server configured — go to Settings'); return; }
    g('dot').style.background='var(--warn)';
    try {
        const cb = new Date().getTime();
        const [jr, hr, alr] = await Promise.all([
            fetch(SERVER_URL + '/oi/json?idx=' + currentIdx + '&_=' + cb),
            fetch(SERVER_URL + '/oi/histogram?idx=' + currentIdx + '&_=' + cb).catch(() => null), 
            fetch(SERVER_URL + '/oi/alert_log?idx=' + currentIdx + '&_=' + cb).catch(() => null)
        ]);
        
        const raw = await jr.json();
        if (raw.error) throw new Error(raw.error);

        // FIX 3: REMOVED return statement so cached data still renders!
        if (raw.backend_error) {
            showErr(`BACKEND DATA FROZEN: ${raw.backend_error} <a href="${SERVER_URL}/login" style="color:#fff;text-decoration:underline;margin-left:10px">Try Re-Login</a>`);
            g('dot').style.background='var(--bear)';
        }

        const norm = obj => {
            const r = {};
            Object.keys(obj || {}).forEach(k => {
                const nk = parseFloat(k);
                r[nk] = { ...obj[k], strike: nk };
            });
            return r;
        };
        
        raw.chain = norm(raw.chain); 
        raw.atm_strikes = norm(raw.atm_strikes);
        raw.atm = parseFloat(raw.atm) || 0; 
        raw.max_pain = parseFloat(raw.max_pain) || 0;
        raw.spot = parseFloat(raw.spot) || 0; 
        raw.futures = parseFloat(raw.futures) || raw.spot;
        raw.premium = parseFloat(raw.premium) || 0; 
        raw.pcr = parseFloat(raw.pcr) || 0; 
        raw.vix = parseFloat(raw.vix) || 0;
        
        let hist = [];
        if (hr && hr.ok) {
            try { hist = (await hr.json()).map(h => ({...h, strike: parseFloat(h.strike||0)})); } catch(e) {}
        }

        if (alr && alr.ok) {
            try { renderAlertLog(await alr.json()); } catch (e) {}
        }
        
        if (!raw.backend_error) hideErr(); 
        
        render(raw,hist);
        secs=300;
        g('hdr-live').textContent='₹'+fmt(raw.spot)+' · PCR '+raw.pcr.toFixed(2);
        if (!raw.backend_error) g('dot').style.background='var(--bull)';
        
    } catch(e) {
        showErr(e.message);
        g('dot').style.background='var(--bear)';
        g('hdr-live').textContent='Error: '+e.message;
    }
}

// FIX 4: Add auto-refresh every 2 minutes
setInterval(() => {
    // Make sure we only fetch if the app isn't busy snapping a screenshot
    if (typeof isSnapping === 'undefined' || !isSnapping) {
        doFetch();
    }
}, 120000);

window.onload = () => {
    try { doFetch(); } catch(e) { console.error("Init failed:", e); }
};