"""
====================================================
  MULTI-ASSET OI SERVER — Triple Engine Architecture
  Supports: NIFTY 50, BANK NIFTY, SENSEX
====================================================
"""

import os, csv, time, math, threading, json, urllib.parse, traceback, glob, calendar
from datetime import datetime, date, timedelta
from flask import Flask, jsonify, request, redirect, send_file, send_from_directory
from flask_cors import CORS
from autosnap import start_auto_snapper
import requests

app = Flask(__name__)
CORS(app)

@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

API_KEY      = "48131639-7647-4f99-84e2-6113734955ce"
API_SECRET   = "0j2fmzd437"
REDIRECT_URI = "https://nifty-oi.onrender.com/callback"

MANUAL_ACCESS_TOKEN = ""

TELEGRAM_BOT_TOKEN = "8709594892:AAGcSqRJLvSr-gX405Nbp3LQ0kJPghYPax4"  
TELEGRAM_CHAT_ID   = "7851805837"     

CACHE_TTL    = 300  
ATM_RANGE    = 5
TOKEN_FILE   = "token_data.json"
DATA_FILE    = "data_cache.json"  
STATE_FILE   = "server_state.json"

token_store  = {"access_token": None}
debug_status = {"last_error": "Initializing Triple Engine..."}

INDICES = {
    "NIFTY": {"key": "NSE_INDEX|Nifty 50", "step": 50},
    "BANKNIFTY": {"key": "NSE_INDEX|Nifty Bank", "step": 100},
    "SENSEX": {"key": "BSE_INDEX|SENSEX", "step": 100}
}

_EXPIRY_DAY_CACHE = {}

STORE = {idx: {
    "baseline_oi": {}, "baseline_vix": None, "baseline_rsi": {},
    "history": [], 
    "prev_oi": {}, "prev_pcr": None, "prev_spot": None,
    "sent_alerts": {}, "last_summary": 0,
    "alert_log": [],          
    "pcr_history": [],        
    "prev_max_ce_strike": None,   
    "prev_max_pe_strike": None,   
    "straddle_history": [],   
} for idx in INDICES}

oi_cache = {idx: {"data": None, "last_fetch": 0} for idx in INDICES}
candle_cache_store = {idx: {"1m": [], "3m": [], "5m": [], "15m": [], "last_full_fetch": 0, "last_fetch_day": ""} for idx in INDICES}

fetch_locks = {idx: threading.Lock() for idx in INDICES}
background_started = False
background_lock = threading.Lock()

@app.before_request
def init_background():
    global background_started
    with background_lock:
        if not background_started:
            threading.Thread(target=loop, daemon=True).start()
            try:
                start_auto_snapper()
                print("[INIT] Telegram Auto-Snapper successfully bound to worker.")
            except Exception as e:
                print(f"[INIT] Auto-snapper start failed: {e}")
            background_started = True

def reverse_engineer_baseline(idx):
    if len(STORE[idx]["baseline_oi"]) > 0: 
        return
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r") as f:
                d_all = json.load(f)
                d = d_all.get(idx, {})
                if d.get("timestamp") and d["timestamp"].startswith(date.today().isoformat()):
                    chain = d.get("chain", {})
                    if chain:
                        for s, v in chain.items():
                            b_coi = v["call_oi"] - v.get("call_oi_chg_day", 0)
                            b_poi = v["put_oi"] - v.get("put_oi_chg_day", 0)
                            b_cltp = v["call_ltp"] - v.get("call_ltp_chg_day", 0)
                            b_pltp = v["put_ltp"] - v.get("put_ltp_chg_day", 0)
                            STORE[idx]["baseline_oi"][str(s)] = {"call_oi": b_coi, "put_oi": b_poi, "call_ltp": b_cltp, "put_ltp": b_pltp}
    except Exception: 
        pass

def load_server_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                st = json.load(f)
                if st.get("date") == date.today().isoformat():
                    for idx in INDICES:
                        saved_idx = st.get(idx, {})
                        STORE[idx]["baseline_oi"] = saved_idx.get("baseline_oi", {})
                        STORE[idx]["baseline_vix"] = saved_idx.get("baseline_vix")
                        STORE[idx]["baseline_rsi"] = saved_idx.get("baseline_rsi", {})
                        STORE[idx]["prev_oi"] = saved_idx.get("prev_oi", {})
                        STORE[idx]["prev_spot"] = saved_idx.get("prev_spot")
                        STORE[idx]["prev_pcr"] = saved_idx.get("prev_pcr")
                        STORE[idx]["history"] = saved_idx.get("history", [])
        except Exception: 
            pass
    for idx in INDICES: 
        reverse_engineer_baseline(idx)

def save_server_state():
    try:
        st = {"date": date.today().isoformat()}
        for idx in INDICES:
            st[idx] = {
                "baseline_oi": STORE[idx].get("baseline_oi", {}),
                "baseline_vix": STORE[idx].get("baseline_vix"),
                "baseline_rsi": STORE[idx].get("baseline_rsi", {}),
                "prev_oi": STORE[idx].get("prev_oi", {}),
                "prev_spot": STORE[idx].get("prev_spot"),
                "prev_pcr": STORE[idx].get("prev_pcr"),
                "history": STORE[idx].get("history", [])
            }
        with open(STATE_FILE, "w") as f: 
            json.dump(st, f)
    except Exception as e: 
        pass

load_server_state()

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN: 
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try: requests.post(url, json=payload, timeout=5)
    except Exception: pass

def generate_5min_summary(idx, data, atm_strikes, atm, is_boot=False):
    spot = data.get("spot", 0)
    pcr  = data.get("pcr", 0)
    vix  = data.get("vix", 0)
    intel = data.get("intelligence", {})
    net_flow_l = intel.get("cumulative_net_flow_l", 0)
    flow_bias  = "🟢 BULLISH" if net_flow_l > 0 else "🔴 BEARISH" if net_flow_l < 0 else "⚪ NEUTRAL"
    
    lvl = intel.get("levels", {})
    orb_sig  = "🟡" if "RANGE"  in lvl.get("orb_status",  "") else "🟢" if "BULLISH" in lvl.get("orb_status",  "") else "🔴"
    yest_sig = "🟡" if "RANGE"  in lvl.get("yest_status", "") else "🟢" if "ABOVE"   in lvl.get("yest_status", "") else "🔴"

    atm_float = float(atm)
    atm_v = {}
    for k, v in atm_strikes.items():
        if abs(float(k) - atm_float) < 0.1:
            atm_v = v; break

    s_curr  = atm_v.get("call_ltp", 0) + atm_v.get("put_ltp", 0)
    s_decay = intel.get("straddle_decay", 0)
    vix_mat = intel.get("vix_matrix", {})
    boot_note = "<i>(Building baseline...)</i>" if is_boot else ""

    # PCR changes
    pcr_5m   = intel.get("pcr_5m_chg",  0)
    pcr_day  = intel.get("pcr_day_chg", 0)
    pcr_5m_s = f"{pcr_5m:+.3f}" if pcr_5m else "—"
    pcr_d_s  = f"{pcr_day:+.3f}" if pcr_day else "—"

    # Combined OI Case
    coi_lbl  = intel.get("combined_oi_label", "—")
    coi_desc = intel.get("combined_oi_desc",  "—")

    # Open=High/Low
    oeh = intel.get("open_eq_high", False)
    oel = intel.get("open_eq_low",  False)
    oeh_note = " 🔶 OPEN=HIGH (Bearish bias — gap up & sell)" if oeh else ""
    oel_note = " 🔷 OPEN=LOW (Bullish bias — gap down & buy)" if oel else ""

    # VIX signal
    vix_sig_map = {
        "STRONG BULLISH": "📈 Price↑+VIX↑ | Breakout possible, don't short",
        "WEAK BULLISH":   "📈 Price↑+VIX↓ | Weak rally, resistance may hold",
        "STRONG BEARISH": "📉 Price↓+VIX↑ | Panic sell, support may break",
        "WEAK BEARISH":   "📉 Price↓+VIX↓ | Normal correction, bounce likely",
        "SIDEWAYS_UP":    "⚡ Sideways+VIX↑ | Big move loading",
        "SIDEWAYS_DOWN":  "😴 Sideways+VIX↓ | Boring, theta decay",
    }
    vix_sig_txt = vix_sig_map.get(vix_mat.get("signal", ""), vix_mat.get("desc", "Need more data"))

    msg = (
        f"⏱ <b>5-MIN {idx} SCANNER</b>\n"
        f"🎯 <b>Spot:</b> ₹{spot:,.0f} | <b>Open:</b> ₹{lvl.get('today_open', '-')}{oeh_note}{oel_note}\n"
        f"📊 <b>VIX:</b> {vix:.2f} | <b>VIX Signal: {vix_mat.get('signal', 'WAITING')}</b>\n"
        f"   ↳ {vix_sig_txt}\n"
        f"{orb_sig} <b>ORB:</b> {lvl.get('orb_status', '-')} @ {lvl.get('orb_time', '-')}\n"
        f"{yest_sig} <b>Yest H/L:</b> {lvl.get('yest_status', '-')} @ {lvl.get('yest_time', '-')}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"⚖️ <b>PCR:</b> {pcr:.2f} | 5m: {pcr_5m_s} | Day: {pcr_d_s}\n"
        f"   ({'BULLISH' if pcr > 1.2 else 'BEARISH' if pcr < 0.8 else 'NEUTRAL'} zone)\n"
        f"⚖️ <b>Straddle:</b> ₹{s_curr:.1f} ({s_decay:+.1f}% Day)\n"
        f"🌊 <b>Smart Flow:</b> {flow_bias} ({net_flow_l:+.1f}L Net)\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📦 <b>Combined OI:</b> {coi_lbl}\n"
        f"   ↳ {coi_desc}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🔴 <b>ATM CE:</b> ₹{atm_v.get('call_ltp', 0):.1f} | 5m: {atm_v.get('call_ltp_chg', 0):+.1f} | Day: {atm_v.get('call_ltp_chg_day', 0):+.1f} {boot_note}\n"
        f"🟢 <b>ATM PE:</b> ₹{atm_v.get('put_ltp', 0):.1f} | 5m: {atm_v.get('put_ltp_chg', 0):+.1f} | Day: {atm_v.get('put_ltp_chg_day', 0):+.1f} {boot_note}\n"
        f"━━━━━━━━━━━━━━━━\n\n"
    )
    
    def get_short_cond(flow):
        c = flow.get("condition", "")
        e = flow.get("emoji", "⚪")
        if "LONG BUILDUP"   in c: return f"Long Build {e}"
        if "SHORT COVERING" in c: return f"Short Cover 📈"
        if "SHORT BUILDUP"  in c: return f"Writing {e}"
        if "LONG UNWINDING" in c: return f"Unwinding {e}"
        return "Stable ⚪"

    strikes_to_show = [float(k) for k in atm_strikes.keys()]
    for s in sorted(strikes_to_show, reverse=True):
        if abs(s - atm) > 2 * INDICES[idx]["step"]: continue
        v = {}
        for k_str, val in atm_strikes.items():
            if abs(float(k_str) - s) < 0.1: v = val; break
        marker = " ◄ ATM" if abs(s - atm) < 0.1 else ""
        c_ltp, c_5m, c_d = v.get("call_ltp",0), v.get("call_ltp_chg",0), v.get("call_ltp_chg_day",0)
        c_oi5, c_oid      = v.get("call_oi_chg",0)/100000, v.get("call_oi_chg_day",0)/100000
        c_cond = get_short_cond(v.get("call_flow", {}))
        p_ltp, p_5m, p_d = v.get("put_ltp",0), v.get("put_ltp_chg",0), v.get("put_ltp_chg_day",0)
        p_oi5, p_oid      = v.get("put_oi_chg",0)/100000, v.get("put_oi_chg_day",0)/100000
        p_cond = get_short_cond(v.get("put_flow", {}))
        # Open=High/Low for this option
        c_oeh = v.get("call_open",0) > 0 and c_ltp >= v.get("call_open",0)*0.999
        p_oeh = v.get("put_open",0)  > 0 and p_ltp >= v.get("put_open",0)*0.999
        c_oel = v.get("call_open",0) > 0 and c_ltp <= v.get("call_open",0)*0.97
        p_oel = v.get("put_open",0)  > 0 and p_ltp <= v.get("put_open",0)*0.97

        msg += f"🎯 <b>{int(s)}{marker}</b>\n"
        msg += f"🔴 <b>CE | {c_cond}</b>{' 🔶OEH' if c_oeh else ''}{' 🔷OEL' if c_oel else ''}\n"
        msg += f"LTP: ₹{c_ltp:.1f} (5m: {c_5m:+.1f} | Day: {c_d:+.1f})\n"
        msg += f"OI : 5m: {c_oi5:+.2f}L | Day: {c_oid:+.2f}L\n\n"
        msg += f"🟢 <b>PE | {p_cond}</b>{' 🔶OEH' if p_oeh else ''}{' 🔷OEL' if p_oel else ''}\n"
        msg += f"LTP: ₹{p_ltp:.1f} (5m: {p_5m:+.1f} | Day: {p_d:+.1f})\n"
        msg += f"OI : 5m: {p_oi5:+.2f}L | Day: {p_oid:+.2f}L\n"
        msg += f"〰️〰️〰️〰️〰️〰️〰️〰️\n"
        
    return msg.strip()

def process_telegram_alerts(idx, alerts, data, atm_strikes, atm):
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current_mins = ist_now.hour * 60 + ist_now.minute
    today_str = ist_now.strftime("%Y-%m-%d")
    
    holidays = ["2026-01-15", "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25"]
    if ist_now.weekday() >= 5 or today_str in holidays or not (540 <= current_mins <= 935): return
        
    current_time = time.time()
    store = STORE[idx]
    
    if store["last_summary"] == 0: 
        store["last_summary"] = current_time
        summary = generate_5min_summary(idx, data, atm_strikes, atm, is_boot=True)
        send_telegram_alert(f"🚀 <b>SERVER LIVE ({idx})</b>\n\n{summary}")
        return
        
    try:
        for a in alerts:
            msg = f"{a['icon']} <b>[{idx}] {a['type']}</b>\n{a['message']}"
            if msg not in store["sent_alerts"] or (current_time - store["sent_alerts"][msg] > 1800):
                send_telegram_alert(msg)
                store["sent_alerts"][msg] = current_time
        store["sent_alerts"] = {k: v for k, v in store["sent_alerts"].items() if current_time - v < 3600}
        
        if current_time - store["last_summary"] >= 270: 
            summary = generate_5min_summary(idx, data, atm_strikes, atm)
            send_telegram_alert(summary)
            store["last_summary"] = current_time
    except Exception: pass

def save_token(token):
    token_store["access_token"] = token
    try:
        with open(TOKEN_FILE, "w") as f: json.dump({"access_token": token}, f)
    except Exception: pass

def load_token():
    if MANUAL_ACCESS_TOKEN and len(MANUAL_ACCESS_TOKEN) > 50:
        token_store["access_token"] = MANUAL_ACCESS_TOKEN
        return
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f: 
                saved = json.load(f).get("access_token")
                if saved: token_store["access_token"] = saved
        except Exception: pass

load_token()

def hdrs():
    load_token()
    return {"Authorization": f"Bearer {token_store['access_token']}", "Accept": "application/json", "Api-Version": "2.0"}

def fetch_spot(idx):
    sym = INDICES[idx]["key"]
    keys_to_try = [sym]
    if idx == "NIFTY": keys_to_try.extend(["NSE_INDEX|Nifty 50", "NSE_INDEX|NIFTY 50"])
    elif idx == "BANKNIFTY": keys_to_try.extend(["NSE_INDEX|Nifty Bank", "NSE_INDEX|NIFTY BANK", "NSE_INDEX|BANKNIFTY"])
    elif idx == "SENSEX": keys_to_try.extend(["BSE_INDEX|SENSEX", "BSE_INDEX|Sensex"])
    
    keys_to_try = list(dict.fromkeys(keys_to_try))
    
    for key in keys_to_try:
        try:
            r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": key}, headers=hdrs(), timeout=8)
            d = r.json().get("data", {})
            if d:
                k = list(d.keys())[0]
                INDICES[idx]["key"] = key
                return float(d[k].get("last_price", 0))
        except Exception: 
            pass
    return 0

def fetch_futures(spot, idx):
    now = date.today()
    months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
    prefix = "NIFTY" if idx == "NIFTY" else "BANKNIFTY" if idx == "BANKNIFTY" else "SENSEX"
    for delta in [0, 1]:
        m = (now.month - 1 + delta) % 12
        y = str(now.year)[2:] if now.month + delta <= 12 else str(now.year + 1)[2:]
        sym = f"BSE_FO|{prefix}{y}{months[m]}FUT" if idx == "SENSEX" else f"NSE_FO|{prefix}{y}{months[m]}FUT"
        try:
            r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": sym}, headers=hdrs(), timeout=5)
            if r.status_code == 200 and r.json().get("data"):
                d = r.json()["data"]
                key = list(d.keys())[0]
                p = d[key].get("last_price") or d[key].get("ltp") or 0
                if p: return float(p)
        except Exception: pass
    return round(spot * 1.005, 2)

def fetch_vix():
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": "NSE_INDEX|India VIX"}, headers=hdrs(), timeout=10)
        if r.status_code == 200 and r.json().get("data"):
            d = r.json()["data"]
            key = list(d.keys())[0]
            return float(d[key].get("last_price") or d[key].get("ltp") or 0)
    except Exception: pass
    return 0

def _parse_candles(raw_list):
    unique = {}
    for c in (raw_list or []):
        if len(c) >= 5:
            unique[c[0]] = {"time": c[0], "open": float(c[1]), "high": float(c[2]),
                            "low": float(c[3]), "close": float(c[4]),
                            "vol": float(c[5]) if len(c) > 5 else 0}
    res = list(unique.values())
    res.sort(key=lambda x: x["time"])
    return res

def fetch_base_1m_candles(idx):
    safe_key = urllib.parse.quote(INDICES[idx]["key"])
    store    = candle_cache_store[idx]
    now_ts   = time.time()
    try:
        url_intra = f"https://api.upstox.com/v2/historical-candle/intraday/{safe_key}/1minute"
        r = requests.get(url_intra, headers=hdrs(), timeout=15)
        if r.status_code == 200:
            today_candles = _parse_candles(r.json().get("data", {}).get("candles", []))
        else:
            today_candles = []
    except Exception:
        today_candles = []

    existing = store.get("1m", [])
    today_str = date.today().isoformat()
    historical = [c for c in existing if c["time"][:10] != today_str]
    should_fetch_hist = (store.get("last_fetch_day", "") != today_str) and not historical

    if should_fetch_hist:
        store["last_fetch_day"] = today_str
        try:
            to_dt   = date.today().strftime("%Y-%m-%d")
            from_dt = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")
            url_h   = f"https://api.upstox.com/v2/historical-candle/{safe_key}/1minute/{to_dt}/{from_dt}"
            rh = requests.get(url_h, headers=hdrs(), timeout=15)
            if rh.status_code == 200:
                hist_raw = _parse_candles(rh.json().get("data",{}).get("candles",[]))
                historical = [c for c in hist_raw if c["time"][:10] != today_str]
                store["last_full_fetch"] = now_ts
        except Exception: pass

    all_candles = historical + today_candles
    all_candles.sort(key=lambda x: x["time"])
    cutoff = (datetime.now() - timedelta(days=5)).isoformat()
    all_candles = [c for c in all_candles if c["time"] >= cutoff]
    store["1m"] = all_candles
    return all_candles

def resample_candles(candles_1m, tf):
    if not candles_1m: return []
    res = []; cg = []; ct = None
    for c in candles_1m:
        try:
            dt = datetime.strptime(c["time"][:16], "%Y-%m-%dT%H:%M")
            gt = dt.replace(minute=(dt.minute // tf) * tf, second=0, microsecond=0)
            if ct is None: ct = gt
            if gt == ct: cg.append(c)
            else:
                res.append({"time": ct.isoformat(), "open": cg[0]["open"], "high": max(x["high"] for x in cg), "low": min(x["low"] for x in cg), "close": cg[-1]["close"], "vol": sum(x.get("vol", 0) for x in cg)})
                cg = [c]; ct = gt
        except Exception: pass
    if cg: res.append({"time": ct.isoformat(), "open": cg[0]["open"], "high": max(x["high"] for x in cg), "low": min(x["low"] for x in cg), "close": cg[-1]["close"], "vol": sum(x.get("vol", 0) for x in cg)})
    return res

def fetch_chain_raw(idx, expiry):
    sym = INDICES[idx]["key"]
    keys_to_try = [sym]
    if idx == "NIFTY": keys_to_try.extend(["NSE_INDEX|Nifty 50", "NSE_INDEX|NIFTY 50"])
    elif idx == "BANKNIFTY": keys_to_try.extend(["NSE_INDEX|Nifty Bank", "NSE_INDEX|NIFTY BANK", "NSE_INDEX|BANKNIFTY"])
    elif idx == "SENSEX": keys_to_try.extend(["BSE_INDEX|SENSEX", "BSE_INDEX|Sensex"])
    keys_to_try = list(dict.fromkeys(keys_to_try))
    
    for key in keys_to_try:
        try:
            r = requests.get("https://api.upstox.com/v2/option/chain", params={"instrument_key": key, "expiry_date": expiry}, headers=hdrs(), timeout=6)
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    INDICES[idx]["key"] = key
                    return data
        except Exception: pass
    return []

# 🚨 UPDATED: Exact Expiry Matcher based on precise rules (Nifty=Tue, Sensex=Thu, BankNifty=Last Tue)
def get_expiry(idx):
    sym = INDICES[idx]["key"]
    keys_to_try = [sym]
    if idx == "NIFTY": keys_to_try.extend(["NSE_INDEX|Nifty 50", "NSE_INDEX|NIFTY 50"])
    elif idx == "BANKNIFTY": keys_to_try.extend(["NSE_INDEX|Nifty Bank", "NSE_INDEX|NIFTY BANK", "NSE_INDEX|BANKNIFTY"])
    elif idx == "SENSEX": keys_to_try.extend(["BSE_INDEX|SENSEX", "BSE_INDEX|Sensex"])
    keys_to_try = list(dict.fromkeys(keys_to_try))
    
    for key in keys_to_try:
        try:
            r = requests.get("https://api.upstox.com/v2/option/contract", params={"instrument_key": key}, headers=hdrs(), timeout=10)
            if r.status_code == 200:
                items = r.json().get("data", [])
                if items:
                    INDICES[idx]["key"] = key
                    exps = sorted([i if isinstance(i, str) else i.get("expiry") for i in items if i])
                    today_str = datetime.today().strftime("%Y-%m-%d")
                    for e in exps:
                        if e and e >= today_str: return e
        except Exception: 
            pass

    # Strict Mathematical Fallback (If API is completely unresponsive)
    holidays = ["2026-01-15", "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25"]
    today_dt = date.today()
    ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
    
    def get_last_tuesday(y, m):
        last_day = calendar.monthrange(y, m)[1]
        d = date(y, m, last_day)
        offset = (d.weekday() - 1) % 7 # 1 is Tuesday
        return d - timedelta(days=offset)

    if idx == "BANKNIFTY":
        target_date = get_last_tuesday(today_dt.year, today_dt.month)
        if today_dt > target_date or (today_dt == target_date and (ist_time.hour > 15 or (ist_time.hour == 15 and ist_time.minute > 30))):
            nm = today_dt.month + 1 if today_dt.month < 12 else 1
            ny = today_dt.year if today_dt.month < 12 else today_dt.year + 1
            target_date = get_last_tuesday(ny, nm)
        
        while target_date.strftime("%Y-%m-%d") in holidays:
            target_date -= timedelta(days=1)
            
        return target_date.strftime("%Y-%m-%d")
        
    else:
        target = 1 if idx == "NIFTY" else 3 # NIFTY=Tue(1), SENSEX=Thu(3)
        weekday = today_dt.weekday()
        days_until = (target - weekday) % 7
        
        if days_until == 0 and (ist_time.hour > 15 or (ist_time.hour == 15 and ist_time.minute > 30)):
            days_until = 7
            
        target_date = today_dt + timedelta(days=days_until)
        
        while target_date.strftime("%Y-%m-%d") in holidays:
            target_date -= timedelta(days=1)
            
        return target_date.strftime("%Y-%m-%d")

def compute_max_pain(chain):
    strikes = sorted([float(k) for k in chain.keys()])
    if not strikes: return 0
    min_loss, mp = float("inf"), strikes[0]
    for s in strikes:
        loss = sum(v["call_oi"]*(s-float(k)) if float(k)<s else v["put_oi"]*(float(k)-s) if float(k)>s else 0 for k,v in chain.items())
        if loss < min_loss: min_loss, mp = loss, s
    return mp

def get_vwap(candles):
    if not candles: return None
    last_date = candles[-1]["time"][:10]
    cum_vol = cum_pv = 0
    for c in candles:
        if c.get("time", "").startswith(last_date):
            v = c.get('vol', 0)
            cum_vol += v
            cum_pv += ((c['high'] + c['low'] + c['close']) / 3) * v
    return round(cum_pv / cum_vol, 2) if cum_vol > 0 else None

def calc_ema_array(prices, period):
    if not prices: return []
    if len(prices) < period: return [round(sum(prices[:i+1])/(i+1), 2) for i in range(len(prices))]
    emas = [None] * (period - 1)
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    emas.append(round(ema, 2))
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
        emas.append(round(ema, 2))
    return emas

def calc_supertrend(candles, period=7, multiplier=3.0):
    """
    TradingView-matching SuperTrend using Wilder's RMA (smoothed moving average) for ATR.
    TV uses ta.atr() which is RMA not simple average — this matches TV exactly.
    """
    if len(candles) < period + 2: return None, None
    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]

    # True ranges
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(candles))]

    # Wilder's RMA (same as TradingView ta.rma / ta.atr)
    alpha = 1.0 / period
    atr = sum(trs[:period]) / period          # seed with SMA
    for i in range(period, len(trs)):
        atr = alpha * trs[i] + (1 - alpha) * atr

    hl2 = (highs[-1] + lows[-1]) / 2
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr

    # Full band-flip logic matching TradingView (track direction over history)
    direction = "BULLISH"
    prev_upper, prev_lower = upper, lower
    prev_dir = "BULLISH"
    for i in range(period + 1, len(candles)):
        h = highs[i]; l = lows[i]; pc = closes[i-1]; c = closes[i]
        tr_i = max(h-l, abs(h-pc), abs(l-pc))
        atr   = alpha * tr_i + (1 - alpha) * atr
        hl2_i = (h + l) / 2
        raw_upper = hl2_i + multiplier * atr
        raw_lower = hl2_i - multiplier * atr
        # Band continuation rule (TV style)
        cur_upper = raw_upper if (raw_upper < prev_upper or pc > prev_upper) else prev_upper
        cur_lower = raw_lower if (raw_lower > prev_lower or pc < prev_lower) else prev_lower
        if prev_dir == "BULLISH":
            direction = "BEARISH" if c < cur_lower else "BULLISH"
        else:
            direction = "BULLISH" if c > cur_upper else "BEARISH"
        prev_upper, prev_lower, prev_dir = cur_upper, cur_lower, direction

    st_val = (highs[-1] + lows[-1]) / 2 + multiplier * atr if direction == "BEARISH" else \
             (highs[-1] + lows[-1]) / 2 - multiplier * atr
    return direction, round(st_val, 2)

def calc_rsi_array(closes, p=14):
    if len(closes) < p+1: return []
    gains = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    if sum(losses[:p]) == 0: return [None]*p + [100.0] * (len(gains) - p + 1)
    ag, al = sum(gains[:p])/p, sum(losses[:p])/p
    rsis = [None]*p
    rsis.append(100.0 if al==0 else 100 - (100/(1+ag/al)))
    for i in range(p, len(gains)):
        ag = (ag*(p-1) + gains[i])/p
        al = (al*(p-1) + losses[i])/p
        rsis.append(100.0 if al==0 else 100 - (100/(1+ag/al)))
    return rsis

def calc_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return None, None, None
    ema_fast, ema_slow = calc_ema_array(prices, fast), calc_ema_array(prices, slow)
    macd_line = [round(f - s, 4) if f is not None and s is not None else None for f, s in zip(ema_fast, ema_slow)]
    valid_macd = [x for x in macd_line if x is not None]
    if len(valid_macd) < signal: return None, None, None
    sig_arr = calc_ema_array(valid_macd, signal)
    macd_val = valid_macd[-1]
    sig_val  = sig_arr[-1] if sig_arr else None
    hist_val = round(macd_val - sig_val, 4) if sig_val is not None else None
    return round(macd_val, 2), round(sig_val, 2) if sig_val is not None else None, round(hist_val, 2) if hist_val is not None else None

def calc_adx_full(candles, p=14):
    """
    TradingView-matching ADX using Wilder's smoothing (RMA).
    TV uses ta.dmi() which uses RMA not EMA — seed with SMA then Wilder smooth.
    """
    if not candles or len(candles) < p * 2 + 2: return None, None, None
    trl, pdml, ndml = [], [], []
    for i in range(1, len(candles)):
        h, l  = candles[i]["high"],   candles[i]["low"]
        ph,pl = candles[i-1]["high"], candles[i-1]["low"]
        pc    = candles[i-1]["close"]
        trl.append(max(h-l, abs(h-pc), abs(l-pc)))
        dm_up   = h - ph
        dm_down = pl - l
        pdml.append(dm_up   if dm_up   > dm_down and dm_up   > 0 else 0)
        ndml.append(dm_down if dm_down > dm_up   and dm_down > 0 else 0)

    # Wilder's RMA smoothing (same as TV ta.rma)
    def wilder_rma(data, n):
        if len(data) < n: return []
        result = []
        val = sum(data[:n]) / n      # SMA seed
        result.append(val)
        for i in range(n, len(data)):
            val = (val * (n - 1) + data[i]) / n
            result.append(val)
        return result

    atr_s = wilder_rma(trl, p)
    pdm_s = wilder_rma(pdml, p)
    ndm_s = wilder_rma(ndml, p)

    min_len = min(len(atr_s), len(pdm_s), len(ndm_s))
    if min_len < p: return None, None, None

    dxl, pdi_last, ndi_last = [], 0, 0
    for i in range(min_len):
        a = atr_s[i]
        if a == 0: continue
        pdi_v = 100 * pdm_s[i] / a
        ndi_v = 100 * ndm_s[i] / a
        pdi_last, ndi_last = pdi_v, ndi_v
        s = pdi_v + ndi_v
        dxl.append(100 * abs(pdi_v - ndi_v) / s if s else 0)

    if len(dxl) < p: return None, None, None
    # ADX = RMA of DX
    adx_arr = wilder_rma(dxl, p)
    if not adx_arr: return None, None, None
    return round(adx_arr[-1], 2), round(pdi_last, 1), round(ndi_last, 1)

def get_indicators(candles):
    if not candles or len(candles) < 16:
        return {"rsi": None, "adx": None, "pdi": None, "ndi": None, "candle_count": len(candles) if candles else 0}
    closes  = [c["close"] for c in candles]
    rsis = calc_rsi_array(closes, 14)
    adx_val, pdi, ndi = calc_adx_full(candles, 14)
    adx_sig = "N/A"
    if adx_val is not None:
        adx_sig = ("STRONG BULL" if (pdi or 0) > (ndi or 0) else "STRONG BEAR") if adx_val >= 25 else "DEVELOPING" if adx_val >= 20 else "WEAK/RANGING"
    return {"rsi": rsis[-1] if rsis else None, "adx": adx_val, "pdi": pdi, "ndi": ndi, "adx_signal": adx_sig, "candle_count": len(candles)}

def extract_levels(candles, spot):
    if not candles: return {}
    dates = sorted(list(set([c["time"][:10] for c in candles])))
    if not dates: return {}
    today_candles = [c for c in candles if c["time"][:10] == dates[-1]]
    yest_candles = [c for c in candles if c["time"][:10] == dates[-2]] if len(dates) > 1 else []
    
    yest_high = max([c["high"] for c in yest_candles]) if yest_candles else None
    yest_low = min([c["low"] for c in yest_candles]) if yest_candles else None
    today_open = today_candles[0]["open"] if today_candles else None
    
    orb_candles = [c for c in today_candles if "09:15" <= c["time"][11:16] <= "09:29"]
    orb_high = max([c["high"] for c in orb_candles]) if orb_candles else None
    orb_low = min([c["low"] for c in orb_candles]) if orb_candles else None
        
    orb_status, orb_time = "IN ORB RANGE", "-"
    if orb_high and orb_low:
        orb_status = "ABOVE ORB (BULLISH)" if spot > orb_high else "BELOW ORB (BEARISH)" if spot < orb_low else orb_status
        for c in today_candles:
            if c["time"][11:16] > "09:29":
                if c["high"] > orb_high and "ABOVE" in orb_status: orb_time = c["time"][11:16]; break
                elif c["low"] < orb_low and "BELOW" in orb_status: orb_time = c["time"][11:16]; break

    yest_status, yest_time = "INSIDE YEST RANGE", "-"
    if yest_high and yest_low:
        yest_status = "ABOVE YEST HIGH" if spot > yest_high else "BELOW YEST LOW" if spot < yest_low else yest_status
        for c in today_candles:
            if c["high"] > yest_high and "ABOVE" in yest_status: yest_time = c["time"][11:16]; break
            elif c["low"] < yest_low and "BELOW" in yest_status: yest_time = c["time"][11:16]; break
                
    return {"today_open": today_open, "yest_high": yest_high, "yest_low": yest_low, "orb_high": orb_high, "orb_low": orb_low, "orb_status": orb_status, "orb_time": orb_time, "yest_status": yest_status, "yest_time": yest_time}

def compute_tf_signals(idx, candles, label, st_period=7, st_multiplier=3.0):
    if not candles or len(candles) < 30:   # need ≥30 candles for Wilder's RMA to stabilise
        return {"label": label, "candle_count": len(candles) if candles else 0, "ts_start": "-", "ts_pull": "-", "ts_cont": "-", "ts_st": "-", "ema_crossovers": []}
    
    store = STORE[idx]
    vwap = get_vwap(candles)
    closes = [c["close"] for c in candles]
    times = [c["time"] for c in candles]
    
    ema7_arr, ema15_arr, ema21_arr = calc_ema_array(closes, 7), calc_ema_array(closes, 15), calc_ema_array(closes, 21)
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    trend_start = pull_time = cont_time = st_time = "-"
    is_bull, await_pull_b, await_pull_s, curr_st = None, False, False, None
    ema_crossovers = []

    for i in range(15, len(candles)):
        e7, e15 = ema7_arr[i], ema15_arr[i]
        if e7 is None or e15 is None: continue
        c_close = closes[i]
        raw_t = times[i]
        try:
            d_parts = raw_t[:10].split("-"); t_parts = raw_t[11:16]
            is_today = (raw_t[:10] == today_str)
            c_time = f"Today {t_parts}" if is_today else f"{d_parts[2]}-{d_parts[1]} {t_parts}"
        except: is_today, c_time = False, "-"

        curr_bull = e7 > e15
        
        if is_bull is None:
            is_bull = curr_bull; trend_start = c_time
            ema_crossovers.append({"time": c_time, "dir": "BULL" if curr_bull else "BEAR", "close": round(c_close, 2), "is_today": is_today, "label": "Initial"})
            if curr_bull: await_pull_b = True
            else: await_pull_s = True
        elif is_bull != curr_bull:
            trend_start, pull_time, cont_time = c_time, "-", "-"
            ema_crossovers.append({"time": c_time, "dir": "BULL" if curr_bull else "BEAR", "close": round(c_close, 2), "is_today": is_today, "label": "Cross ▲" if curr_bull else "Cross ▼"})
            is_bull = curr_bull
            if curr_bull: await_pull_b = True
            else: await_pull_s = True
                
        if is_bull:
            if await_pull_b and (c_close < e7 or c_close < e15): pull_time = c_time; cont_time = "..."; await_pull_b = False
            elif not await_pull_b and c_close > e7: cont_time = c_time; await_pull_b = True
        else:
            if await_pull_s and (c_close > e7 or c_close > e15): pull_time = c_time; cont_time = "..."; await_pull_s = False
            elif not await_pull_s and c_close < e7: cont_time = c_time; await_pull_s = True
                
        s_dir, _ = calc_supertrend(candles[:i+1], st_period, st_multiplier)
        if curr_st is None: curr_st, st_time = s_dir, c_time
        elif curr_st != s_dir: st_time, curr_st = c_time, s_dir

    ema7, ema15, ema21, price = ema7_arr[-1], ema15_arr[-1], (ema21_arr[-1] if ema21_arr else None), closes[-1]
    st_dir, st_val = calc_supertrend(candles, st_period, st_multiplier)
    
    trend = "N/A"
    if ema7 and ema15:
        if price > ema7 and ema7 > ema15: trend = "STRONG BULLISH"
        elif price > ema7 and ema7 < ema15: trend = "RECOVERING"
        elif price < ema7 and ema7 > ema15: trend = "MILD BEARISH"
        else: trend = "STRONG BEARISH"

    rsis = calc_rsi_array(closes, 14)
    curr_rsi = round(rsis[-1], 2) if rsis and rsis[-1] is not None else 0
    prev_rsi = round(rsis[-2], 2) if rsis and len(rsis) > 1 and rsis[-2] is not None else curr_rsi
    
    base_r = store["baseline_rsi"].get(label)
    if not base_r and curr_rsi > 0: store["baseline_rsi"][label] = base_r = curr_rsi
    rsi_5m_chg = round(curr_rsi - prev_rsi, 2) if curr_rsi else 0
    rsi_day_chg = round(curr_rsi - base_r, 2) if curr_rsi and base_r else 0
    rsi_sig = ("OVERBOUGHT" if curr_rsi >= 70 else "OVERSOLD" if curr_rsi <= 30 else "BULLISH" if curr_rsi >= 55 else "BEARISH" if curr_rsi <= 45 else "NEUTRAL") if curr_rsi else None

    macd_val, macd_sig, macd_hist = calc_macd(closes)
    adx_val, pdi, ndi = calc_adx_full(candles, 14)
    adx_sig = ("STRONG BULL" if (pdi or 0) > (ndi or 0) else "STRONG BEAR") if adx_val and adx_val >= 25 else ("DEVELOPING" if adx_val and adx_val >= 20 else "RANGING")

    return {
        "label": label, "candle_count": len(candles), "current_price": round(price, 2) if price else None, 
        "ema7": ema7, "ema15": ema15, "ema21": ema21, "vwap": vwap, 
        "price_above_ema7": (price > ema7) if ema7 else None, "price_above_ema15": (price > ema15) if ema15 else None, 
        "ema7_above_ema15": (ema7 > ema15) if (ema7 and ema15) else None, "price_above_vwap": (price > vwap) if vwap else None,
        "trend": trend, "supertrend": st_dir, "supertrend_val": st_val, 
        "rsi": curr_rsi if curr_rsi > 0 else None, "rsi_signal": rsi_sig, "rsi_5m_chg": rsi_5m_chg, "rsi_day_chg": rsi_day_chg,
        "adx": adx_val, "pdi": pdi, "ndi": ndi, "adx_signal": adx_sig,
        "macd": macd_val, "macd_signal": macd_sig, "macd_hist": macd_hist,
        "ts_start": trend_start, "ts_pull": pull_time, "ts_cont": cont_time, "ts_st": st_time,
        "ema_crossovers": list(reversed(ema_crossovers[-20:]))
    }

def price_oi_matrix(spot, prev_spot, chain, atm, idx):
    step = INDICES[idx]["step"]
    if prev_spot is None: return "INITIALIZING","—","Waiting for second data cycle"
    price_up, price_dn = spot > prev_spot, spot < prev_spot
    total_oi_chg = sum(v["call_oi_chg"]+v["put_oi_chg"] for s,v in chain.items() if abs(float(s)-atm)<=ATM_RANGE*step)
    if price_up and total_oi_chg > 0: return "FRESH LONG BUILD","BULLISH","New buyers entering — strong upward momentum. Hold longs."
    elif price_up and total_oi_chg < 0: return "SHORT COVERING","WEAK BULLISH","Bears exiting, not fresh bulls. Rally may lack strength."
    elif price_dn and total_oi_chg > 0: return "FRESH SHORT BUILD","BEARISH","New sellers entering — strong downward momentum. Hold shorts."
    elif price_dn and total_oi_chg < 0: return "LONG UNWINDING","WEAK BEARISH","Bulls exiting. Fall may slow — no new shorts yet."
    else: return "NO CHANGE","NEUTRAL","OI unchanged this cycle."

def market_state(pcr, adx, oi_matrix_signal, alerts, vix):
    signals=[]
    if oi_matrix_signal in ("BULLISH","WEAK BULLISH"):   signals.append(1)
    elif oi_matrix_signal in ("BEARISH","WEAK BEARISH"): signals.append(-1)
    if adx and adx>=25: signals.append(1 if oi_matrix_signal=="BULLISH" else -1)
    elif adx and adx<20: signals.append(0)
    if pcr<0.6 or pcr>1.5: signals.append(0)
    elif pcr<0.8: signals.append(-1)
    elif pcr>1.2: signals.append(1)
    
    if any(a["type"]=="BREAKOUT UP" for a in alerts): return "BREAKOUT IMMINENT"
    if any(a["type"]=="BREAKOUT DOWN" for a in alerts): return "BREAKOUT IMMINENT"
    if pcr<0.6 or pcr>1.5: return "REVERSAL ZONE"
    
    score = sum(signals)
    if score >= 2: return "TRENDING UP"
    elif score <= -2: return "TRENDING DOWN"
    elif adx and adx<20: return "RANGING"
    elif vix>25: return "HIGH VOLATILITY RANGE"
    else: return "NEUTRAL / WAIT"

def analyze_vix_price(spot, baseline_trend_val, vix, base_vix):
    if not baseline_trend_val or not base_vix or base_vix == 0: return {"signal": "WAITING", "desc": "Need more data for baseline comparison"}
    price_up, vix_up = spot > baseline_trend_val, vix > base_vix
    if price_up and vix_up: return {"signal": "STRONG BULLISH", "desc": "Price ↑ + VIX ↑ | Fear rising + Rally = Breakout expected."}
    elif price_up and not vix_up: return {"signal": "WEAK BULLISH", "desc": "Price ↑ + VIX ↓ | Move lacks power = Resistance might hold."}
    elif not price_up and vix_up: return {"signal": "STRONG BEARISH", "desc": "Price ↓ + VIX ↑ | Panic selling = Support might break."}
    elif not price_up and not vix_up: return {"signal": "WEAK BEARISH", "desc": "Price ↓ + VIX ↓ | Normal correction = Support might hold."}
    return {"signal": "NEUTRAL", "desc": "Market flat"}

def process_chain(idx, raw, spot):
    store = STORE[idx]
    now = time.time()
    
    if not store["baseline_oi"] and raw:
        store["baseline_oi"] = {}
        for item in raw:
            s = str(item.get("strike_price"))
            if s and s != "None":
                cmd, pmd = item.get("call_options", {}).get("market_data", {}), item.get("put_options", {}).get("market_data", {})
                store["baseline_oi"][s] = {"call_oi": float(cmd.get("oi") or 0), "put_oi": float(pmd.get("oi") or 0), "call_ltp": float(cmd.get("ltp") or 0), "put_ltp": float(pmd.get("ltp") or 0)}

    target_5m, target_10m = now - 300, now - 600
    prev_chain, prev2_chain = {}, {}

    if store["history"]:
        valid_5m = [h for h in store["history"] if 180 <= (now - h["ts"]) <= 480]
        if valid_5m: prev_chain = min(valid_5m, key=lambda x: abs(x["ts"] - target_5m)).get("chain", {})
        else:
            old_entries = [h for h in store["history"] if (now - h["ts"]) >= 60]
            if old_entries: prev_chain = old_entries[0].get("chain", {})

        valid_10m = [h for h in store["history"] if 420 <= (now - h["ts"]) <= 840]
        if valid_10m: prev2_chain = min(valid_10m, key=lambda x: abs(x["ts"] - target_10m)).get("chain", {})

    base_chain = store["baseline_oi"]
    result = {}

    for item in raw:
        strike = float(item.get("strike_price", 0))
        if not strike: continue
        
        ce_md, pe_md = item.get("call_options", {}).get("market_data", {}), item.get("put_options", {}).get("market_data", {})
        ce_gk, pe_gk = item.get("call_options", {}).get("option_greeks", {}), item.get("put_options", {}).get("option_greeks", {})
        
        call_oi, put_oi = float(ce_md.get("oi") or 0), float(pe_md.get("oi") or 0)
        call_vol, put_vol = float(ce_md.get("volume") or 0), float(pe_md.get("volume") or 0)
        call_ltp, put_ltp = float(ce_md.get("ltp") or ce_md.get("last_price") or 0), float(pe_md.get("ltp") or pe_md.get("last_price") or 0)
        call_open, put_open = float(ce_md.get("open_price") or 0), float(pe_md.get("open_price") or 0)
        
        s_str = str(strike)
        prev_v, base_v, prev2_v = prev_chain.get(s_str, {}), base_chain.get(s_str, {}), prev2_chain.get(s_str, {})

        c_oi_5m, p_oi_5m = call_oi - prev_v.get("call_oi", call_oi), put_oi - prev_v.get("put_oi", put_oi)
        c_oi_prev5m, p_oi_prev5m = prev_v.get("call_oi", call_oi) - prev2_v.get("call_oi", prev_v.get("call_oi", call_oi)), prev_v.get("put_oi", put_oi) - prev2_v.get("put_oi", prev_v.get("put_oi", put_oi))
        
        c_ltp_d = call_ltp - (ce_md.get("previous_close") or ce_md.get("close_price") or base_v.get("call_ltp", call_ltp))
        p_ltp_d = put_ltp - (pe_md.get("previous_close") or pe_md.get("close_price") or base_v.get("put_ltp", put_ltp))

        result[strike] = {
            "strike": strike, "call_open": call_open, "put_open": put_open,
            "call_oi": call_oi, "call_oi_chg": round(c_oi_5m, 2), "call_oi_chg_day": round(call_oi - base_v.get("call_oi", call_oi), 2),
            "call_oi_velocity": round(c_oi_5m - c_oi_prev5m, 2), "call_vol": call_vol, "put_vol": put_vol,
            "call_vol_oi": round(call_vol/call_oi, 2) if call_oi else 0, "call_iv": round(float(ce_gk.get("iv") or 0)*100, 2), 
            "call_ltp": call_ltp, "call_ltp_chg": round(call_ltp - prev_v.get("call_ltp", call_ltp), 2), "call_ltp_chg_day": round(c_ltp_d, 2),
            "call_delta": float(ce_gk.get("delta") or 0), "call_gamma": float(ce_gk.get("gamma") or 0), "call_theta": float(ce_gk.get("theta") or 0), "call_vega": float(ce_gk.get("vega") or 0),
            "call_gex": float(ce_gk.get("gamma") or 0) * call_oi * 25,
            
            "put_oi": put_oi, "put_oi_chg": round(p_oi_5m, 2), "put_oi_chg_day": round(put_oi - base_v.get("put_oi", put_oi), 2),
            "put_oi_velocity": round(p_oi_5m - p_oi_prev5m, 2), "put_vol_oi": round(put_vol/put_oi, 2) if put_oi else 0,
            "put_iv": round(float(pe_gk.get("iv") or 0)*100, 2), 
            "put_ltp": put_ltp, "put_ltp_chg": round(put_ltp - prev_v.get("put_ltp", put_ltp), 2), "put_ltp_chg_day": round(p_ltp_d, 2),
            "put_delta": float(pe_gk.get("delta") or 0), "put_gamma": float(pe_gk.get("gamma") or 0), "put_theta": float(pe_gk.get("theta") or 0), "put_vega": float(pe_gk.get("vega") or 0),
            "put_gex": float(pe_gk.get("gamma") or 0) * put_oi * 25
        }
    return result

def classify_strike_oi_flow(v, prev_spot, spot):
    c_c, cl, c_o = v.get("call_oi_chg", 0), v.get("call_ltp_chg", 0), v.get("call_oi", 0)
    p_c, pl, p_o = v.get("put_oi_chg", 0), v.get("put_ltp_chg", 0), v.get("put_oi", 0)
    THRESH = 2000 
    
    if c_c > THRESH and cl > 0: cf = ("LONG BUILDUP", "BULLISH", "🟢")
    elif c_c < -THRESH and cl > 0: cf = ("SHORT COVERING", "WEAK BULLISH", "📈")
    elif c_c > THRESH and cl <= 0: cf = ("SHORT BUILDUP", "BEARISH", "🔴")
    elif c_c < -THRESH and cl <= 0: cf = ("LONG UNWINDING", "WEAK BEARISH", "🟡")
    else: cf = ("STABLE", "NEUTRAL", "⚪")

    if p_c > THRESH and pl > 0: pf = ("LONG BUILDUP", "BEARISH", "🔴")
    elif p_c < -THRESH and pl > 0: pf = ("SHORT COVERING", "WEAK BEARISH", "🟡")
    elif p_c > THRESH and pl <= 0: pf = ("SHORT BUILDUP", "BULLISH", "✅")
    elif p_c < -THRESH and pl <= 0: pf = ("LONG UNWINDING", "WEAK BULLISH", "📈")
    else: pf = ("STABLE", "NEUTRAL", "⚪")

    total_chg = c_c + p_c
    net_note = "🔄 OI SHIFT: Money moving to CALL side." if c_c > 25000 and p_c < -25000 else "🔄 OI SHIFT: Money moving to PUT side." if p_c > 25000 and c_c < -25000 else "💥 BOTH SIDES ADDING OI: High uncertainty." if c_c > 25000 and p_c > 25000 else "🌀 BOTH SIDES EXITING: Position squareoff." if c_c < -25000 and p_c < -25000 else "— No significant OI flow this cycle."

    return ({"condition": cf[0], "signal": cf[1], "emoji": cf[2], "oi_chg_l": round(c_c/100000,2), "oi_total_l": round(c_o/100000,2)},
            {"condition": pf[0], "signal": pf[1], "emoji": pf[2], "oi_chg_l": round(p_c/100000,2), "oi_total_l": round(p_o/100000,2)},
            {"total_oi_chg_l": round(total_chg/100000,2), "total_oi_l": round((c_o+p_o)/100000,2), "net_note": net_note})

def get_activity(atm_strikes, idx):
    acts, thresh = [], 20000 if idx == "NIFTY" else 10000 if idx == "BANKNIFTY" else 5000
    for s_str, v in atm_strikes.items():
        s = float(s_str)
        if v["call_oi_chg"] > thresh: acts.append({"strike": s, "type": "CE", "trend": "BEAR", "ltp": v["call_ltp"], "oi_chg": v["call_oi_chg"], "note": "Heavy Resistance Added"})
        elif v["call_oi_chg"] < -thresh: acts.append({"strike": s, "type": "CE", "trend": "BULL", "ltp": v["call_ltp"], "oi_chg": v["call_oi_chg"], "note": "Resistance Unwinding"})
        if v["put_oi_chg"] > thresh: acts.append({"strike": s, "type": "PE", "trend": "BULL", "ltp": v["put_ltp"], "oi_chg": v["put_oi_chg"], "note": "Heavy Support Added"})
        elif v["put_oi_chg"] < -thresh: acts.append({"strike": s, "type": "PE", "trend": "BEAR", "ltp": v["put_ltp"], "oi_chg": v["put_oi_chg"], "note": "Support Unwinding"})
    return sorted(acts, key=lambda x: abs(x["oi_chg"]), reverse=True)[:4]

def get_migrations(atm_strikes):
    migs, ce_strikes, pe_strikes = [], sorted(atm_strikes.values(), key=lambda x: x["call_oi_chg"]), sorted(atm_strikes.values(), key=lambda x: x["put_oi_chg"])
    if ce_strikes and ce_strikes[0]["call_oi_chg"] < -25000 and ce_strikes[-1]["call_oi_chg"] > 25000: migs.append({"from": str(ce_strikes[0]["strike"]), "to": str(ce_strikes[-1]["strike"]), "type": "CALL", "volume": abs(ce_strikes[0]["call_oi_chg"]), "note": "Resistance shifting"})
    if pe_strikes and pe_strikes[0]["put_oi_chg"] < -25000 and pe_strikes[-1]["put_oi_chg"] > 25000: migs.append({"from": str(pe_strikes[0]["strike"]), "to": str(pe_strikes[-1]["strike"]), "type": "PUT", "volume": abs(pe_strikes[0]["put_oi_chg"]), "note": "Support shifting"})
    return migs

def get_pin_risk(chain, atm):
    closest, max_oi = None, 0
    for s_str, v in chain.items():
        s = float(s_str)
        if abs(s - atm) <= 150 and v["call_oi"] + v["put_oi"] > max_oi: max_oi, closest = v["call_oi"] + v["put_oi"], s
    if closest: return {"label": f"{closest} STRADDLE PIN", "score": 9.5, "desc": "Max pain concentration dragging price."}
    return {"label": "NO PIN RISK", "score": 0, "desc": "Market is clear"}

def get_analysis(mkt_state, pcr, vix, net_flow_l):
    return [
        {"title": "MARKET TREND", "status": mkt_state, "desc": "Primary trend based on Price Action + OI Flow"},
        {"title": "PCR SENTIMENT", "status": "BULLISH" if pcr > 1.0 else "BEARISH", "desc": f"Put-Call Ratio is currently at {pcr}"},
        {"title": "VOLATILITY (VIX)", "status": "ELEVATED" if vix > 15 else "STABLE", "desc": f"India VIX is trading at {vix}"},
        {"title": "SMART MONEY FLOW", "status": "LONG BUILDUP" if net_flow_l > 0 else "SHORT SELLING", "desc": f"Net OI Flow is {net_flow_l:+.1f}L contracts"}
    ]

def refresh(idx):
    load_token()
    if not token_store.get("access_token"):
        debug_status["last_error"] = "Token missing. Please login."
        return

    oi_cache[idx]["last_fetch"] = time.time()
    store = STORE[idx]
    step = INDICES[idx]["step"]

    try:
        spot = fetch_spot(idx)
        
        today_str = date.today().isoformat()
        cached_expiry = _EXPIRY_DAY_CACHE.get(f"{idx}_{today_str}")
        raw, expiry = [], cached_expiry
        
        if cached_expiry: 
            raw = fetch_chain_raw(idx, cached_expiry)
            
        if not raw:
            expiry = get_expiry(idx)
            if expiry:
                raw = fetch_chain_raw(idx, expiry)
                if raw: _EXPIRY_DAY_CACHE[f"{idx}_{today_str}"] = expiry
        
        if not raw:
            if os.path.exists(DATA_FILE):
                try:
                    with open(DATA_FILE, "r") as f:
                        full_cache = json.load(f)
                        if idx in full_cache and full_cache[idx].get("chain"):
                            oi_cache[idx]["data"] = full_cache[idx]
                            debug_status["last_error"] = f"[{idx}] API offline. Loaded cached data."
                            return
                except: pass
            err_msg = f"[{idx}] Upstox returned no chain data. Token expired or API limit reached."
            debug_status["last_error"] = err_msg
            if oi_cache[idx].get("data"): oi_cache[idx]["data"]["backend_error"] = err_msg
            else: oi_cache[idx]["data"] = {"backend_error": err_msg, "timestamp": datetime.now().isoformat()}
            return

        atm = round(round(spot/step)*step, 2)
        chain = process_chain(idx, raw, spot)
        if not chain: return

        max_pain = compute_max_pain(chain)
        atm_strikes = {str(s):v for s,v in chain.items() if abs(float(s)-atm) <= ATM_RANGE * step}
        
        total_call = sum(v["call_oi"] for v in chain.values())
        total_put  = sum(v["put_oi"]  for v in chain.values())
        pcr        = round(total_put/total_call,2) if total_call else 0
        
        prev_pcr = store["history"][-1]["pcr"] if store["history"] else pcr
        pcr_chg  = round(pcr - prev_pcr, 3)
        # PCR 5-min change (last entry vs 5-min-ago entry)
        pcr_5m_chg = 0
        if len(store["pcr_history"]) >= 2:
            pcr_5m_chg = round(store["pcr_history"][-1]["pcr"] - store["pcr_history"][-2]["pcr"], 3)
        # PCR day change (vs first entry of day)
        pcr_day_chg = 0
        if len(store["pcr_history"]) >= 1:
            pcr_day_chg = round(pcr - store["pcr_history"][0]["pcr"], 3)
        futures  = fetch_futures(spot, idx)
        vix      = fetch_vix()
        
        if store["baseline_vix"] is None and vix > 0: store["baseline_vix"] = vix

        candles_1m  = fetch_base_1m_candles(idx)
        levels_data = extract_levels(candles_1m, spot)
        
        if candles_1m:
            candle_cache_store[idx]["3m"]  = resample_candles(candles_1m, 3)[-60:]
            candle_cache_store[idx]["5m"]  = resample_candles(candles_1m, 5)[-60:]
            candle_cache_store[idx]["15m"] = resample_candles(candles_1m, 15)[-40:]

        cum_put_add = sum(v["put_oi_chg_day"] for v in chain.values())
        cum_call_add = sum(v["call_oi_chg_day"] for v in chain.values())
        cum_net_flow = cum_put_add - cum_call_add

        atm_float = float(atm)
        atm_v = {}
        for k, v in atm_strikes.items():
            if abs(float(k) - atm_float) < 0.1:
                atm_v = v; break

        current_straddle = atm_v.get("call_ltp", 0) + atm_v.get("put_ltp", 0)
        old_data = oi_cache[idx].get("data") or {}
        morning_straddle = old_data.get("intelligence", {}).get("morning_straddle")
        if morning_straddle is None and current_straddle > 0: morning_straddle = current_straddle
        straddle_decay = ((current_straddle - morning_straddle) / morning_straddle * 100) if morning_straddle and morning_straddle > 0 else 0

        # ── alerts MUST be defined before any code that uses it ───────────────
        alerts = []
        for s_str,v in chain.items():
            s = float(s_str)
            dist = s - spot
            if 0 < dist <= (step*3) and v["call_oi_chg"] < 0 and abs(v["call_oi_chg"]) > v["call_oi"]*0.05:
                alerts.append({"type":"BREAKOUT UP","icon":"⚡","message":"Res OI dropping"})
            if -(step*3) <= dist < 0 and v["put_oi_chg"] < 0 and abs(v["put_oi_chg"]) > v["put_oi"]*0.05:
                alerts.append({"type":"BREAKOUT DOWN","icon":"⚡","message":"Sup OI dropping"})

        # ── Open = High / Open = Low detection (index level) ──────────────────
        open_eq_high = open_eq_low = False
        if candles_1m:
            today_str_d = datetime.now().strftime("%Y-%m-%d")
            today_1m = [c for c in candles_1m if c["time"][:10] == today_str_d]
            if today_1m:
                day_open = today_1m[0]["open"]
                day_high = max(c["high"] for c in today_1m)
                day_low  = min(c["low"]  for c in today_1m)
                open_eq_high = abs(day_open - day_high) <= step * 0.1
                open_eq_low  = abs(day_open - day_low)  <= step * 0.1

        # ── Combined OI Analysis (4 cases) ─────────────────────────────────
        total_ce_chg_day = sum(v.get("call_oi_chg_day", 0) for v in chain.values())
        total_pe_chg_day = sum(v.get("put_oi_chg_day",  0) for v in chain.values())
        ce_up = total_ce_chg_day > 0
        pe_up = total_pe_chg_day > 0
        if ce_up and not pe_up:
            combined_oi_case = 1; combined_oi_label = "CASE 1: CE↑ PE↓ — BEARISH"
            combined_oi_desc = "Resistance building, support collapsing. Strong Sell bias."; combined_oi_col = "BEAR"
        elif not ce_up and pe_up:
            combined_oi_case = 2; combined_oi_label = "CASE 2: CE↓ PE↑ — BULLISH"
            combined_oi_desc = "Resistance breaking, support building. Strong Buy bias."; combined_oi_col = "BULL"
        elif ce_up and pe_up:
            combined_oi_case = 3; combined_oi_label = "CASE 3: CE↑ PE↑ — RANGE BOUND"
            combined_oi_desc = "Both sides adding OI. Market trapped in range. Avoid directional trades."; combined_oi_col = "NEUTRAL"
        else:
            combined_oi_case = 4; combined_oi_label = "CASE 4: CE↓ PE↓ — UNCERTAIN"
            combined_oi_desc = "Everyone exiting. Low conviction. Wait and watch."; combined_oi_col = "WARN"

        # ── Vega tracking per ATM strike (for Vega history table) ─────────────
        ist_hm = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%H:%M")
        atm_vega_row = {
            "time": ist_hm,
            "call_vega": round(atm_v.get("call_vega", 0), 2),
            "put_vega":  round(atm_v.get("put_vega",  0), 2),
            "diff":      round(atm_v.get("call_vega", 0) - abs(atm_v.get("put_vega", 0)), 2),
        }
        # Store vega history (last 30 entries)
        if "vega_history" not in store: store["vega_history"] = []
        # Only append if time changed
        if not store["vega_history"] or store["vega_history"][-1]["time"] != ist_hm:
            store["vega_history"].append(atm_vega_row)
            store["vega_history"] = store["vega_history"][-30:]

        prev_spot = store["history"][-1]["spot"] if store["history"] else spot
        oi_cond, oi_signal, oi_desc = price_oi_matrix(spot, prev_spot, chain, atm, idx)

        for s_str, v in atm_strikes.items():
            s = float(s_str)
            cf, pf, nf = classify_strike_oi_flow(v, prev_spot, spot)
            v["call_flow"], v["put_flow"], v["net_flow"] = cf, pf, nf

        ind_data = get_indicators(candle_cache_store[idx]["5m"])
        ind_data["tech"] = {
            "3m": compute_tf_signals(idx, candle_cache_store[idx]["3m"],  "3min"),
            "5m": compute_tf_signals(idx, candle_cache_store[idx]["5m"],  "5min"),
            "15m": compute_tf_signals(idx, candle_cache_store[idx]["15m"], "15min")
        }

        vwap_val = get_vwap(candle_cache_store[idx]["5m"])
        baseline_trend_val = vwap_val if vwap_val else ind_data["tech"]["15m"].get("ema15")
        vix_matrix = analyze_vix_price(spot, baseline_trend_val, vix, store["baseline_vix"])
        
        mkt_state = market_state(pcr, ind_data.get("adx"), oi_signal, alerts, vix)
        
        gex_data = [{"strike":float(s),"net_gex":v.get("call_gex",0) - v.get("put_gex",0)} for s,v in sorted(chain.items(), key=lambda x:float(x[0])) if abs(float(s)-atm) <= 10*step]
        gex_flip = min(gex_data, key=lambda x:abs(x["net_gex"])) if gex_data else None

        wall_shifts = []
        max_ce_strike = max(chain.items(), key=lambda x: x[1]["call_oi"])[0] if chain else None
        max_pe_strike = max(chain.items(), key=lambda x: x[1]["put_oi"])[0] if chain else None
        if store["prev_max_ce_strike"] is not None and max_ce_strike != store["prev_max_ce_strike"]:
            wall_shifts.append({"type": "CALL_WALL_SHIFT", "from": store["prev_max_ce_strike"], "to": max_ce_strike, "icon": "🔄", "message": f"Call wall shifted: {store['prev_max_ce_strike']} → {max_ce_strike}"})
            alerts.append({"type": "CALL WALL SHIFT", "icon": "🔄", "message": f"Resistance moved {store['prev_max_ce_strike']} → {max_ce_strike}"})
        if store["prev_max_pe_strike"] is not None and max_pe_strike != store["prev_max_pe_strike"]:
            wall_shifts.append({"type": "PUT_WALL_SHIFT", "from": store["prev_max_pe_strike"], "to": max_pe_strike, "icon": "🔄", "message": f"Put wall shifted: {store['prev_max_pe_strike']} → {max_pe_strike}"})
            alerts.append({"type": "PUT WALL SHIFT", "icon": "🔄", "message": f"Support moved {store['prev_max_pe_strike']} → {max_pe_strike}"})
        store["prev_max_ce_strike"], store["prev_max_pe_strike"] = max_ce_strike, max_pe_strike

        ist_ts = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%H:%M:%S")
        for a in alerts: store["alert_log"].append({"time": ist_ts, "type": a.get("type", "ALERT"), "msg":  a.get("message", ""), "icon": a.get("icon", "⚡")})
        store["alert_log"] = store["alert_log"][-100:]

        ist_hm = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%H:%M")
        store["pcr_history"].append({"time": ist_hm, "pcr": pcr})
        store["pcr_history"] = store["pcr_history"][-40:]

        if current_straddle > 0:
            store["straddle_history"].append({"time": ist_hm, "value": round(current_straddle, 1)})
            store["straddle_history"] = store["straddle_history"][-40:]

        skew_data=[]
        for dist in [1,2,3]:
            c_strike, p_strike = atm+dist*step, atm-dist*step
            ce, pe = chain.get(str(c_strike),{}), chain.get(str(p_strike),{})
            if ce.get("call_iv") and pe.get("put_iv"): skew_data.append({"dist":dist,"strike":c_strike,"call_iv":ce["call_iv"],"put_iv":pe["put_iv"],"skew":round(pe["put_iv"]-ce["call_iv"],2)})
        avg_skew=round(sum(x["skew"] for x in skew_data)/len(skew_data),2) if skew_data else 0
        iv_skew = {"data":skew_data,"avg_skew":avg_skew,"signal":"BEARISH SKEW — put IV elevated" if avg_skew>3 else "BULLISH SKEW — call IV elevated" if avg_skew<-3 else "NEUTRAL SKEW — balanced"}

        ce_by_oi, pe_by_oi = sorted(chain.items(), key=lambda x: x[1]["call_oi"], reverse=True), sorted(chain.items(), key=lambda x: x[1]["put_oi"],  reverse=True)
        top3_ce, top3_pe = sum(v["call_oi"] for _,v in ce_by_oi[:3]), sum(v["put_oi"]  for _,v in pe_by_oi[:3])
        atm_zone  = [v for sk,v in chain.items() if abs(float(sk)-atm)<=2*step]
        tot_ce_vol, tot_pe_vol = sum(v.get("call_vol",0) for v in chain.values()), sum(v.get("put_vol",0)  for v in chain.values())
        sm_bull, sm_bear = sum(v.get("put_oi_chg_day",0) for v in chain.values() if v.get("put_oi_chg_day",0)>0), sum(abs(v.get("call_oi_chg_day",0)) for v in chain.values() if v.get("call_oi_chg_day",0)<0)
        
        oi_analytics = {
            "ce_concentration_pct": round(top3_ce/total_call*100,1) if total_call else 0, "pe_concentration_pct": round(top3_pe/total_put*100,1) if total_put else 0,
            "top_ce_strike": float(ce_by_oi[0][0]) if ce_by_oi else 0, "top_pe_strike": float(pe_by_oi[0][0]) if pe_by_oi else 0,
            "ce_wall_pct": round(chain.get(str(int(float(ce_by_oi[0][0])) if ce_by_oi else 0),{}).get("call_oi",0)/total_call*100,1) if total_call else 0,
            "pe_wall_pct": round(chain.get(str(int(float(pe_by_oi[0][0])) if pe_by_oi else 0),{}).get("put_oi",0)/total_put*100,1) if total_put else 0,
            "net_delta": round(sum(v.get("call_delta",0)*v["call_oi"]*25 for v in chain.values()) - sum(abs(v.get("put_delta",0))*v["put_oi"]*25 for v in chain.values()), 0),
            "delta_bias": "BULLISH" if sum(v.get("call_delta",0)*v["call_oi"]*25 for v in chain.values()) - sum(abs(v.get("put_delta",0))*v["put_oi"]*25 for v in chain.values()) > 0 else "BEARISH",
            "oi_accel_signal": "ACCELERATING" if (sum(v.get("call_oi_velocity",0) for v in atm_zone)+sum(v.get("put_oi_velocity",0) for v in atm_zone))>0 else "DECELERATING",
            "total_ce_chg_atm": round(sum(v.get("call_oi_chg",0) for v in atm_zone)/100000,2),
            "total_pe_chg_atm": round(sum(v.get("put_oi_chg",0)  for v in atm_zone)/100000,2),
            "mkt_vol_oi_ce": round(tot_ce_vol/total_call,3) if total_call else 0, "mkt_vol_oi_pe": round(tot_pe_vol/total_put,3) if total_put else 0,
            "pcr_trend": "RISING" if len(store["pcr_history"]) >= 3 and store["pcr_history"][-1]["pcr"]-store["pcr_history"][-3]["pcr"] > 0.03 else "FALLING" if len(store["pcr_history"]) >= 3 and store["pcr_history"][-1]["pcr"]-store["pcr_history"][-3]["pcr"] < -0.03 else "FLAT",
            "straddle_vs_open": round(current_straddle-(morning_straddle or current_straddle),1),
            "sm_flow_bias": "BULLISH" if sm_bull>sm_bear else "BEARISH" if sm_bear>sm_bull else "NEUTRAL",
            "sm_bull_flow_l": round(sm_bull/100000,1), "sm_bear_flow_l": round(sm_bear/100000,1),
        }

        ind_data["tech"]["overall_bias"] = mkt_state
        ind_data["tech"]["confluence"] = "Aligned" if mkt_state.startswith("BULL") or mkt_state.startswith("BEAR") else "Mixed"
        fut_prem = futures - spot

        # ── Vega Trend outcome ─────────────────────────────────────────────
        vega_hist = store.get("vega_history", [])
        vega_trend_label = "WAITING"
        vega_trend_col   = "NEUTRAL"
        if len(vega_hist) >= 2:
            cv_now = vega_hist[-1]["call_vega"]
            pv_now = abs(vega_hist[-1]["put_vega"])
            diff_now  = vega_hist[-1]["diff"]
            diff_prev = vega_hist[-2]["diff"]
            # Call Vega > Put Vega → more call premium → bullish expectation
            if diff_now < -20:
                vega_trend_label = "BULLISH"; vega_trend_col = "BULL"
            elif diff_now > 20:
                vega_trend_label = "BEARISH"; vega_trend_col = "BEAR"
            else:
                vega_trend_label = "NEUTRAL"; vega_trend_col = "NEUTRAL"

        intelligence = {
            "cycle_count": len(store["history"]), "market_state": mkt_state, "oi_matrix_condition": oi_cond, "oi_matrix_signal": oi_signal, "oi_matrix_desc": oi_desc,
            "pcr_zone": "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.8 else "NEUTRAL", "levels": levels_data, "alerts": alerts, 
            "gex": {"profile": gex_data[:11], "flip_zone": gex_flip["strike"] if gex_flip else "—"}, "skew": iv_skew,
            "future_bias": "LONG BUILDUP" if fut_prem > (step * 0.2) else "SHORT BUILDUP" if fut_prem < -(step * 0.1) else "NEUTRAL",
            "future_desc": f"{fut_prem:+.1f} pts premium",
            "max_pain_signal": "BULLISH DRIFT" if (spot - max_pain) > (step * 0.5) else "BEARISH DRIFT" if (spot - max_pain) < -(step * 0.5) else "PINNED",
            "max_pain_desc": f"{abs(spot - max_pain):.1f} pts from MP",
            "index_technicals": ind_data["tech"], "cumulative_net_flow_l": round(cum_net_flow / 100000, 2), "morning_straddle": morning_straddle, "straddle_decay": round(straddle_decay, 2), "vix_matrix": vix_matrix, "migrations": get_migrations(atm_strikes), "activity": get_activity(atm_strikes, idx), "analysis": get_analysis(mkt_state, pcr, vix, round(cum_net_flow / 100000, 2)), "pin_risk": get_pin_risk(chain, atm), "wall_shifts": wall_shifts, "max_ce_strike": max_ce_strike, "max_pe_strike": max_pe_strike, "pcr_history": store["pcr_history"][-20:], "straddle_history": store["straddle_history"][-20:], "oi_analytics": oi_analytics,
            # New additions
            "pcr_5m_chg": pcr_5m_chg, "pcr_day_chg": pcr_day_chg,
            "open_eq_high": open_eq_high, "open_eq_low": open_eq_low,
            "combined_oi_case": combined_oi_case, "combined_oi_label": combined_oi_label,
            "combined_oi_desc": combined_oi_desc, "combined_oi_col": combined_oi_col,
            "total_ce_chg_day": round(total_ce_chg_day/100000, 2),
            "total_pe_chg_day": round(total_pe_chg_day/100000, 2),
            # Vega history + trend
            "vega_history": vega_hist[-20:],
            "vega_trend_label": vega_trend_label,
            "vega_trend_col": vega_trend_col,
        }
        
        # Add vega values to atm_strikes for per-option display
        for s_str, v in atm_strikes.items():
            v["call_vega_val"] = round(v.get("call_vega", 0), 3)
            v["put_vega_val"]  = round(v.get("put_vega",  0), 3)
            v["vega_diff"]     = round(v.get("call_vega", 0) - abs(v.get("put_vega", 0)), 3)

        greeks = {
            "delta_call": {"val": atm_v.get("call_delta", 0), "desc": "CE Delta"},
            "delta_put":  {"val": atm_v.get("put_delta",  0), "desc": "PE Delta"},
            "gamma":  {"val": atm_v.get("call_gamma", 0), "desc": "Gamma (CE=PE at ATM)"},
            "theta":  {"val": atm_v.get("call_theta", 0), "desc": "Theta (daily decay ₹)"},
            "vega":   {"val": atm_v.get("call_vega",  0), "desc": "Vega (IV sensitivity)"},
            "ce_iv":  {"val": atm_v.get("call_iv", 0),   "desc": "CE Implied Volatility %"},
            "pe_iv":  {"val": atm_v.get("put_iv",  0),   "desc": "PE Implied Volatility %"},
        }
        # Open=High/Low for ATM options
        atm_ce_open_eq_high = atm_v.get("call_open", 0) > 0 and abs(atm_v.get("call_ltp", 0) - atm_v.get("call_open", 0)) < 0.5
        atm_pe_open_eq_high = atm_v.get("put_open",  0) > 0 and abs(atm_v.get("put_ltp",  0) - atm_v.get("put_open",  0)) < 0.5
        atm_ce_open_eq_low  = atm_v.get("call_open", 0) > 0 and atm_v.get("call_ltp", 0) <= atm_v.get("call_open", 0) * 0.98
        atm_pe_open_eq_low  = atm_v.get("put_open",  0) > 0 and atm_v.get("put_ltp",  0) <= atm_v.get("put_open",  0) * 0.98
        intelligence["atm_ce_open_eq_high"] = atm_v.get("call_ltp", 0) >= atm_v.get("call_open", 0) * 0.999 and atm_v.get("call_open", 0) > 0
        intelligence["atm_pe_open_eq_high"] = atm_v.get("put_ltp", 0)  >= atm_v.get("put_open",  0) * 0.999 and atm_v.get("put_open",  0) > 0
        adx_top, pdi_top, ndi_top = calc_adx_full(candle_cache_store[idx]["5m"], 14)
        ind_data["adx"] = adx_top; ind_data["pdi"] = pdi_top; ind_data["ndi"] = ndi_top

        data = {
            "backend_error": None, "spot": spot, "futures": futures, "premium": round(futures-spot,2),
            "atm": atm, "pcr": pcr, "pcr_chg": pcr_chg, "vix": vix, "max_pain": max_pain, "expiry": expiry, 
            "total_call_oi": total_call, "total_put_oi": total_put, "indicators": ind_data, "intelligence": intelligence,
            "atm_strikes": atm_strikes, "chain": chain, "greeks": greeks, "timestamp": datetime.now().isoformat(), "index_name": idx
        }
        oi_cache[idx]["data"] = data
        
        chain_snapshot = {str(s): {"call_oi": v["call_oi"], "put_oi": v["put_oi"], "call_ltp": v["call_ltp"], "put_ltp": v["put_ltp"]} for s,v in chain.items()}
        store["history"].append({"ts": time.time(), "chain": chain_snapshot, "spot": spot, "pcr": pcr})
        store["history"] = [x for x in store["history"] if time.time() - x["ts"] <= 900]
        
        try:
            full_cache = {}
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, "r") as f: full_cache = json.load(f)
            full_cache[idx] = data
            with open(DATA_FILE, "w") as f: json.dump(full_cache, f)
        except: pass
        try: save_server_state()
        except: pass

        debug_status["last_error"] = f"[{idx}] OK {datetime.now().strftime('%H:%M:%S')} Spot={spot} PCR={pcr}"
        
        try: process_telegram_alerts(idx, alerts, data, atm_strikes, atm)
        except Exception as e: print(f"Telegram Alert failed for {idx}: {e}")
        
    except Exception as e:
        debug_status["last_error"] = f"CRASH in {idx}: {str(e)}"
        if oi_cache[idx].get("data"): oi_cache[idx]["data"]["backend_error"] = f"Crash: {str(e)}"
        else: oi_cache[idx]["data"] = {"backend_error": f"Crash: {str(e)}", "timestamp": datetime.now().isoformat()}

def loop():
    while True:
        cycle_start = time.time()
        for idx in INDICES.keys():
            try: refresh(idx)
            except Exception as e: pass
            time.sleep(1)
        elapsed = time.time() - cycle_start
        time.sleep(max(10, CACHE_TTL - elapsed))

@app.route("/")
def dashboard(): return send_file("dashboard.html")

@app.route('/gallery')
def gallery():
    os.makedirs("static/screenshots", exist_ok=True)
    files = glob.glob("static/screenshots/*.png")
    files.sort(key=os.path.getmtime, reverse=True)
    html = """<html><head><title>OI Snap Gallery</title><style>body { background: #07090c; color: #c9d1d9; font-family: sans-serif; text-align: center; }.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; padding: 20px; }.card { background: #0f1319; border: 1px solid #212836; border-radius: 10px; padding: 10px; }img { width: 100%; border-radius: 5px; cursor: pointer; transition: 0.2s; }img:hover { transform: scale(1.02); box-shadow: 0 0 15px rgba(0, 184, 255, 0.4); }a { color: #00b8ff; text-decoration: none; font-weight: bold; }</style></head><body><h1>📸 Automated Screenshot Gallery</h1><p><a href="/">← Back to Live Dashboard</a></p><div class="grid">"""
    if not files: html += "<h3>No screenshots taken yet. Waiting for the first 5-minute cycle...</h3>"
    for f in files: html += f'''<div class="card"><h4 style="margin-top:5px; color:#8b949e">{os.path.basename(f)}</h4><a href="/static/screenshots/{os.path.basename(f)}" target="_blank"><img src="/static/screenshots/{os.path.basename(f)}" loading="lazy"></a></div>'''
    html += "</div></body></html>"
    return html

@app.route('/static/screenshots/<filename>')
def serve_screenshot(filename): return send_from_directory('static/screenshots', filename)

@app.route("/oi/json")
def oi_json():
    idx = request.args.get("idx", "NIFTY")
    if idx not in INDICES: idx = "NIFTY"
    
    last_fetch = oi_cache[idx].get("last_fetch", 0)
    if time.time() - last_fetch > 240: 
        if fetch_locks[idx].acquire(blocking=False):
            def bg_ref():
                try: refresh(idx)
                finally: fetch_locks[idx].release()
            threading.Thread(target=bg_ref, daemon=True).start()

    d = oi_cache[idx].get("data")
    if not d and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                d = json.load(f).get(idx)
                if d: oi_cache[idx]["data"] = d
        except: pass
    if not d: return jsonify({"error": f"Data Empty for {idx}. [Diagnostic: {debug_status.get('last_error', 'Unknown Error')}] — Click login."})
    return jsonify(d)

@app.route("/oi/histogram")
def histogram():
    idx = request.args.get("idx", "NIFTY")
    d = oi_cache[idx if idx in INDICES else "NIFTY"].get("data")
    if not d or not d.get("chain"): return jsonify([])
    atm, step = float(d["atm"]), float(INDICES[idx if idx in INDICES else "NIFTY"]["step"])
    return jsonify(sorted([v for s,v in d["chain"].items() if abs(float(s)-atm) <= ATM_RANGE * step], key=lambda x: float(x.get("strike", 0))))

@app.route("/oi/alert_log")
def alert_log_route(): return jsonify(list(reversed(STORE[request.args.get("idx", "NIFTY")].get("alert_log", []))))

@app.route("/oi/pcr_history")
def pcr_history_route(): return jsonify(STORE[request.args.get("idx", "NIFTY")].get("pcr_history", []))

@app.route("/oi/debug")
def oi_debug():
    ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    info = {
        "server_time_ist": ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        "last_error": debug_status.get("last_error"),
        "token_loaded": bool(token_store.get("access_token")),
        "indices": {}
    }
    for idx in INDICES:
        d, cs, hist = oi_cache[idx].get("data") or {}, candle_cache_store[idx], STORE[idx].get("history", [])
        info["indices"][idx] = {
            "has_data": bool(d), "data_timestamp": d.get("timestamp", "—"), "spot": d.get("spot", 0), "pcr": d.get("pcr", 0),
            "backend_error": d.get("backend_error"), "history_count": len(hist),
            "oldest_history_s": round(time.time() - hist[0]["ts"]) if hist else None,
            "newest_history_s": round(time.time() - hist[-1]["ts"]) if hist else None,
            "expiry_cached": _EXPIRY_DAY_CACHE.get(f"{idx}_{date.today().isoformat()}"),
            "candles_5m": len(cs.get("5m", [])), "last_fetch_ago_s": round(time.time() - oi_cache[idx].get("last_fetch", 0))
        }
    return jsonify(info)

@app.route("/login")
def login(): return redirect(f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URI}")

@app.route("/callback")
def callback():
    code = request.args.get("code")
    resp = requests.post("https://api.upstox.com/v2/login/authorization/token", data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"}, headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    data = resp.json()
    if "access_token" not in data: return f"<h2>Login Failed</h2><a href='/login'>Try again</a>"
    save_token(data.get("access_token"))
    send_telegram_alert("✅ <b>Upstox Login Successful!</b> Triple Engine is tracking.")
    threading.Thread(target=lambda: [refresh(idx) for idx in INDICES], daemon=True).start()
    return """<html><body style="font-family:sans-serif;background:#0a0c10;color:#00e676;padding:40px"><h2>✅ Login Successful!</h2><p><a href="/" style="color:#40c4ff">→ Open Dashboard</a></p><script>setTimeout(()=>window.location.href="/",2000)</script></body></html>"""

if __name__ == "__main__": app.run(host="0.0.0.0", port=5000, debug=False)