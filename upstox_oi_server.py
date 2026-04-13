"""
====================================================
  MULTI-ASSET OI SERVER — Triple Engine Architecture
  Supports: NIFTY 50, BANK NIFTY, SENSEX
====================================================
"""

import os, csv, time, math, threading, json, urllib.parse, traceback, glob
from datetime import datetime, date, timedelta
from flask import Flask, jsonify, request, redirect, send_file, send_from_directory
from flask_cors import CORS
from autosnap import start_auto_snapper
import requests

app = Flask(__name__)
CORS(app)

start_auto_snapper()

@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

API_KEY      = "48131639-7647-4f99-84e2-6113734955ce"
API_SECRET   = "0j2fmzd437"
REDIRECT_URI = "https://nifty-oi.onrender.com/callback"

# Kept empty so the LOGIN button works!
MANUAL_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIxOTI5MDEiLCJqdGkiOiI2OWRjOWY5NjhmNDVmNDU3Y2EwNzQ3OTAiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzc2MDY2NDU0LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NzYxMTc2MDB9.NCOhEsBoNVWgDiaxbsRA51yQ_pUbwvO0LLBXC1OqeS0"


TELEGRAM_BOT_TOKEN = "8709594892:AAGcSqRJLvSr-gX405Nbp3LQ0kJPghYPax4"  
TELEGRAM_CHAT_ID   = "7851805837"     

CACHE_TTL    = 120  
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

EXPIRY_CACHE = {"NIFTY": None, "BANKNIFTY": None, "SENSEX": None}

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

oi_cache = {idx: {"data": None} for idx in INDICES}
candle_cache_store = {idx: {"1m": [], "3m": [], "15m": []} for idx in INDICES}

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
        print("State save failed safely:", e)

load_server_state()

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN: 
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try: 
        requests.post(url, json=payload, timeout=5)
    except Exception: 
        pass

def generate_5min_summary(idx, data, atm_strikes, atm, is_boot=False):
    spot = data.get("spot", 0)
    pcr = data.get("pcr", 0)
    intel = data.get("intelligence", {})
    net_flow_l = intel.get("cumulative_net_flow_l", 0)
    flow_bias = "🟢 BULLISH" if net_flow_l > 0 else "🔴 BEARISH" if net_flow_l < 0 else "⚪ NEUTRAL"
    
    lvl = intel.get("levels", {})
    orb_sig = "🟡" if "RANGE" in lvl.get("orb_status", "") else "🟢" if "BULLISH" in lvl.get("orb_status", "") else "🔴"
    yest_sig = "🟡" if "RANGE" in lvl.get("yest_status", "") else "🟢" if "ABOVE" in lvl.get("yest_status", "") else "🔴"

    atm_float = float(atm)
    atm_v = {}
    for k, v in atm_strikes.items():
        if abs(float(k) - atm_float) < 0.1:
            atm_v = v
            break

    s_curr = atm_v.get("call_ltp", 0) + atm_v.get("put_ltp", 0)
    s_decay = intel.get("straddle_decay", 0)
    
    t5 = intel.get("index_technicals", {}).get("5m", {})
    vix_mat = intel.get("vix_matrix", {})
    
    boot_note = "<i>(Building baseline...)</i>" if is_boot else ""
    
    msg = (
        f"⏱ <b>5-MIN {idx} SCANNER</b>\n"
        f"🎯 <b>Spot:</b> ₹{spot} | <b>Open:</b> ₹{lvl.get('today_open', '-')}\n"
        f"{orb_sig} <b>ORB:</b> {lvl.get('orb_status', '-')} (Brk: {lvl.get('orb_time', '-')})\n"
        f"{yest_sig} <b>Yest H/L:</b> {lvl.get('yest_status', '-')} (Brk: {lvl.get('yest_time', '-')})\n"
        f"⚖️ <b>Straddle:</b> ₹{s_curr:.1f} ({s_decay:+.1f}% Day)\n"
        f"🔴 <b>ATM CE:</b> ₹{atm_v.get('call_ltp', 0):.1f} ({atm_v.get('call_ltp_chg', 0):+.1f} 5m) {boot_note}\n"
        f"🟢 <b>ATM PE:</b> ₹{atm_v.get('put_ltp', 0):.1f} ({atm_v.get('put_ltp_chg', 0):+.1f} 5m) {boot_note}\n"
        f"🌊 <b>Smart Flow:</b> {flow_bias} ({net_flow_l:+.1f}L Net)\n"
        f"📊 <b>VIX Matrix: {vix_mat.get('signal', 'WAITING')}</b>\n"
        f"↳ <i>{vix_mat.get('desc', 'Need more data')}</i>\n"
        f"━━━━━━━━━━━━━━━━\n\n"
    )
    
    def get_short_cond(flow):
        c = flow.get("condition", "")
        e = flow.get("emoji", "⚪")
        if "LONG BUILDUP" in c: return f"Long Build {e}"
        if "SHORT COVERING" in c: return f"Short Cover 📈"
        if "SHORT BUILDUP" in c or "WRITING" in c: return f"Writing {e}"
        if "LONG UNWINDING" in c or "EXIT" in c: return f"Unwinding {e}"
        if "FAKE" in c or "NO" in c: return f"Trap/Flat ⚠️"
        if "ADDITION" in c: return f"Heavy Add {e}"
        return f"Stable ⚪"

    strikes_to_show = [float(k) for k in atm_strikes.keys()]
    for s in sorted(strikes_to_show, reverse=True):
        if abs(s - atm) > 2 * INDICES[idx]["step"]: 
            continue
        v = {}
        for k_str, val in atm_strikes.items():
            if abs(float(k_str) - s) < 0.1:
                v = val
                break
        marker = " ◄ ATM" if abs(s - atm) < 0.1 else ""
        c_ltp, c_ltp_5m, c_ltp_d = v.get("call_ltp", 0), v.get("call_ltp_chg", 0), v.get("call_ltp_chg_day", 0)
        c_oi_5m, c_oi_d = v.get("call_oi_chg", 0)/100000, v.get("call_oi_chg_day", 0)/100000
        c_cond = get_short_cond(v.get("call_flow", {}))
        
        p_ltp, p_ltp_5m, p_ltp_d = v.get("put_ltp", 0), v.get("put_ltp_chg", 0), v.get("put_ltp_chg_day", 0)
        p_oi_5m, p_oi_d = v.get("put_oi_chg", 0)/100000, v.get("put_oi_chg_day", 0)/100000
        p_cond = get_short_cond(v.get("put_flow", {}))

        msg += f"🎯 <b>{int(s)}{marker}</b>\n"
        msg += f"🔴 <b>CE | {c_cond}</b>\n"
        msg += f"LTP: ₹{c_ltp:.1f} (5m: {c_ltp_5m:+.1f} | Day: {c_ltp_d:+.1f})\n"
        msg += f"OI : 5m: {c_oi_5m:+.2f}L | Day: {c_oi_d:+.2f}L\n\n"
        msg += f"🟢 <b>PE | {p_cond}</b>\n"
        msg += f"LTP: ₹{p_ltp:.1f} (5m: {p_ltp_5m:+.1f} | Day: {p_ltp_d:+.1f})\n"
        msg += f"OI : 5m: {p_oi_5m:+.2f}L | Day: {p_oi_d:+.2f}L\n"
        msg += f"〰️〰️〰️〰️〰️〰️〰️〰️\n"
        
    return msg.strip()

def process_telegram_alerts(idx, alerts, data, atm_strikes, atm):
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current_mins = ist_now.hour * 60 + ist_now.minute
    today_str = ist_now.strftime("%Y-%m-%d")
    
    holidays = [
        "2026-01-15", "2026-01-26", "2026-03-03", "2026-03-26", 
        "2026-03-31", "2026-04-03", "2026-04-14", "2026-05-01", 
        "2026-05-28", "2026-06-26", "2026-09-14", "2026-10-02", 
        "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25"
    ]
    
    if ist_now.weekday() >= 5 or today_str in holidays or not (540 <= current_mins <= 935):
        return
        
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
        
        if current_time - store["last_summary"] >= 290: 
            summary = generate_5min_summary(idx, data, atm_strikes, atm)
            send_telegram_alert(summary)
            store["last_summary"] = current_time
    except Exception: 
        pass

def save_token(token):
    token_store["access_token"] = token
    try:
        with open(TOKEN_FILE, "w") as f: 
            json.dump({"access_token": token}, f)
    except Exception: 
        pass

def load_token():
    if MANUAL_ACCESS_TOKEN and len(MANUAL_ACCESS_TOKEN) > 50:
        token_store["access_token"] = MANUAL_ACCESS_TOKEN
        return
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f: 
                token_store["access_token"] = json.load(f).get("access_token")
        except Exception: 
            pass

load_token()

def hdrs():
    load_token()
    return {"Authorization": f"Bearer {token_store['access_token']}", "Accept": "application/json", "Api-Version": "2.0"}

@app.route("/login")
def login(): 
    return redirect(f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URI}")

@app.route("/callback")
def callback():
    code = request.args.get("code")
    resp = requests.post("https://api.upstox.com/v2/login/authorization/token", data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"}, headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    data = resp.json()
    if "access_token" not in data:
        debug_status["last_error"] = f"Upstox Auth Rejected"
        return f"<h2>Login Failed</h2><a href='/login'>Try again</a>"
    
    save_token(data.get("access_token"))
    send_telegram_alert("✅ <b>Upstox Login Successful!</b> Triple Engine is tracking.")
    
    # Run the initial load in the background slowly so it doesn't crash the UI
    def run_init():
        for idx in INDICES: 
            refresh(idx)
            time.sleep(2)
            
    threading.Thread(target=run_init, daemon=True).start()
    
    return """<html><body style="font-family:sans-serif;background:#0a0c10;color:#00e676;padding:40px"><h2>✅ Login Successful!</h2><p><a href="/" style="color:#40c4ff">→ Open Dashboard</a></p><script>setTimeout(()=>window.location.href="/",2000)</script></body></html>"""

def fetch_spot(idx):
    sym = INDICES[idx]["key"]
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": sym}, headers=hdrs(), timeout=10)
        d = r.json().get("data", {})
        
        if not d and idx == "NIFTY":
            r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": "NSE_INDEX|NIFTY 50"}, headers=hdrs(), timeout=10)
            d = r.json().get("data", {})
            
        key = list(d.keys())[0] if d else None
        return float(d[key].get("last_price", 0)) if key else 0
    except Exception: 
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
        except Exception: 
            pass
    return round(spot * 1.005, 2)

def fetch_vix():
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": "NSE_INDEX|India VIX"}, headers=hdrs(), timeout=10)
        if r.status_code == 200 and r.json().get("data"):
            d = r.json()["data"]
            key = list(d.keys())[0]
            return float(d[key].get("last_price") or d[key].get("ltp") or 0)
    except Exception: 
        pass
    return 0

def fetch_base_1m_candles(idx):
    try:
        safe_key = urllib.parse.quote(INDICES[idx]["key"])
        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        url_hist = f"https://api.upstox.com/v2/historical-candle/{safe_key}/1minute/{to_date}/{from_date}"
        r_hist = requests.get(url_hist, headers=hdrs(), timeout=10)
        url_intra = f"https://api.upstox.com/v2/historical-candle/intraday/{safe_key}/1minute"
        r_intra = requests.get(url_intra, headers=hdrs(), timeout=10)
        candles = []
        if r_hist.status_code == 200: 
            candles += r_hist.json().get("data", {}).get("candles", [])
        if r_intra.status_code == 200: 
            candles += r_intra.json().get("data", {}).get("candles", [])
        unique = {}
        for c in candles:
            if len(c) >= 5: 
                unique[c[0]] = {"time": c[0], "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4]), "vol": float(c[5]) if len(c)>5 else 0}
        res = list(unique.values())
        res.sort(key=lambda x: x["time"])
        return res
    except Exception: 
        return []

def resample_candles(candles_1m, tf):
    if not candles_1m: return []
    res = []; cg = []; ct = None
    for c in candles_1m:
        try:
            dt = datetime.strptime(c["time"][:16], "%Y-%m-%dT%H:%M")
            gt = dt.replace(minute=(dt.minute // tf) * tf, second=0, microsecond=0)
            if ct is None: 
                ct = gt
            if gt == ct: 
                cg.append(c)
            else:
                res.append({"time": ct.isoformat(), "open": cg[0]["open"], "high": max(x["high"] for x in cg), "low": min(x["low"] for x in cg), "close": cg[-1]["close"], "vol": sum(x.get("vol", 0) for x in cg)})
                cg = [c]
                ct = gt
        except Exception: 
            pass
    if cg: 
        res.append({"time": ct.isoformat(), "open": cg[0]["open"], "high": max(x["high"] for x in cg), "low": min(x["low"] for x in cg), "close": cg[-1]["close"], "vol": sum(x.get("vol", 0) for x in cg)})
    return res

def get_valid_expiry_list(idx):
    sym = INDICES[idx]["key"]
    try:
        r = requests.get("https://api.upstox.com/v2/option/contract", params={"instrument_key": sym}, headers=hdrs(), timeout=5)
        if r.status_code == 200:
            data = r.json().get("data", [])
            exps = set()
            for i in data:
                if isinstance(i, str): exps.add(i)
                elif isinstance(i, dict) and i.get("expiry"): exps.add(i.get("expiry"))
            today_str = date.today().strftime("%Y-%m-%d")
            valid = sorted([e for e in exps if e >= today_str])
            if valid: return valid
    except: pass
    today = date.today()
    return [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(8)]

def find_valid_expiry(idx):
    if EXPIRY_CACHE[idx] and EXPIRY_CACHE[idx] >= date.today().strftime("%Y-%m-%d"):
        raw = fetch_chain(idx, EXPIRY_CACHE[idx])
        if raw: return EXPIRY_CACHE[idx], raw
        
    dates_to_test = get_valid_expiry_list(idx)
    for test_date in dates_to_test[:4]: 
        time.sleep(0.4) 
        raw = fetch_chain(idx, test_date)
        if raw:
            EXPIRY_CACHE[idx] = test_date
            return test_date, raw
            
    return None, []

def fetch_chain(idx, expiry):
    sym = INDICES[idx]["key"]
    for attempt in range(3): 
        try:
            r = requests.get("https://api.upstox.com/v2/option/chain", params={"instrument_key": sym, "expiry_date": expiry}, headers=hdrs(), timeout=5)
            if r.status_code == 429: 
                time.sleep(1) 
                continue
            if r.status_code == 200 and r.json().get("data"):
                return r.json().get("data", [])
                
            if idx == "NIFTY":
                r = requests.get("https://api.upstox.com/v2/option/chain", params={"instrument_key": "NSE_INDEX|NIFTY 50", "expiry_date": expiry}, headers=hdrs(), timeout=5)
                if r.status_code == 429:
                    time.sleep(1)
                    continue
                if r.status_code == 200 and r.json().get("data"):
                    INDICES["NIFTY"]["key"] = "NSE_INDEX|NIFTY 50"
                    return r.json().get("data", [])
            break
        except: 
            time.sleep(1)
    return []

def compute_max_pain(chain):
    strikes = sorted([float(k) for k in chain.keys()])
    if not strikes: return 0
    min_loss = float("inf")
    mp = strikes[0]
    for s in strikes:
        loss = sum(v["call_oi"]*(s-float(k)) if float(k)<s else v["put_oi"]*(float(k)-s) if float(k)>s else 0 for k,v in chain.items())
        if loss < min_loss: 
            min_loss = loss
            mp = s
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

def calc_ema(prices, period):
    if not prices: return None
    if len(prices) < period: 
        return round(sum(prices) / len(prices), 2)
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]: 
        ema = p * k + ema * (1 - k)
    return round(ema, 2)

def calc_ema_array(prices, period):
    if not prices: return []
    if len(prices) < period: 
        return [round(sum(prices[:i+1])/(i+1), 2) for i in range(len(prices))]
    emas = [None] * (period - 1)
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    emas.append(round(ema, 2))
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
        emas.append(round(ema, 2))
    return emas

def calc_supertrend(candles, period=7, multiplier=3.0):
    if len(candles) < period + 1: return None, None
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(candles))]
    atr = sum(trs[-period:]) / period
    hl2 = (highs[-1] + lows[-1]) / 2
    direction = "BULLISH" if closes[-1] > (hl2 - multiplier * atr) else "BEARISH"
    st_val = round((hl2 - multiplier * atr) if direction == "BULLISH" else (hl2 + multiplier * atr), 2)
    return direction, st_val

def calc_rsi(closes, p=14):
    if len(closes) < p+1: return None
    gains = [max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    losses = [max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    ag = sum(gains[:p])/p
    al = sum(losses[:p])/p
    for i in range(p, len(gains)):
        ag = (ag*(p-1)+gains[i])/p
        al = (al*(p-1)+losses[i])/p
    return 100.0 if al==0 else round(100-100/(1+ag/al), 2)

def calc_rsi_array(closes, p=14):
    if len(closes) < p+1: return []
    gains = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    if sum(losses[:p]) == 0: 
        return [None]*p + [100.0] * (len(gains) - p + 1)
    ag = sum(gains[:p])/p
    al = sum(losses[:p])/p
    rsis = [None]*p
    rsis.append(100.0 if al==0 else 100 - (100/(1+ag/al)))
    for i in range(p, len(gains)):
        ag = (ag*(p-1) + gains[i])/p
        al = (al*(p-1) + losses[i])/p
        rsis.append(100.0 if al==0 else 100 - (100/(1+ag/al)))
    return rsis

def calc_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return None, None, None
    ema_fast = calc_ema_array(prices, fast)
    ema_slow = calc_ema_array(prices, slow)
    macd_line = [
        round(f - s, 4) if f is not None and s is not None else None
        for f, s in zip(ema_fast, ema_slow)
    ]
    valid_macd = [x for x in macd_line if x is not None]
    if len(valid_macd) < signal:
        return None, None, None
    sig_arr = calc_ema_array(valid_macd, signal)
    macd_val = valid_macd[-1]
    sig_val  = sig_arr[-1] if sig_arr else None
    hist_val = round(macd_val - sig_val, 4) if sig_val is not None else None
    return round(macd_val, 2), round(sig_val, 2) if sig_val is not None else None, round(hist_val, 2) if hist_val is not None else None

def calc_adx(candles, p=14):
    if not candles or len(candles) < p + 2: return None
    trl, pdml, ndml = [], [], []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        ph, pl = candles[i-1]["high"], candles[i-1]["low"]
        trl.append(max(h-l, abs(h-pc), abs(l-pc)))
        pdml.append(max(h-ph, 0) if (h-ph) > (pl-l) else 0)
        ndml.append(max(pl-l, 0) if (pl-l) > (h-ph) else 0)
    
    def sm(lst, p):
        if sum(lst[:p]) == 0: return [0]*len(lst)
        s = sum(lst[:p])
        r = [s]
        for i in range(p, len(lst)): 
            s = s - s/p + lst[i]
            r.append(s)
        return r
        
    atr = sm(trl, p)
    pDM = sm(pdml, p)
    nDM = sm(ndml, p)
    dxl = []
    
    for i in range(len(atr)):
        if atr[i] == 0: continue
        pdi = 100 * pDM[i] / atr[i]
        ndi = 100 * nDM[i] / atr[i]
        dx = 100 * abs(pdi-ndi) / (pdi+ndi) if (pdi+ndi) else 0
        dxl.append((dx, pdi, ndi))
    if not dxl: return None
    return round(sum(x[0] for x in dxl[-p:]) / min(p, len(dxl)), 2)

def get_indicators(candles):
    if not candles or len(candles) < 16: 
        return {"rsi": None, "adx": None, "candle_count": len(candles) if candles else 0}
    closes = [c["close"] for c in candles]
    rsi_val = calc_rsi(closes, 14)
    adx_val = calc_adx(candles, 14)
    return {"rsi": rsi_val, "adx": adx_val, "candle_count": len(candles)}

def extract_levels(candles, spot):
    if not candles: return {}
    dates = sorted(list(set([c["time"][:10] for c in candles])))
    if not dates: return {}
    
    today_str = dates[-1]
    yest_str = dates[-2] if len(dates) > 1 else None
    
    today_candles = [c for c in candles if c["time"][:10] == today_str]
    yest_candles = [c for c in candles if c["time"][:10] == yest_str] if yest_str else []
    
    yest_high = max([c["high"] for c in yest_candles]) if yest_candles else None
    yest_low = min([c["low"] for c in yest_candles]) if yest_candles else None
    
    today_open = today_candles[0]["open"] if today_candles else None
    
    orb_high, orb_low = None, None
    orb_candles = [c for c in today_candles if "09:15" <= c["time"][11:16] <= "09:29"]
    if len(orb_candles) >= 1:
        orb_high = max([c["high"] for c in orb_candles])
        orb_low = min([c["low"] for c in orb_candles])
        
    orb_status = "IN ORB RANGE"
    orb_time = "-"
    if orb_high and orb_low:
        if spot > orb_high: orb_status = "ABOVE ORB (BULLISH)"
        elif spot < orb_low: orb_status = "BELOW ORB (BEARISH)"
        
        for c in today_candles:
            t = c["time"][11:16]
            if t > "09:29":
                if c["high"] > orb_high and orb_status.startswith("ABOVE"):
                    orb_time = t
                    break
                elif c["low"] < orb_low and orb_status.startswith("BELOW"):
                    orb_time = t
                    break

    yest_status = "INSIDE YEST RANGE"
    yest_time = "-"
    if yest_high and yest_low:
        if spot > yest_high: yest_status = "ABOVE YEST HIGH"
        elif spot < yest_low: yest_status = "BELOW YEST LOW"
        
        for c in today_candles:
            t = c["time"][11:16]
            if c["high"] > yest_high and yest_status.startswith("ABOVE"):
                yest_time = t
                break
            elif c["low"] < yest_low and yest_status.startswith("BELOW"):
                yest_time = t
                break
                
    return {
        "today_open": today_open,
        "yest_high": yest_high, "yest_low": yest_low,
        "orb_high": orb_high, "orb_low": orb_low,
        "orb_status": orb_status, "orb_time": orb_time,
        "yest_status": yest_status, "yest_time": yest_time
    }

def compute_tf_signals(idx, candles, label, st_period, st_multiplier):
    if not candles or len(candles) < 15: 
        return {"label": label, "candle_count": len(candles) if candles else 0, "ts_start": "-", "ts_pull": "-", "ts_cont": "-", "ts_st": "-"}
    
    store = STORE[idx]
    vwap = get_vwap(candles)
    closes = [c["close"] for c in candles]
    times = [c["time"] for c in candles]
    
    ema7_arr = calc_ema_array(closes, 7)
    ema15_arr = calc_ema_array(closes, 15)
    
    trend_start, pull_time, cont_time, st_time = "-", "-", "-", "-"
    is_bull = None
    await_pull_b = await_pull_s = False
    curr_st = None
    
    for i in range(15, len(candles)):
        e7, e15 = ema7_arr[i], ema15_arr[i]
        if e7 is None or e15 is None: 
            continue
        c_close = closes[i]
        
        try:
            d_str = times[i][:10]
            t_str = times[i][11:16]
            c_time = f"{d_str[8:10]}-{d_str[5:7]} {t_str}" 
        except Exception: 
            c_time = "-"

        curr_bull = e7 > e15
        
        if is_bull is None:
            is_bull = curr_bull
            trend_start = c_time
            if curr_bull: 
                await_pull_b = True
            else: 
                await_pull_s = True
        elif is_bull != curr_bull:
            trend_start = c_time
            pull_time, cont_time = "-", "-"
            is_bull = curr_bull
            if curr_bull: 
                await_pull_b = True
            else: 
                await_pull_s = True
                
        if is_bull:
            if await_pull_b and (c_close < e7 or c_close < e15):
                pull_time = c_time
                cont_time = "..."
                await_pull_b = False
            elif not await_pull_b and c_close > e7:
                cont_time = c_time
                await_pull_b = True
        else:
            if await_pull_s and (c_close > e7 or c_close > e15):
                pull_time = c_time
                cont_time = "..."
                await_pull_s = False
            elif not await_pull_s and c_close < e7:
                cont_time = c_time
                await_pull_s = True
                
        s_dir, _ = calc_supertrend(candles[:i+1], st_period, st_multiplier)
        if curr_st is None: 
            curr_st = s_dir
            st_time = c_time
        elif curr_st != s_dir:
            st_time = c_time
            curr_st = s_dir

    ema7, ema15, price = ema7_arr[-1], ema15_arr[-1], closes[-1]
    st_dir, st_val = calc_supertrend(candles, st_period, st_multiplier)
    
    trend = "N/A"
    if ema7 and ema15:
        if price > ema7 and ema7 > ema15: 
            trend = "STRONG BULLISH"
        elif price > ema7 and ema7 < ema15: 
            trend = "RECOVERING"
        elif price < ema7 and ema7 > ema15: 
            trend = "MILD BEARISH"
        else: 
            trend = "STRONG BEARISH"

    rsis = calc_rsi_array(closes, 14)
    curr_rsi = round(rsis[-1], 2) if rsis and rsis[-1] is not None else 0
    prev_rsi = round(rsis[-2], 2) if rsis and len(rsis) > 1 and rsis[-2] is not None else curr_rsi
    
    base_r = store["baseline_rsi"].get(label)
    if not base_r and curr_rsi > 0:
        store["baseline_rsi"][label] = curr_rsi
        base_r = curr_rsi
        
    rsi_5m_chg = round(curr_rsi - prev_rsi, 2) if curr_rsi else 0
    rsi_day_chg = round(curr_rsi - base_r, 2) if curr_rsi and base_r else 0

    macd_val, macd_sig, macd_hist = calc_macd(closes)

    return {
        "label": label, "candle_count": len(candles), "current_price": round(price, 2) if price else None, 
        "ema7": ema7, "ema15": ema15, "vwap": vwap, 
        "price_above_ema7": price > ema7 if ema7 else None, "price_above_ema15": price > ema15 if ema15 else None, 
        "ema7_above_ema15": ema7 > ema15 if ema7 and ema15 else None, "price_above_vwap": price > vwap if vwap else None,
        "trend": trend, "supertrend": st_dir, "supertrend_val": st_val, 
        "rsi": curr_rsi if curr_rsi > 0 else None,
        "rsi_5m_chg": rsi_5m_chg,
        "rsi_day_chg": rsi_day_chg,
        "macd": macd_val, "macd_signal": macd_sig, "macd_hist": macd_hist,
        "ts_start": trend_start, "ts_pull": pull_time, "ts_cont": cont_time, "ts_st": st_time
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
    if not baseline_trend_val or not base_vix or base_vix == 0: 
        return {"signal": "WAITING", "desc": "Need more data for baseline comparison"}
    price_up = spot > baseline_trend_val
    vix_up = vix > base_vix
    
    if price_up and vix_up: return {"signal": "STRONG BULLISH", "desc": "Price ↑ + VIX ↑ | Fear rising + Rally = Breakout expected."}
    elif price_up and not vix_up: return {"signal": "WEAK BULLISH", "desc": "Price ↑ + VIX ↓ | Move lacks power = Resistance might hold."}
    elif not price_up and vix_up: return {"signal": "STRONG BEARISH", "desc": "Price ↓ + VIX ↑ | Panic selling = Support might break."}
    elif not price_up and not vix_up: return {"signal": "WEAK BEARISH", "desc": "Price ↓ + VIX ↓ | Normal correction = Support might hold."}
    return {"signal": "NEUTRAL", "desc": "Market flat"}

def process_chain(idx, raw, spot):
    store = STORE[idx]
    history = store.setdefault("history", [])
    now = time.time()
    
    target_ts = now - 300
    prev_record = None
    if history:
        prev_record = min(history, key=lambda x: abs(x["ts"] - target_ts))
        if now - history[0]["ts"] < 240: 
            prev_record = history[0]

    target_ts_v = now - 600
    prev2_record = None
    if len(history) >= 2:
        prev2_record = min(history, key=lambda x: abs(x["ts"] - target_ts_v))
        if prev2_record is prev_record:
            others = [h for h in history if h is not prev_record]
            prev2_record = min(others, key=lambda x: abs(x["ts"] - target_ts_v)) if others else None

    prev_chain = prev_record["chain"] if prev_record else {}
    prev2_chain = prev2_record["chain"] if prev2_record else {}
    base_chain = store["baseline_oi"]

    result = {}
    for item in raw:
        strike = float(item.get("strike_price",0))
        if not strike: continue
        ce, pe = item.get("call_options",{}), item.get("put_options",{})
        ce_md, pe_md = ce.get("market_data",{}), pe.get("market_data",{})
        ce_gk, pe_gk = ce.get("option_greeks",{}), pe.get("option_greeks",{})
        
        call_oi, put_oi = float(ce_md.get("oi",0) or 0), float(pe_md.get("oi",0) or 0)
        call_vol, put_vol = float(ce_md.get("volume",0) or 0), float(pe_md.get("volume",0) or 0)
        call_ltp, put_ltp = float(ce_md.get("ltp",0) or ce_md.get("last_price",0) or 0), float(pe_md.get("ltp",0) or pe_md.get("last_price",0) or 0)
        call_open = float(ce_md.get("open_price", 0))
        put_open  = float(pe_md.get("open_price", 0))
        
        prev_v = prev_chain.get(str(strike), {})
        base_v = base_chain.get(str(strike), {})

        c_oi_5m = call_oi - prev_v.get("call_oi", call_oi)
        p_oi_5m = put_oi - prev_v.get("put_oi", put_oi)

        prev2_v = prev2_chain.get(str(strike), {})
        c_oi_prev5m = prev_v.get("call_oi", call_oi) - prev2_v.get("call_oi", prev_v.get("call_oi", call_oi))
        p_oi_prev5m = prev_v.get("put_oi", put_oi)   - prev2_v.get("put_oi", prev_v.get("put_oi", put_oi))
        c_oi_velocity = round(c_oi_5m - c_oi_prev5m, 2)
        p_oi_velocity = round(p_oi_5m - p_oi_prev5m, 2)
        
        c_oi_d = call_oi - base_v.get("call_oi", call_oi)
        p_oi_d = put_oi - base_v.get("put_oi", put_oi)
        
        c_ltp_5m = call_ltp - prev_v.get("call_ltp", call_ltp)
        p_ltp_5m = put_ltp - prev_v.get("put_ltp", put_ltp)

        c_prev_close = ce_md.get("previous_close") or ce_md.get("close_price")
        p_prev_close = pe_md.get("previous_close") or pe_md.get("close_price")
        c_ltp_d = call_ltp - c_prev_close if c_prev_close else (call_ltp - base_v.get("call_ltp", call_ltp))
        p_ltp_d = put_ltp - p_prev_close if p_prev_close else (put_ltp - base_v.get("put_ltp", put_ltp))

        result[strike] = {
            "strike": strike,
            "call_open": call_open, "put_open": put_open,
            "call_oi": call_oi, "call_oi_chg": round(c_oi_5m, 2), "call_oi_chg_day": round(c_oi_d, 2),
            "call_oi_velocity": c_oi_velocity,
            "call_vol": call_vol, "put_vol": put_vol,
            "call_vol_oi": round(call_vol/call_oi, 2) if call_oi else 0,
            "call_iv": round(float(ce_gk.get("iv",0) or 0)*100,2), 
            "call_ltp": call_ltp, "call_ltp_chg": round(c_ltp_5m, 2), "call_ltp_chg_day": round(c_ltp_d, 2),
            "call_delta": float(ce_gk.get("delta",0) or 0), "call_gamma": float(ce_gk.get("gamma",0) or 0), 
            "call_theta": float(ce_gk.get("theta",0) or 0), "call_vega": float(ce_gk.get("vega",0) or 0),
            "call_gex": float(ce_gk.get("gamma",0) or 0) * call_oi * 25,
            
            "put_oi": put_oi, "put_oi_chg": round(p_oi_5m, 2), "put_oi_chg_day": round(p_oi_d, 2),
            "put_oi_velocity": p_oi_velocity,
            "put_vol_oi": round(put_vol/put_oi, 2) if put_oi else 0,
            "put_iv": round(float(pe_gk.get("iv",0) or 0)*100,2), 
            "put_ltp": put_ltp, "put_ltp_chg": round(p_ltp_5m, 2), "put_ltp_chg_day": round(p_ltp_d, 2),
            "put_delta": float(pe_gk.get("delta",0) or 0), "put_gamma": float(pe_gk.get("gamma",0) or 0), 
            "put_theta": float(pe_gk.get("theta",0) or 0), "put_vega": float(pe_gk.get("vega",0) or 0),
            "put_gex": float(pe_gk.get("gamma",0) or 0) * put_oi * 25
        }
        
    if len(store["baseline_oi"]) == 0 and result: 
        store["baseline_oi"] = {str(s): {"call_oi": v["call_oi"], "put_oi": v["put_oi"], "call_ltp": v["call_ltp"], "put_ltp": v["put_ltp"]} for s,v in result.items()}
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
    if c_c > 25000 and p_c < -25000: net_note = "🔄 OI SHIFT: Money moving to CALL side."
    elif p_c > 25000 and c_c < -25000: net_note = "🔄 OI SHIFT: Money moving to PUT side."
    elif c_c > 25000 and p_c > 25000: net_note = "💥 BOTH SIDES ADDING OI: High uncertainty."
    elif c_c < -25000 and p_c < -25000: net_note = "🌀 BOTH SIDES EXITING: Position squareoff."
    else: net_note = "— No significant OI flow this cycle."

    return (
        {"condition": cf[0], "signal": cf[1], "emoji": cf[2], "oi_chg_l": round(c_c/100000,2), "oi_total_l": round(c_o/100000,2)},
        {"condition": pf[0], "signal": pf[1], "emoji": pf[2], "oi_chg_l": round(p_c/100000,2), "oi_total_l": round(p_o/100000,2)},
        {"total_oi_chg_l": round(total_chg/100000,2), "total_oi_l": round((c_o+p_o)/100000,2), "net_note": net_note}
    )

def get_activity(atm_strikes, idx):
    acts = []
    thresh = 20000 if idx == "NIFTY" else 10000 if idx == "BANKNIFTY" else 5000
    
    for s_str, v in atm_strikes.items():
        s = float(s_str)
        if v["call_oi_chg"] > thresh:
            acts.append({"strike": s, "type": "CE", "trend": "BEAR", "ltp": v["call_ltp"], "oi_chg": v["call_oi_chg"], "note": "Heavy Resistance Added"})
        elif v["call_oi_chg"] < -thresh:
            acts.append({"strike": s, "type": "CE", "trend": "BULL", "ltp": v["call_ltp"], "oi_chg": v["call_oi_chg"], "note": "Resistance Unwinding"})
            
        if v["put_oi_chg"] > thresh:
            acts.append({"strike": s, "type": "PE", "trend": "BULL", "ltp": v["put_ltp"], "oi_chg": v["put_oi_chg"], "note": "Heavy Support Added"})
        elif v["put_oi_chg"] < -thresh:
            acts.append({"strike": s, "type": "PE", "trend": "BEAR", "ltp": v["put_ltp"], "oi_chg": v["put_oi_chg"], "note": "Support Unwinding"})
            
    acts.sort(key=lambda x: abs(x["oi_chg"]), reverse=True)
    return acts[:4]

def get_migrations(atm_strikes):
    migs = []
    ce_strikes = sorted(atm_strikes.values(), key=lambda x: x["call_oi_chg"])
    pe_strikes = sorted(atm_strikes.values(), key=lambda x: x["put_oi_chg"])
    
    if ce_strikes and ce_strikes[0]["call_oi_chg"] < -25000 and ce_strikes[-1]["call_oi_chg"] > 25000:
        migs.append({"from": str(ce_strikes[0]["strike"]), "to": str(ce_strikes[-1]["strike"]), "type": "CALL", "volume": abs(ce_strikes[0]["call_oi_chg"]), "note": "Resistance shifting"})
        
    if pe_strikes and pe_strikes[0]["put_oi_chg"] < -25000 and pe_strikes[-1]["put_oi_chg"] > 25000:
        migs.append({"from": str(pe_strikes[0]["strike"]), "to": str(pe_strikes[-1]["strike"]), "type": "PUT", "volume": abs(pe_strikes[0]["put_oi_chg"]), "note": "Support shifting"})
        
    return migs

def get_pin_risk(chain, atm):
    closest = None
    max_oi = 0
    for s_str, v in chain.items():
        s = float(s_str)
        if abs(s - atm) <= 150:
            tot = v["call_oi"] + v["put_oi"]
            if tot > max_oi:
                max_oi = tot
                closest = s
    if closest:
        return {"label": f"{closest} STRADDLE PIN", "score": 9.5, "desc": "Max pain concentration dragging price."}
    return {"label": "NO PIN RISK", "score": 0, "desc": "Market is clear"}

def get_analysis(mkt_state, pcr, vix, net_flow_l):
    return [
        {"title": "MARKET TREND", "status": mkt_state, "desc": "Primary trend based on Price Action + OI Flow"},
        {"title": "PCR SENTIMENT", "status": "BULLISH" if pcr > 1.0 else "BEARISH", "desc": f"Put-Call Ratio is currently at {pcr}"},
        {"title": "VOLATILITY (VIX)", "status": "ELEVATED" if vix > 15 else "STABLE", "desc": f"India VIX is trading at {vix}"},
        {"title": "SMART MONEY FLOW", "status": "LONG BUILDUP" if net_flow_l > 0 else "SHORT SELLING", "desc": f"Net OI Flow is {net_flow_l:+.1f}L contracts"}
    ]

# ══════════════════════════════════════════════════
#  MAIN REFRESH LOOP
# ══════════════════════════════════════════════════

def refresh(idx):
    load_token()
    if not token_store.get("access_token"):
        debug_status["last_error"] = "Token missing. Please login."
        return

    store = STORE[idx]
    step = INDICES[idx]["step"]

    try:
        spot   = fetch_spot(idx)
        time.sleep(0.4) 
        expiry, raw = find_valid_expiry(idx)
        
        if not raw:
            if os.path.exists(DATA_FILE):
                try:
                    with open(DATA_FILE, "r") as f:
                        full_cache = json.load(f)
                        if idx in full_cache and full_cache[idx].get("chain"):
                            oi_cache[idx]["data"] = full_cache[idx]
                            debug_status["last_error"] = f"[{idx}] API offline (Weekend/Limit). Loaded cached data."
                            return
                except: pass
                
            err_msg = f"[{idx}] Upstox returned no chain data. Token expired or API limit reached."
            debug_status["last_error"] = err_msg
            if oi_cache[idx].get("data"): oi_cache[idx]["data"]["backend_error"] = err_msg
            else: oi_cache[idx]["data"] = {"backend_error": err_msg, "timestamp": datetime.now().isoformat()}
            return

        atm   = round(round(spot/step)*step, 2)
        chain = process_chain(idx, raw, spot)
        if not chain: return

        max_pain = compute_max_pain(chain)
        atm_strikes = {str(s):v for s,v in chain.items() if abs(float(s)-atm) <= ATM_RANGE * step}
        
        total_call  = sum(v["call_oi"] for v in chain.values())
        total_put   = sum(v["put_oi"]  for v in chain.values())
        pcr         = round(total_put/total_call,2) if total_call else 0
        
        prev_pcr = store["history"][-1]["pcr"] if store["history"] else pcr
        pcr_chg  = round(pcr - prev_pcr, 3)
        time.sleep(0.4)
        futures  = fetch_futures(spot, idx)
        time.sleep(0.4)
        vix      = fetch_vix()
        
        if store["baseline_vix"] is None and vix > 0: store["baseline_vix"] = vix

        time.sleep(0.4)
        candles_1m  = fetch_base_1m_candles(idx)
        levels_data = extract_levels(candles_1m, spot)
        
        if candles_1m:
            candle_cache_store[idx]["3m"] = resample_candles(candles_1m, 3)[-60:]
            candle_cache_store[idx]["5m"] = resample_candles(candles_1m, 5)[-60:]
            candle_cache_store[idx]["15m"] = resample_candles(candles_1m, 15)[-40:]

        cum_put_add = sum(v["put_oi_chg_day"] for v in chain.values())
        cum_call_add = sum(v["call_oi_chg_day"] for v in chain.values())
        cum_net_flow = cum_put_add - cum_call_add

        atm_float = float(atm)
        atm_v = {}
        for k, v in atm_strikes.items():
            if abs(float(k) - atm_float) < 0.1:
                atm_v = v
                break

        current_straddle = atm_v.get("call_ltp", 0) + atm_v.get("put_ltp", 0)
        old_data = oi_cache[idx].get("data") or {}
        morning_straddle = old_data.get("intelligence", {}).get("morning_straddle")
        if morning_straddle is None and current_straddle > 0: morning_straddle = current_straddle
        straddle_decay = ((current_straddle - morning_straddle) / morning_straddle * 100) if morning_straddle and morning_straddle > 0 else 0

        alerts = []
        for s_str,v in chain.items():
            s = float(s_str)
            dist = s - spot
            if 0 < dist <= (step*3) and v["call_oi_chg"] < 0 and abs(v["call_oi_chg"]) > v["call_oi"]*0.05: alerts.append({"type":"BREAKOUT UP","icon":"⚡","message":f"Res OI dropping"})
            if -(step*3) <= dist < 0 and v["put_oi_chg"] < 0 and abs(v["put_oi_chg"]) > v["put_oi"]*0.05: alerts.append({"type":"BREAKOUT DOWN","icon":"⚡","message":f"Sup OI dropping"})

        prev_spot = store["history"][-1]["spot"] if store["history"] else spot
        oi_cond, oi_signal, oi_desc = price_oi_matrix(spot, prev_spot, chain, atm, idx)

        for s_str, v in atm_strikes.items():
            s = float(s_str)
            cf, pf, nf = classify_strike_oi_flow(v, prev_spot, spot)
            v["call_flow"], v["put_flow"], v["net_flow"] = cf, pf, nf

        ind_data = get_indicators(candle_cache_store[idx]["5m"])
        ind_data["tech"] = {
            "3m": compute_tf_signals(idx, candle_cache_store[idx]["3m"], "3min", 1, 1.0),
            "5m": compute_tf_signals(idx, candle_cache_store[idx]["5m"], "5min", 1, 1.0),
            "15m": compute_tf_signals(idx, candle_cache_store[idx]["15m"], "15min", 1, 1.0)
        }

        vwap_val = get_vwap(candle_cache_store[idx]["5m"])
        baseline_trend_val = vwap_val if vwap_val else ind_data["tech"]["15m"].get("ema15")
        vix_matrix = analyze_vix_price(spot, baseline_trend_val, vix, store["baseline_vix"])
        
        mkt_state = market_state(pcr, ind_data.get("adx"), oi_signal, alerts, vix)
        
        gex_data = [{"strike":float(s),"net_gex":v.get("call_gex",0) - v.get("put_gex",0)} for s,v in sorted(chain.items(), key=lambda x:float(x[0])) if abs(float(s)-atm) <= 10*step]
        gex_flip = min(gex_data, key=lambda x:abs(x["net_gex"])) if gex_data else None

        wall_shifts = []
        if chain:
            max_ce_strike = max(chain.items(), key=lambda x: x[1]["call_oi"])[0]
            max_pe_strike = max(chain.items(), key=lambda x: x[1]["put_oi"])[0]
        else:
            max_ce_strike = max_pe_strike = None
        if store["prev_max_ce_strike"] is not None and max_ce_strike != store["prev_max_ce_strike"]:
            shift_msg = f"Call wall shifted: {store['prev_max_ce_strike']} → {max_ce_strike}"
            wall_shifts.append({"type": "CALL_WALL_SHIFT", "from": store["prev_max_ce_strike"],
                                 "to": max_ce_strike, "icon": "🔄", "message": shift_msg})
            alerts.append({"type": "CALL WALL SHIFT", "icon": "🔄",
                           "message": f"Resistance moved {store['prev_max_ce_strike']} → {max_ce_strike}"})
        if store["prev_max_pe_strike"] is not None and max_pe_strike != store["prev_max_pe_strike"]:
            shift_msg = f"Put wall shifted: {store['prev_max_pe_strike']} → {max_pe_strike}"
            wall_shifts.append({"type": "PUT_WALL_SHIFT", "from": store["prev_max_pe_strike"],
                                 "to": max_pe_strike, "icon": "🔄", "message": shift_msg})
            alerts.append({"type": "PUT WALL SHIFT", "icon": "🔄",
                           "message": f"Support moved {store['prev_max_pe_strike']} → {max_pe_strike}"})
        store["prev_max_ce_strike"] = max_ce_strike
        store["prev_max_pe_strike"] = max_pe_strike

        ist_ts = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%H:%M:%S")
        for a in alerts:
            store["alert_log"].append({
                "time": ist_ts,
                "type": a.get("type", "ALERT"),
                "msg":  a.get("message", a.get("msg", "")),
                "icon": a.get("icon", "⚡")
            })
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

        ind_data["tech"]["overall_bias"] = mkt_state
        ind_data["tech"]["confluence"] = "Aligned" if mkt_state.startswith("BULL") or mkt_state.startswith("BEAR") else "Mixed"
        
        fut_prem = futures - spot
        if fut_prem > (step * 0.2): future_bias = "LONG BUILDUP"
        elif fut_prem < -(step * 0.1): future_bias = "SHORT BUILDUP"
        else: future_bias = "NEUTRAL"
        future_desc = f"{fut_prem:+.1f} pts premium"

        mp_drift = spot - max_pain
        if mp_drift > (step * 0.5): max_pain_signal = "BULLISH DRIFT"
        elif mp_drift < -(step * 0.5): max_pain_signal = "BEARISH DRIFT"
        else: max_pain_signal = "PINNED"
        max_pain_desc = f"{abs(mp_drift):.1f} pts from MP"

        intelligence = {
            "cycle_count": len(store["history"]),
            "market_state": mkt_state,
            "oi_matrix_condition": oi_cond, "oi_matrix_signal": oi_signal, "oi_matrix_desc": oi_desc,
            "pcr_zone": "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.8 else "NEUTRAL",
            "levels": levels_data,
            "alerts": alerts, 
            "gex": {"profile": gex_data[:11], "flip_zone": gex_flip["strike"] if gex_flip else "—"},
            "skew": iv_skew,
            "future_bias": future_bias,
            "future_desc": future_desc,
            "max_pain_signal": max_pain_signal,
            "max_pain_desc": max_pain_desc,
            "index_technicals": ind_data["tech"],
            "cumulative_net_flow_l": round(cum_net_flow / 100000, 2),
            "morning_straddle": morning_straddle,
            "straddle_decay": round(straddle_decay, 2),
            "vix_matrix": vix_matrix,
            "migrations": get_migrations(atm_strikes),
            "activity": get_activity(atm_strikes, idx),
            "analysis": get_analysis(mkt_state, pcr, vix, round(cum_net_flow / 100000, 2)),
            "pin_risk": get_pin_risk(chain, atm),
            "wall_shifts": wall_shifts,
            "max_ce_strike": max_ce_strike,
            "max_pe_strike": max_pe_strike,
            "pcr_history": store["pcr_history"][-20:],
            "straddle_history": store["straddle_history"][-20:],
        }
        
        greeks = {
            "delta": {"val": atm_v.get("call_delta", 0), "desc": "Call Delta"},
            "gamma": {"val": atm_v.get("call_gamma", 0), "desc": "Call Gamma"},
            "theta": {"val": atm_v.get("call_theta", 0), "desc": "Call Theta"},
            "vega": {"val": atm_v.get("call_vega", 0), "desc": "Call Vega"}
        }

        data = {
            "backend_error": None, 
            "spot": spot, "futures": futures, "premium": round(futures-spot,2),
            "atm": atm, "pcr": pcr, "pcr_chg": pcr_chg, "vix": vix,
            "max_pain": max_pain, "expiry": expiry, 
            "total_call_oi": total_call, "total_put_oi": total_put,
            "indicators": ind_data, "intelligence": intelligence,
            "atm_strikes": atm_strikes, "chain": chain, "greeks": greeks,
            "timestamp": datetime.now().isoformat(),
            "index_name": idx
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
        
        try:
            save_server_state()
        except Exception as e:
            print("Save state failed:", e)

        debug_status["last_error"] = f"[{idx}] Data fetched successfully."
        process_telegram_alerts(idx, alerts, data, atm_strikes, atm)
        
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"[{idx} REFRESH CRASH]\n", error_trace)
        debug_status["last_error"] = f"CRASH in {idx}: {str(e)}"
        if oi_cache[idx].get("data"): oi_cache[idx]["data"]["backend_error"] = f"Crash: {str(e)}"
        else: oi_cache[idx]["data"] = {"backend_error": f"Crash: {str(e)}", "timestamp": datetime.now().isoformat()}

def loop():
    time.sleep(5) 
    while True:
        for idx in INDICES.keys():
            refresh(idx)
            time.sleep(2) 
        time.sleep(CACHE_TTL)

threading.Thread(target=loop, daemon=True).start()

@app.route("/")
def dashboard(): return send_file("dashboard.html")

@app.route('/gallery')
def gallery():
    os.makedirs("static/screenshots", exist_ok=True)
    files = glob.glob("static/screenshots/*.png")
    files.sort(key=os.path.getmtime, reverse=True)
    html = """<html><head><title>OI Snap Gallery</title><style>body { background: #07090c; color: #c9d1d9; font-family: sans-serif; text-align: center; }.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; padding: 20px; }.card { background: #0f1319; border: 1px solid #212836; border-radius: 10px; padding: 10px; }img { width: 100%; border-radius: 5px; cursor: pointer; transition: 0.2s; }img:hover { transform: scale(1.02); box-shadow: 0 0 15px rgba(0, 184, 255, 0.4); }a { color: #00b8ff; text-decoration: none; font-weight: bold; }</style></head><body><h1>📸 Automated Screenshot Gallery</h1><p><a href="/">← Back to Live Dashboard</a></p><div class="grid">"""
    if not files: html += "<h3>No screenshots taken yet. Waiting for the first 5-minute cycle...</h3>"
    for f in files:
        filename = os.path.basename(f)
        html += f'''<div class="card"><h4 style="margin-top:5px; color:#8b949e">{filename}</h4><a href="/static/screenshots/{filename}" target="_blank"><img src="/static/screenshots/{filename}" loading="lazy"></a></div>'''
    html += "</div></body></html>"
    return html

@app.route('/static/screenshots/<filename>')
def serve_screenshot(filename): return send_from_directory('static/screenshots', filename)

@app.route("/oi/json")
def oi_json():
    idx = request.args.get("idx", "NIFTY")
    if idx not in INDICES: idx = "NIFTY"
    d = oi_cache[idx].get("data")
    if not d and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                d = json.load(f).get(idx)
                if d: oi_cache[idx]["data"] = d
        except: pass
    if not d: 
        diag_msg = debug_status.get('last_error', 'Unknown Error')
        return jsonify({"error": f"Data Empty for {idx}. [Diagnostic: {diag_msg}] — Click login."})
    return jsonify(d)

@app.route("/oi/histogram")
def histogram():
    idx = request.args.get("idx", "NIFTY")
    if idx not in INDICES: idx = "NIFTY"
    d = oi_cache[idx].get("data")
    if not d and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f: d = json.load(f).get(idx)
        except: pass
    if not d or not d.get("chain"): return jsonify([])
    
    chain = d["chain"]
    atm = float(d["atm"])
    step = float(INDICES[idx]["step"])
    
    safe_list = []
    for s_str, v in chain.items():
        try:
            s_float = float(s_str)
            if abs(s_float - atm) <= ATM_RANGE * step:
                safe_list.append(v)
        except Exception:
            pass
            
    return jsonify(sorted(safe_list, key=lambda x: float(x.get("strike", 0))))

@app.route("/telegram/force_summary")
def force_telegram_summary():
    idx = request.args.get("idx", "NIFTY")
    d = oi_cache[idx].get("data")
    if not d: return f"No data available for {idx} yet.", 400
    msg = generate_5min_summary(idx, d, d.get("atm_strikes", {}), d.get("atm", 0))
    send_telegram_alert(msg)
    return f"Summary sent to Telegram for {idx} successfully!", 200

@app.route("/oi/alert_log")
def alert_log_route():
    idx = request.args.get("idx", "NIFTY")
    if idx not in INDICES: idx = "NIFTY"
    return jsonify(list(reversed(STORE[idx].get("alert_log", []))))

@app.route("/oi/pcr_history")
def pcr_history_route():
    idx = request.args.get("idx", "NIFTY")
    if idx not in INDICES: idx = "NIFTY"
    return jsonify(STORE[idx].get("pcr_history", []))

@app.route("/callback")
def callback():
    code = request.args.get("code")
    resp = requests.post("https://api.upstox.com/v2/login/authorization/token", data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"}, headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    data = resp.json()
    if "access_token" not in data:
        debug_status["last_error"] = f"Upstox Auth Rejected"
        return f"<h2>Login Failed</h2><a href='/login'>Try again</a>"
    
    save_token(data.get("access_token"))
    send_telegram_alert("✅ <b>Upstox Login Successful!</b> Triple Engine is tracking.")
    
    def run_init():
        for idx in INDICES: 
            refresh(idx)
            time.sleep(2)
            
    threading.Thread(target=run_init, daemon=True).start()
    
    return """<html><body style="font-family:sans-serif;background:#0a0c10;color:#00e676;padding:40px"><h2>✅ Login Successful!</h2><p><a href="/" style="color:#40c4ff">→ Open Dashboard</a></p><script>setTimeout(()=>window.location.href="/",2000)</script></body></html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)