

"""
====================================================
  NIFTY50 OI Server — The Absolute Master Edition
  Fixes: get_indicators crash | Adds: RSI & LTP Changes
====================================================
"""

import os, csv, time, math, threading, json, urllib.parse, traceback
from datetime import datetime, date, timedelta
from flask import Flask, jsonify, request, redirect, send_file
from flask_cors import CORS
import requests

app  = Flask(__name__)
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
NIFTY_KEY    = "NSE_INDEX|Nifty 50"

# 🚨 TELEGRAM CREDENTIALS 🚨
TELEGRAM_BOT_TOKEN = "8709594892:AAGcSqRJLvSr-gX405Nbp3LQ0kJPghYPax4"  
TELEGRAM_CHAT_ID   = "7851805837" 

CACHE_TTL    = 150  
STRIKE_STEP  = 50
ATM_RANGE    = 5
TOKEN_FILE   = "token_data.json"
DATA_FILE    = "data_cache.json"  
STATE_FILE   = "server_state.json" 

token_store     = {"access_token": None}
oi_cache        = {"data": None}

baseline_oi     = {}
baseline_vix    = None
baseline_rsi    = {} 
prev_oi         = {}
prev_pcr        = None
prev_spot       = None

candle_cache    = []
candle_cache_3m = []
candle_cache_15 = []          
ltp_history     = {}          

sent_alerts = {}
last_5min_summary = 0 
debug_status = {"last_error": "No data fetched yet. Waiting for first cycle."}

# ══════════════════════════════════════════════════
#  STATE RECOVERY ENGINE
# ══════════════════════════════════════════════════
def reverse_engineer_baseline():
    global baseline_oi
    if len(baseline_oi) > 0: return
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r") as f:
                d = json.load(f)
                if d.get("timestamp") and d["timestamp"].startswith(date.today().isoformat()):
                    chain = d.get("chain", {})
                    if chain:
                        for s, v in chain.items():
                            b_coi = v["call_oi"] - v.get("call_oi_chg_day", 0)
                            b_poi = v["put_oi"] - v.get("put_oi_chg_day", 0)
                            b_cltp = v["call_ltp"] - v.get("call_ltp_chg_day", 0)
                            b_pltp = v["put_ltp"] - v.get("put_ltp_chg_day", 0)
                            baseline_oi[str(s)] = {"call_oi": b_coi, "put_oi": b_poi, "call_ltp": b_cltp, "put_ltp": b_pltp}
    except: pass

def load_server_state():
    global baseline_oi, baseline_vix, prev_oi, prev_pcr, prev_spot, baseline_rsi
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                st = json.load(f)
                if st.get("date") == date.today().isoformat():
                    baseline_oi = st.get("baseline_oi", {})
                    baseline_vix = st.get("baseline_vix")
                    baseline_rsi = st.get("baseline_rsi", {})
                    prev_oi = st.get("prev_oi", {})
                    prev_spot = st.get("prev_spot")
                    prev_pcr = st.get("prev_pcr")
        except: pass
    reverse_engineer_baseline()

def save_server_state():
    try:
        st = {
            "date": date.today().isoformat(),
            "baseline_oi": baseline_oi,
            "baseline_vix": baseline_vix,
            "baseline_rsi": baseline_rsi,
            "prev_oi": prev_oi,
            "prev_spot": prev_spot,
            "prev_pcr": prev_pcr
        }
        with open(STATE_FILE, "w") as f:
            json.dump(st, f)
    except: pass

load_server_state()

# ══════════════════════════════════════════════════
#  TELEGRAM BOT ENGINE
# ══════════════════════════════════════════════════
def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE": return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try: requests.post(url, json=payload, timeout=5)
    except: pass

def generate_5min_summary(data, atm_strikes, atm):
    spot = data.get("spot", 0)
    pcr = data.get("pcr", 0)
    intel = data.get("intelligence", {})
    net_flow_l = intel.get("cumulative_net_flow_l", 0)
    flow_bias = "🟢 BULLISH" if net_flow_l > 0 else "🔴 BEARISH" if net_flow_l < 0 else "⚪ NEUTRAL"
    
    atm_v = atm_strikes.get(str(atm), {})
    s_curr = atm_v.get("call_ltp", 0) + atm_v.get("put_ltp", 0)
    s_decay = intel.get("straddle_decay", 0)
    s_low = int(atm - s_curr)
    s_high = int(atm + s_curr)
    vix_mat = intel.get("vix_matrix", {})
    
    t5 = intel.get("index_technicals", {}).get("5min", {})
    t15 = intel.get("index_technicals", {}).get("15min", {})
    
    msg = (
        f"⏱ <b>5-MIN NIFTY SCANNER</b>\n"
        f"🎯 <b>Spot:</b> ₹{spot} | <b>PCR:</b> {pcr}\n"
        f"⚖️ <b>Straddle:</b> ₹{s_curr:.1f} ({s_decay:+.1f}% Day)\n"
        f"🔴 <b>ATM CE:</b> ₹{atm_v.get('call_ltp', 0):.1f} ({atm_v.get('call_ltp_chg_day', 0):+.1f} D | {atm_v.get('call_ltp_chg', 0):+.1f} 5m)\n"
        f"🟢 <b>ATM PE:</b> ₹{atm_v.get('put_ltp', 0):.1f} ({atm_v.get('put_ltp_chg_day', 0):+.1f} D | {atm_v.get('put_ltp_chg', 0):+.1f} 5m)\n"
        f"🌊 <b>Smart Flow:</b> {flow_bias} ({net_flow_l:+.1f}L Net)\n"
        f"📊 <b>VIX Matrix: {vix_mat.get('signal', 'N/A')}</b>\n"
        f"↳ <i>{vix_mat.get('desc', 'N/A')}</i>\n"
        f"⏱ <b>TIMING (5m):</b> Cross: {t5.get('ts_start','-')} | Pull: {t5.get('ts_pull','-')} | Cont: {t5.get('ts_cont','-')} | ST: {t5.get('ts_st','-')}\n"
        f"⏱ <b>TIMING (15m):</b> Cross: {t15.get('ts_start','-')} | Pull: {t15.get('ts_pull','-')} | Cont: {t15.get('ts_cont','-')} | ST: {t15.get('ts_st','-')}\n"
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
        if abs(s - atm) > 2 * STRIKE_STEP: continue
        v = atm_strikes[str(s)]
        marker = " ◄ ATM" if s == atm else ""
        
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

def process_telegram_alerts(alerts, data, atm_strikes, atm):
    global sent_alerts, last_5min_summary
    current_time = time.time()
    if last_5min_summary == 0: 
        last_5min_summary = current_time
        return
    try:
        for a in alerts:
            msg = f"{a['icon']} <b>{a['type']}</b>\n{a['message']}"
            if msg not in sent_alerts or (current_time - sent_alerts[msg] > 1800):
                send_telegram_alert(msg)
                sent_alerts[msg] = current_time
        sent_alerts = {k: v for k, v in sent_alerts.items() if current_time - v < 3600}
        if current_time - last_5min_summary >= 290: 
            summary = generate_5min_summary(data, atm_strikes, atm)
            send_telegram_alert(summary)
            last_5min_summary = current_time
    except: pass

# ══════════════════════════════════════════════════
#  AUTH & API FETCHERS
# ══════════════════════════════════════════════════
def save_token(token):
    token_store["access_token"] = token
    try:
        with open(TOKEN_FILE, "w") as f: json.dump({"access_token": token}, f)
    except: pass

def load_token():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f: token_store["access_token"] = json.load(f).get("access_token")
        except: pass

load_token()

def hdrs():
    load_token()
    return {"Authorization": f"Bearer {token_store['access_token']}", "Accept": "application/json", "Api-Version": "2.0"}

@app.route("/login")
def login(): return redirect(f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URI}")

@app.route("/callback")
def callback():
    code = request.args.get("code")
    resp = requests.post("https://api.upstox.com/v2/login/authorization/token", data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"}, headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    data = resp.json()
    if "access_token" not in data:
        debug_status["last_error"] = f"Upstox Auth Rejected"
        return f"<h2>Login Failed</h2><a href='/login'>Try again</a>"
    save_token(data.get("access_token"))
    send_telegram_alert("✅ <b>Upstox Login Successful!</b> Server is tracking.")
    refresh()
    return """<html><body style="font-family:sans-serif;background:#0a0c10;color:#00e676;padding:40px"><h2>✅ Login Successful!</h2><p><a href="/" style="color:#40c4ff">→ Open Dashboard</a></p></body></html>"""

def fetch_spot():
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": NIFTY_KEY}, headers=hdrs(), timeout=10)
        d = r.json().get("data", {}); key = list(d.keys())[0] if d else None
        return float(d[key].get("last_price", 0)) if key else 0
    except: return 0

def fetch_futures(spot):
    from datetime import date
    now = date.today(); months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
    for delta in [0, 1]:
        m = (now.month - 1 + delta) % 12; y = str(now.year)[2:] if now.month + delta <= 12 else str(now.year + 1)[2:]
        sym = f"NSE_FO|NIFTY{y}{months[m]}FUT"
        try:
            r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": sym}, headers=hdrs(), timeout=5)
            if r.status_code == 200 and r.json().get("data"):
                d = r.json()["data"]; key = list(d.keys())[0]
                p = d[key].get("last_price") or d[key].get("ltp") or 0
                if p: return float(p)
        except: pass
    return round(spot * 1.005, 2)

def fetch_vix():
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp", params={"symbol": "NSE_INDEX|India VIX"}, headers=hdrs(), timeout=10)
        if r.status_code == 200 and r.json().get("data"):
            d = r.json()["data"]; key = list(d.keys())[0]
            return float(d[key].get("last_price") or d[key].get("ltp") or 0)
    except: pass
    return 0

def fetch_base_1m_candles():
    try:
        safe_key = urllib.parse.quote(NIFTY_KEY)
        to_date = date.today().strftime("%Y-%m-%d"); from_date = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        url = f"https://api.upstox.com/v2/historical-candle/{safe_key}/1minute/{to_date}/{from_date}"
        r = requests.get(url, headers=hdrs(), timeout=10)
        if r.status_code == 200:
            cr = r.json().get("data", {}).get("candles", [])
            res = [{"time": c[0], "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4]), "vol": float(c[5]) if len(c)>5 else 0} for c in cr if len(c)>=5]
            res.sort(key=lambda x: x["time"])
            return res
    except: pass
    return []

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
        except: pass
    if cg: res.append({"time": ct.isoformat(), "open": cg[0]["open"], "high": max(x["high"] for x in cg), "low": min(x["low"] for x in cg), "close": cg[-1]["close"], "vol": sum(x.get("vol", 0) for x in cg)})
    return res

def get_expiry():
    try:
        r = requests.get("https://api.upstox.com/v2/option/contract", params={"instrument_key": NIFTY_KEY}, headers=hdrs(), timeout=10)
        if r.status_code == 200:
            items = r.json().get("data", [])
            exps = sorted([i if isinstance(i, str) else i.get("expiry") for i in items if i])
            today = datetime.today().strftime("%Y-%m-%d")
            for e in exps:
                if e and e >= today: return e
    except: pass
    today = date.today(); days = (3 - today.weekday()) % 7
    if days == 0: days = 7
    return (today + timedelta(days=days)).strftime("%Y-%m-%d")

def fetch_chain(expiry):
    try:
        r = requests.get("https://api.upstox.com/v2/option/chain", params={"instrument_key": NIFTY_KEY, "expiry_date": expiry}, headers=hdrs(), timeout=15)
        return r.json().get("data", []) if r.status_code == 200 else []
    except: return []

# ══════════════════════════════════════════════════
#  MATH & PROCESSORS (VERIFIED)
# ══════════════════════════════════════════════════
def compute_max_pain(chain):
    strikes = sorted(chain.keys())
    if not strikes: return 0
    min_loss, mp = float("inf"), strikes[0]
    for s in strikes:
        loss = sum(v["call_oi"]*(s-k) if k<s else v["put_oi"]*(k-s) if k>s else 0 for k,v in chain.items())
        if loss < min_loss: min_loss = loss; mp = s
    return mp

def get_vwap(candles):
    today_date = datetime.now().strftime("%Y-%m-%d")
    cum_vol = cum_pv = 0
    for c in candles:
        if c.get("time", "").startswith(today_date):
            v = c.get('vol', 0)
            cum_vol += v
            cum_pv += ((c['high'] + c['low'] + c['close']) / 3) * v
    return round(cum_pv / cum_vol, 2) if cum_vol > 0 else None

def calc_ema(prices, period):
    if not prices or len(prices) < period: return None
    k = 2.0 / (period + 1); ema = sum(prices[:period]) / period
    for p in prices[period:]: ema = p * k + ema * (1 - k)
    return round(ema, 2)

def calc_ema_array(prices, period):
    if not prices or len(prices) < period: return [None] * len(prices)
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
    highs, lows, closes = [c["high"] for c in candles], [c["low"] for c in candles], [c["close"] for c in candles]
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(candles))]
    atr = sum(trs[-period:]) / period; hl2 = (highs[-1] + lows[-1]) / 2
    direction = "BULLISH" if closes[-1] > (hl2 - multiplier * atr) else "BEARISH"
    st_val = round((hl2 - multiplier * atr) if direction == "BULLISH" else (hl2 + multiplier * atr), 2)
    return direction, st_val

def calc_rsi(closes, p=14):
    if len(closes) < p+1: return None
    gains=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    losses=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    ag=sum(gains[:p])/p; al=sum(losses[:p])/p
    for i in range(p, len(gains)):
        ag=(ag*(p-1)+gains[i])/p; al=(al*(p-1)+losses[i])/p
    return 100.0 if al==0 else round(100-100/(1+ag/al), 2)

def calc_rsi_array(closes, p=14):
    if len(closes) < p+1: return []
    gains = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    if sum(losses[:p]) == 0: 
        rsis = [None]*p + [100.0]
    else:
        ag = sum(gains[:p])/p
        al = sum(losses[:p])/p
        rsis = [None]*p
        rsis.append(100.0 if al==0 else 100 - (100/(1+ag/al)))
    for i in range(p, len(gains)):
        ag = (ag*(p-1) + gains[i])/p
        al = (al*(p-1) + losses[i])/p
        rsis.append(100.0 if al==0 else 100 - (100/(1+ag/al)))
    return rsis

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
        s = sum(lst[:p]); r = [s]
        for i in range(p, len(lst)): 
            s = s - s/p + lst[i]; r.append(s)
        return r
    atr = sm(trl, p); pDM = sm(pdml, p); nDM = sm(ndml, p)
    dxl = []
    for i in range(len(atr)):
        if atr[i] == 0: continue
        pdi = 100 * pDM[i] / atr[i]; ndi = 100 * nDM[i] / atr[i]
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

def compute_tf_signals(candles, label, st_period, st_multiplier):
    if not candles or len(candles) < 15: 
        return {"label": label, "candle_count": len(candles) if candles else 0, "ts_start": "-", "ts_pull": "-", "ts_cont": "-", "ts_st": "-"}
    
    global baseline_rsi
    
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
        if e7 is None or e15 is None: continue
        c_close = closes[i]
        
        try:
            d_str = times[i][:10]
            t_str = times[i][11:16]
            c_time = f"{d_str[8:10]}-{d_str[5:7]} {t_str}" 
        except: 
            c_time = "-"

        curr_bull = e7 > e15
        
        if is_bull is None:
            is_bull = curr_bull
            trend_start = c_time
            if curr_bull: await_pull_b = True
            else: await_pull_s = True
        elif is_bull != curr_bull:
            trend_start = c_time
            pull_time, cont_time = "-", "-"
            is_bull = curr_bull
            if curr_bull: await_pull_b = True
            else: await_pull_s = True
            
        if is_bull:
            if await_pull_b and (c_close < e7 or c_close < e15):
                pull_time = c_time; cont_time = "..."; await_pull_b = False
            elif not await_pull_b and c_close > e7:
                cont_time = c_time; await_pull_b = True
        else:
            if await_pull_s and (c_close > e7 or c_close > e15):
                pull_time = c_time; cont_time = "..."; await_pull_s = False
            elif not await_pull_s and c_close < e7:
                cont_time = c_time; await_pull_s = True
                
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
        if price > ema7 and ema7 > ema15: trend = "STRONG BULLISH"
        elif price > ema7 and ema7 < ema15: trend = "RECOVERING"
        elif price < ema7 and ema7 > ema15: trend = "MILD BEARISH"
        else: trend = "STRONG BEARISH"

    rsis = calc_rsi_array(closes, 14)
    curr_rsi = round(rsis[-1], 2) if rsis and rsis[-1] is not None else 0
    prev_rsi = round(rsis[-2], 2) if rsis and len(rsis) > 1 and rsis[-2] is not None else curr_rsi
    
    base_r = baseline_rsi.get(label)
    if not base_r and curr_rsi > 0:
        baseline_rsi[label] = curr_rsi
        base_r = curr_rsi
        
    rsi_5m_chg = round(curr_rsi - prev_rsi, 2) if curr_rsi else 0
    rsi_day_chg = round(curr_rsi - base_r, 2) if curr_rsi and base_r else 0

    return {
        "label": label, "candle_count": len(candles), "current_price": round(price, 2) if price else None, 
        "ema7": ema7, "ema15": ema15, "vwap": vwap, 
        "price_above_ema7": price > ema7 if ema7 else None, "price_above_ema15": price > ema15 if ema15 else None, 
        "ema7_above_ema15": ema7 > ema15 if ema7 and ema15 else None, "price_above_vwap": price > vwap if vwap else None,
        "trend": trend, "supertrend": st_dir, "supertrend_val": st_val, 
        "rsi": curr_rsi if curr_rsi > 0 else None,
        "rsi_5m_chg": rsi_5m_chg,
        "rsi_day_chg": rsi_day_chg,
        "ts_start": trend_start, "ts_pull": pull_time, "ts_cont": cont_time, "ts_st": st_time
    }

def price_oi_matrix(spot, prev_spot, chain, atm):
    if prev_spot is None or len(prev_oi)==0: return "INITIALIZING","—","Waiting for second data cycle"
    price_up, price_dn = spot > prev_spot, spot < prev_spot
    total_oi_chg = sum(v["call_oi_chg"]+v["put_oi_chg"] for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP)
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

def analyze_vix_price(spot, vwap, vix, base_vix):
    if not vwap or not base_vix or base_vix == 0: 
        return {"signal": "WAITING", "desc": "Need more data for baseline comparison"}
    
    price_up = spot > vwap
    vix_up = vix > base_vix
    
    if price_up and vix_up: return {"signal": "STRONG BULLISH", "desc": "Price ↑ + VIX ↑ | Fear rising + Rally = Breakout expected."}
    elif price_up and not vix_up: return {"signal": "WEAK BULLISH", "desc": "Price ↑ + VIX ↓ | Move lacks power = Resistance might hold."}
    elif not price_up and vix_up: return {"signal": "STRONG BEARISH", "desc": "Price ↓ + VIX ↑ | Panic selling = Support might break."}
    elif not price_up and not vix_up: return {"signal": "WEAK BEARISH", "desc": "Price ↓ + VIX ↓ | Normal correction = Support might hold."}
    return {"signal": "NEUTRAL", "desc": "Market flat"}

def process_chain(raw):
    global prev_oi, baseline_oi
    result={}
    for item in raw:
        strike=float(item.get("strike_price",0))
        if not strike: continue
        ce, pe = item.get("call_options",{}), item.get("put_options",{})
        ce_md, pe_md = ce.get("market_data",{}), pe.get("market_data",{})
        ce_gk, pe_gk = ce.get("option_greeks",{}), pe.get("option_greeks",{})
        
        call_oi, put_oi = float(ce_md.get("oi",0) or 0), float(pe_md.get("oi",0) or 0)
        call_vol, put_vol = float(ce_md.get("volume",0) or 0), float(pe_md.get("volume",0) or 0)
        call_ltp, put_ltp = float(ce_md.get("ltp",0) or ce_md.get("last_price",0) or 0), float(pe_md.get("ltp",0) or pe_md.get("last_price",0) or 0)
        
        prev = prev_oi.get(str(strike), {}) if prev_oi else {}
        base = baseline_oi.get(str(strike), {}) if baseline_oi else {}

        c_oi_5m = call_oi - prev["call_oi"] if "call_oi" in prev else 0
        p_oi_5m = put_oi - prev["put_oi"] if "put_oi" in prev else 0
        c_ltp_5m = call_ltp - prev["call_ltp"] if "call_ltp" in prev else 0
        p_ltp_5m = put_ltp - prev["put_ltp"] if "put_ltp" in prev else 0
        
        c_oi_d = call_oi - base["call_oi"] if "call_oi" in base else 0
        p_oi_d = put_oi - base["put_oi"] if "put_oi" in base else 0
        
        c_prev_close = ce_md.get("previous_close") or ce_md.get("close_price")
        p_prev_close = pe_md.get("previous_close") or pe_md.get("close_price")
        c_ltp_d = call_ltp - c_prev_close if c_prev_close else (call_ltp - base["call_ltp"] if "call_ltp" in base else 0)
        p_ltp_d = put_ltp - p_prev_close if p_prev_close else (put_ltp - base["put_ltp"] if "put_ltp" in base else 0)

        result[strike] = {
            "strike": strike,
            "call_oi": call_oi, "call_oi_chg": round(c_oi_5m, 2), "call_oi_chg_day": round(c_oi_d, 2),
            "call_vol_oi": round(float(ce_md.get("volume",0) or 0)/call_oi,2) if call_oi else 0,
            "call_iv": round(float(ce_gk.get("iv",0) or 0)*100,2), 
            "call_ltp": call_ltp, "call_ltp_chg": round(c_ltp_5m, 2), "call_ltp_chg_day": round(c_ltp_d, 2),
            "call_delta": float(ce_gk.get("delta",0) or 0), "call_gamma": float(ce_gk.get("gamma",0) or 0), "call_gex": float(ce_gk.get("gamma",0) or 0) * call_oi * 25,
            
            "put_oi": put_oi, "put_oi_chg": round(p_oi_5m, 2), "put_oi_chg_day": round(p_oi_d, 2),
            "put_vol_oi": round(float(pe_md.get("volume",0) or 0)/put_oi,2) if put_oi else 0,
            "put_iv": round(float(pe_gk.get("iv",0) or 0)*100,2), 
            "put_ltp": put_ltp, "put_ltp_chg": round(p_ltp_5m, 2), "put_ltp_chg_day": round(p_ltp_d, 2),
            "put_delta": float(pe_gk.get("delta",0) or 0), "put_gamma": float(pe_gk.get("gamma",0) or 0), "put_gex": float(pe_gk.get("gamma",0) or 0) * put_oi * 25
        }
        
    if len(baseline_oi) == 0 and result: 
        baseline_oi = {str(s): {"call_oi": v["call_oi"], "put_oi": v["put_oi"], "call_ltp": v["call_ltp"], "put_ltp": v["put_ltp"]} for s,v in result.items()}
    return result

def classify_strike_oi_flow(v, prev_spot, spot):
    pup, pdn = spot > prev_spot + 5 if prev_spot else False, spot < prev_spot - 5 if prev_spot else False
    c_c, cl, c_o = v.get("call_oi_chg", 0), v.get("call_ltp_chg", 0), v.get("call_oi", 0)
    p_c, pl, p_o = v.get("put_oi_chg", 0), v.get("put_ltp_chg", 0), v.get("put_oi", 0)
    
    # 🔥 HIGH SENSITIVITY FIX (25,000 contracts trigger)
    THRESH = 25000 
    
    if pup and c_c > THRESH and cl > 0: cf = ("LONG BUILDUP", "BULLISH", "🟢")
    elif pup and c_c < -THRESH and cl > 0: cf = ("SHORT COVERING", "WEAK BULLISH", "📈")
    elif pdn and c_c > THRESH and cl < 0: cf = ("FRESH CALL WRITING", "BEARISH", "🔴")
    elif pdn and c_c < -THRESH: cf = ("LONG UNWINDING", "WEAK BEARISH", "🟡")
    elif c_c > 200000: cf = ("HEAVY CALL ADDITION", "WATCH", "🔴")
    elif c_c < -200000: cf = ("HEAVY CALL EXIT", "BULLISH", "✅")
    else: cf = ("STABLE / NO CHANGE", "NEUTRAL", "⚪")

    if pdn and p_c > THRESH and pl > 0: pf = ("LONG BUILDUP", "BEARISH", "🔴")
    elif pdn and p_c < -THRESH and pl > 0: pf = ("SHORT COVERING", "WEAK BEARISH", "🟡")
    elif pup and p_c > THRESH and pl < 0: pf = ("FRESH PUT WRITING", "BULLISH", "✅")
    elif pup and p_c < -THRESH: pf = ("LONG UNWINDING", "WEAK BULLISH", "📈")
    elif p_c > 200000: pf = ("HEAVY PUT ADDITION", "WATCH", "✅")
    elif p_c < -200000: pf = ("HEAVY PUT EXIT", "BEARISH", "🔴")
    else: pf = ("STABLE / NO CHANGE", "NEUTRAL", "⚪")

    total_chg = c_c + p_c
    if c_c > THRESH and p_c < -THRESH: net_note = "🔄 OI SHIFT: Money moving to CALL side."
    elif p_c > THRESH and c_c < -THRESH: net_note = "🔄 OI SHIFT: Money moving to PUT side."
    elif c_c > THRESH and p_c > THRESH: net_note = "💥 BOTH SIDES ADDING OI: High uncertainty."
    elif c_c < -THRESH and p_c < -THRESH: net_note = "🌀 BOTH SIDES EXITING: Position squareoff."
    else: net_note = "— No significant OI flow this cycle."

    return (
        {"condition": cf[0], "signal": cf[1], "emoji": cf[2], "oi_chg_l": round(c_c/100000,2), "oi_total_l": round(c_o/100000,2)},
        {"condition": pf[0], "signal": pf[1], "emoji": pf[2], "oi_chg_l": round(p_c/100000,2), "oi_total_l": round(p_o/100000,2)},
        {"total_oi_chg_l": round(total_chg/100000,2), "total_oi_l": round((c_o+p_o)/100000,2), "net_note": net_note}
    )

# ══════════════════════════════════════════════════
#  MAIN REFRESH LOOP
# ══════════════════════════════════════════════════

def refresh():
    global prev_oi, prev_pcr, prev_spot, candle_cache, candle_cache_15, candle_cache_3m, baseline_vix, ltp_history

    load_token()
    if not token_store.get("access_token"):
        debug_status["last_error"] = "Token missing from memory/disk. Please login."
        return

    try:
        spot   = fetch_spot()
        expiry = get_expiry()
        raw    = fetch_chain(expiry)
        
        if not raw:
            err_msg = f"Upstox returned no chain data. Token expired or IP limit reached."
            debug_status["last_error"] = err_msg
            if oi_cache.get("data"): oi_cache["data"]["backend_error"] = err_msg
            else: oi_cache["data"] = {"backend_error": err_msg, "timestamp": datetime.now().isoformat()}
            return

        atm   = round(round(spot/50)*50, 2)
        chain = process_chain(raw)
        if not chain: return

        max_pain = compute_max_pain(chain)
        atm_strikes = {str(s):v for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP}
        
        total_call  = sum(v["call_oi"] for v in chain.values())
        total_put   = sum(v["put_oi"]  for v in chain.values())
        pcr         = round(total_put/total_call,2) if total_call else 0
        pcr_chg     = round(pcr-prev_pcr,3) if prev_pcr is not None else 0
        futures     = fetch_futures(spot)
        vix         = fetch_vix()
        
        if baseline_vix is None and vix > 0: baseline_vix = vix

        candles_1m  = fetch_base_1m_candles()
        if candles_1m:
            candle_cache_3m = resample_candles(candles_1m, 3)[-60:]
            candle_cache    = resample_candles(candles_1m, 5)[-60:]
            candle_cache_15 = resample_candles(candles_1m, 15)[-40:]

        cum_put_add = sum(v["put_oi_chg_day"] for v in chain.values())
        cum_call_add = sum(v["call_oi_chg_day"] for v in chain.values())
        cum_net_flow = cum_put_add - cum_call_add

        atm_v = atm_strikes.get(str(atm), {})
        current_straddle = atm_v.get("call_ltp", 0) + atm_v.get("put_ltp", 0)
        old_data = oi_cache.get("data") or {}
        morning_straddle = old_data.get("intelligence", {}).get("morning_straddle")
        if morning_straddle is None and current_straddle > 0: morning_straddle = current_straddle
        straddle_decay = ((current_straddle - morning_straddle) / morning_straddle * 100) if morning_straddle and morning_straddle > 0 else 0

        alerts = []
        for s,v in chain.items():
            dist = s - spot
            if 0 < dist <= 150 and v["call_oi_chg"] < 0 and abs(v["call_oi_chg"]) > v["call_oi"]*0.05: alerts.append({"type":"BREAKOUT UP","icon":"⚡","message":f"Res OI dropping"})
            if -150 <= dist < 0 and v["put_oi_chg"] < 0 and abs(v["put_oi_chg"]) > v["put_oi"]*0.05: alerts.append({"type":"BREAKOUT DOWN","icon":"⚡","message":f"Sup OI dropping"})

        oi_cond, oi_signal, oi_desc = price_oi_matrix(spot, prev_spot, chain, atm)

        for s_str, v in atm_strikes.items():
            s = float(s_str)
            cf, pf, nf = classify_strike_oi_flow(v, prev_spot, spot)
            v["call_flow"], v["put_flow"], v["net_flow"] = cf, pf, nf
            
            if s not in ltp_history: ltp_history[s] = {"call": [], "put": []}
            if v.get("call_ltp"): ltp_history[s]["call"] = (ltp_history[s]["call"] + [float(v["call_ltp"])])[-25:]
            if v.get("put_ltp"): ltp_history[s]["put"] = (ltp_history[s]["put"] + [float(v["put_ltp"])])[-25:]
            def s_info(prices):
                e7 = calc_ema(prices, 7)
                e15 = calc_ema(prices, 15)
                price = prices[-1] if prices else None
                return {"ema7": e7, "ema15": e15, "price_above_ema7": price > e7 if e7 and price else None}
            v["ltp_technicals"] = {"call": s_info(ltp_history[s]["call"]), "put": s_info(ltp_history[s]["put"])}

        vwap_val = get_vwap(candle_cache)
        vix_matrix = analyze_vix_price(spot, vwap_val, vix, baseline_vix)
        
        ind_data = get_indicators(candle_cache)
        mkt_state = market_state(pcr, ind_data.get("adx"), oi_signal, alerts, vix)
        
        gex_data = [{"strike":s,"net_gex":v["call_gex"] - v["put_gex"]} for s,v in sorted(chain.items()) if abs(s-atm) <= 10*STRIKE_STEP]
        gex_flip = min(gex_data, key=lambda x:abs(x["net_gex"])) if gex_data else None

        skew_data=[]
        for dist in [1,2,3]:
            c_strike, p_strike = atm+dist*STRIKE_STEP, atm-dist*STRIKE_STEP
            ce, pe = chain.get(c_strike,{}), chain.get(p_strike,{})
            if ce.get("call_iv") and pe.get("put_iv"): skew_data.append({"dist":dist,"call_strike":c_strike,"call_iv":ce["call_iv"],"put_strike":p_strike,"put_iv":pe["put_iv"],"skew":round(pe["put_iv"]-ce["call_iv"],2)})
        avg_skew=round(sum(x["skew"] for x in skew_data)/len(skew_data),2) if skew_data else 0
        iv_skew = {"data":skew_data,"avg_skew":avg_skew,"signal":"BEARISH SKEW — put IV elevated" if avg_skew>3 else "BULLISH SKEW — call IV elevated" if avg_skew<-3 else "NEUTRAL SKEW — balanced"}

        intelligence = {
            "market_state": mkt_state,
            "oi_matrix_condition": oi_cond, "oi_matrix_signal": oi_signal, "oi_matrix_desc": oi_desc,
            "pcr_zone": "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.8 else "NEUTRAL",
            "alerts": alerts, 
            "gex_profile": gex_data[:11], "gex_flip": gex_flip, "iv_skew": iv_skew,
            "index_technicals": {
                "3min": compute_tf_signals(candle_cache_3m, "3min", 1, 1.0),
                "5min": compute_tf_signals(candle_cache, "5min", 1, 1.0),
                "15min": compute_tf_signals(candle_cache_15, "15min", 1, 1.0)
            },
            "cumulative_net_flow_l": round(cum_net_flow / 100000, 2),
            "morning_straddle": morning_straddle,
            "straddle_decay": round(straddle_decay, 2),
            "vix_matrix": vix_matrix
        }

        data = {
            "backend_error": None, 
            "spot": spot, "futures": futures, "premium": round(futures-spot,2),
            "atm": atm, "pcr": pcr, "pcr_chg": pcr_chg, "vix": vix,
            "max_pain": max_pain, "expiry": expiry, 
            "total_call_oi": total_call, "total_put_oi": total_put,
            "indicators": ind_data, "intelligence": intelligence,
            "atm_strikes": atm_strikes, "chain": chain,
            "timestamp": datetime.now().isoformat()
        }

        oi_cache["data"] = data
        try:
            with open(DATA_FILE, "w") as f: json.dump(data, f)
        except: pass

        prev_oi   = {str(s):{"call_oi":v["call_oi"],"put_oi":v["put_oi"],"call_ltp":v["call_ltp"],"put_ltp":v["put_ltp"]} for s,v in chain.items()}
        prev_pcr  = pcr
        prev_spot = spot
        
        save_server_state()
        
        debug_status["last_error"] = "Data fetched successfully."
        process_telegram_alerts(alerts, data, atm_strikes, atm)
        
    except Exception as e:
        error_trace = traceback.format_exc()
        print("[REFRESH CRASH]\n", error_trace)
        debug_status["last_error"] = f"CRASH in refresh(): {str(e)}"
        if oi_cache.get("data"): oi_cache["data"]["backend_error"] = f"Crash: {str(e)}"
        else: oi_cache["data"] = {"backend_error": f"Crash: {str(e)}", "timestamp": datetime.now().isoformat()}

def loop():
    time.sleep(5) 
    while True:
        refresh()
        time.sleep(CACHE_TTL)

threading.Thread(target=loop, daemon=True).start()

# ══════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════

@app.route("/")
def dashboard(): return send_file("dashboard.html")

@app.route("/oi/json")
def oi_json():
    d = oi_cache.get("data")
    if not d and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                d = json.load(f)
                oi_cache["data"] = d
        except: pass
        
    if not d: 
        diag_msg = debug_status.get('last_error', 'Unknown Error')
        return jsonify({"error": f"Data Empty. [Diagnostic: {diag_msg}] — Click login to try again."})
        
    try:
        last_upd = datetime.fromisoformat(d["timestamp"])
        if (datetime.now() - last_upd).total_seconds() > 180:
            refresh()
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, "r") as f: d = json.load(f)
    except: pass
    return jsonify(d)

@app.route("/oi/histogram")
def histogram():
    d = oi_cache.get("data")
    if not d and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f: d = json.load(f)
        except: pass
    if not d or not d.get("chain"): return jsonify([])
    chain=d["chain"]; atm=d["atm"]
    return jsonify(sorted([v for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP], key=lambda x:x["strike"]))

@app.route("/telegram/force_summary")
def force_telegram_summary():
    d = oi_cache.get("data")
    if not d and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f: d = json.load(f)
        except: pass
    if not d: return "No data available yet.", 400
    msg = generate_5min_summary(d, d.get("atm_strikes", {}), d.get("atm", 0))
    send_telegram_alert(msg)
    return "Summary sent to Telegram successfully!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)