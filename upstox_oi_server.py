

# 🚨 TELEGRAM CREDENTIALS 🚨
TELEGRAM_BOT_TOKEN = ""  
TELEGRAM_CHAT_ID   = ""    

"""
====================================================
  NIFTY50 OI Server — Full Intelligence Mode v10 (Final Fix)
  Restored: net_flow to fix UI Crash
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
#  STATE PERSISTENCE
# ══════════════════════════════════════════════════
def load_server_state():
    global baseline_oi, baseline_vix, prev_oi, prev_pcr, prev_spot
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                st = json.load(f)
                if st.get("date") == date.today().isoformat():
                    baseline_oi = st.get("baseline_oi", {})
                    baseline_vix = st.get("baseline_vix")
                    prev_oi = st.get("prev_oi", {})
                    prev_spot = st.get("prev_spot")
                    prev_pcr = st.get("prev_pcr")
        except: pass

def save_server_state():
    try:
        st = {
            "date": date.today().isoformat(),
            "baseline_oi": baseline_oi,
            "baseline_vix": baseline_vix,
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
    
    s_curr = atm_strikes.get(atm, {}).get("call_ltp", 0) + atm_strikes.get(atm, {}).get("put_ltp", 0)
    s_decay = intel.get("straddle_decay", 0)
    s_low = int(atm - s_curr)
    s_high = int(atm + s_curr)
    
    vix_mat = intel.get("vix_matrix", {})
    
    msg = (
        f"⏱ <b>5-MIN NIFTY SCANNER</b>\n"
        f"🎯 <b>Spot:</b> ₹{spot} | <b>PCR:</b> {pcr}\n"
        f"⚖️ <b>Straddle:</b> ₹{s_curr:.1f} ({s_decay:+.1f}% Day) | Range: {s_low}-{s_high}\n"
        f"🌊 <b>Smart Flow:</b> {flow_bias} ({net_flow_l:+.1f}L Net)\n"
        f"📊 <b>VIX Matrix: {vix_mat.get('signal', 'N/A')}</b>\n"
        f"↳ <i>{vix_mat.get('desc', 'N/A')}</i>\n"
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

    for s in sorted(atm_strikes.keys(), reverse=True):
        if abs(s - atm) > 2 * STRIKE_STEP: continue
        v = atm_strikes[s]
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
#  MATH & PROCESSORS
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

def calc_rsi(closes, p=14):
    if len(closes) < p+1: return None
    gains=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    losses=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    ag=sum(gains[:p])/p; al=sum(losses[:p])/p
    for i in range(p, len(gains)):
        ag=(ag*(p-1)+gains[i])/p; al=(al*(p-1)+losses[i])/p
    return 100.0 if al==0 else round(100-100/(1+ag/al), 2)

def calc_adx(candles, p=14):
    if len(candles)<p+2: return None,None,None
    trl,pdml,ndml=[],[],[]
    for i in range(1,len(candles)):
        h,l,pc=candles[i]["high"],candles[i]["low"],candles[i-1]["close"]
        ph,pl=candles[i-1]["high"],candles[i-1]["low"]
        trl.append(max(h-l,abs(h-pc),abs(l-pc)))
        pdml.append(max(h-ph,0) if (h-ph)>(pl-l) else 0)
        ndml.append(max(pl-l,0) if (pl-l)>(h-ph) else 0)
    def sm(lst,p):
        s=sum(lst[:p]); r=[s]
        for i in range(p,len(lst)): s=s-s/p+lst[i]; r.append(s)
        return r
    atr=sm(trl,p); pDM=sm(pdml,p); nDM=sm(ndml,p)
    dxl=[]
    for i in range(len(atr)):
        if atr[i]==0: continue
        pdi=100*pDM[i]/atr[i]; ndi=100*nDM[i]/atr[i]
        dx=100*abs(pdi-ndi)/(pdi+ndi) if (pdi+ndi) else 0
        dxl.append((dx,pdi,ndi))
    if not dxl: return None,None,None
    return round(sum(x[0] for x in dxl[-p:])/min(p,len(dxl)),2), round(dxl[-1][1],2), round(dxl[-1][2],2)

def get_indicators(candles):
    if not candles or len(candles)<16: return {"rsi":None,"adx":None,"candle_count":len(candles) if candles else 0}
    closes=[c["close"] for c in candles]
    rsi=calc_rsi(closes,14)
    adx,pdi,ndi=calc_adx(candles,14)
    return {"rsi":rsi,"adx":adx,"candle_count":len(candles)}

def calc_ema(prices, period):
    if not prices or len(prices) < period: return None
    k = 2.0 / (period + 1); ema = sum(prices[:period]) / period
    for p in prices[period:]: ema = p * k + ema * (1 - k)
    return round(ema, 2)

def calc_supertrend(candles, period=7, multiplier=3.0):
    if len(candles) < period + 1: return None, None
    highs, lows, closes = [c["high"] for c in candles], [c["low"] for c in candles], [c["close"] for c in candles]
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(candles))]
    atr = sum(trs[-period:]) / period; hl2 = (highs[-1] + lows[-1]) / 2
    direction = "BULLISH" if closes[-1] > (hl2 - multiplier * atr) else "BEARISH"
    st_val = round((hl2 - multiplier * atr) if direction == "BULLISH" else (hl2 + multiplier * atr), 2)
    return direction, st_val

def compute_tf_signals(candles, label, st_period, st_multiplier):
    if not candles: return {"label": label, "candle_count": 0}
    vwap = get_vwap(candles)
    closes = [c["close"] for c in candles]
    ema7, ema15, price = calc_ema(closes, 7), calc_ema(closes, 15), closes[-1]
    
    st_dir, st_val = calc_supertrend(candles, period=st_period, multiplier=st_multiplier)
    
    trend = "N/A"
    if ema7 and ema15:
        if price > ema7 and ema7 > ema15: trend = "STRONG BULLISH"
        elif price > ema7 and ema7 < ema15: trend = "RECOVERING"
        elif price < ema7 and ema7 > ema15: trend = "MILD BEARISH"
        else: trend = "STRONG BEARISH"

    def calc_rsi_local(cls, p=14):
        if len(cls)<p+1: return None
        g=[max(cls[i]-cls[i-1],0) for i in range(1,len(cls))]
        l=[max(cls[i-1]-cls[i],0) for i in range(1,len(cls))]
        ag=sum(g[:p])/p; al=sum(l[:p])/p
        for i in range(p, len(g)): ag=(ag*(p-1)+g[i])/p; al=(al*(p-1)+l[i])/p
        return 100.0 if al==0 else round(100-100/(1+ag/al), 2)

    return {"label": label, "candle_count": len(candles), "current_price": round(price, 2) if price else None, "ema7": ema7, "ema15": ema15, "vwap": vwap, "price_above_ema7": price > ema7 if ema7 else None, "price_above_ema15": price > ema15 if ema15 else None, "ema7_above_ema15": ema7 > ema15 if ema7 and ema15 else None, "price_above_vwap": price > vwap if vwap else None, "trend": trend, "supertrend": st_dir, "supertrend_val": st_val, "rsi": calc_rsi_local(closes, 14) if len(closes)>=15 else None}

def analyze_vix_price(spot, vwap, vix, base_vix):
    if not vwap or not base_vix or base_vix == 0: 
        return {"signal": "WAITING", "desc": "Need more data for baseline comparison"}
    
    price_up = spot > vwap
    vix_up = vix > base_vix
    
    if price_up and vix_up:
        return {"signal": "STRONG BULLISH", "desc": "Price ↑ + VIX ↑ | Fear rising + Rally = Breakout expected. Avoid resistance selling."}
    elif price_up and not vix_up:
        return {"signal": "WEAK BULLISH", "desc": "Price ↑ + VIX ↓ | Move lacks power = Resistance might hold. Reversal expected."}
    elif not price_up and vix_up:
        return {"signal": "STRONG BEARISH", "desc": "Price ↓ + VIX ↑ | Panic selling = Support might break. Avoid support buying."}
    elif not price_up and not vix_up:
        return {"signal": "WEAK BEARISH", "desc": "Price ↓ + VIX ↓ | Normal correction = Support might hold. Reversal expected."}
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

# 🔥 RESTORED NET FLOW FOR JS FIX
def classify_strike_oi_flow(v, prev_spot, spot):
    pup, pdn = spot > prev_spot + 5 if prev_spot else False, spot < prev_spot - 5 if prev_spot else False
    c_c, cl, c_o = v.get("call_oi_chg", 0), v.get("call_ltp_chg", 0), v.get("call_oi", 0)
    p_c, pl, p_o = v.get("put_oi_chg", 0), v.get("put_ltp_chg", 0), v.get("put_oi", 0)
    
    THRESH = 100000
    if pup and c_c > THRESH and cl > 0: cf = ("LONG BUILDUP", "BULLISH", "🟢")
    elif pup and c_c < -THRESH and cl > 0: cf = ("SHORT COVERING", "WEAK BULLISH", "📈")
    elif pdn and c_c > THRESH and cl < 0: cf = ("FRESH CALL WRITING", "BEARISH", "🔴")
    elif pdn and c_c < -THRESH: cf = ("LONG UNWINDING", "WEAK BEARISH", "🟡")
    elif c_c > 500000: cf = ("HEAVY CALL ADDITION", "WATCH", "🔴")
    elif c_c < -500000: cf = ("HEAVY CALL EXIT", "BULLISH", "✅")
    else: cf = ("STABLE / NO CHANGE", "NEUTRAL", "⚪")

    if pdn and p_c > THRESH and pl > 0: pf = ("LONG BUILDUP", "BEARISH", "🔴")
    elif pdn and p_c < -THRESH and pl > 0: pf = ("SHORT COVERING", "WEAK BEARISH", "🟡")
    elif pup and p_c > THRESH and pl < 0: pf = ("FRESH PUT WRITING", "BULLISH", "✅")
    elif pup and p_c < -THRESH: pf = ("LONG UNWINDING", "WEAK BULLISH", "📈")
    elif p_c > 500000: pf = ("HEAVY PUT ADDITION", "WATCH", "✅")
    elif p_c < -500000: pf = ("HEAVY PUT EXIT", "BEARISH", "🔴")
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
    global prev_oi, prev_pcr, prev_spot, candle_cache, candle_cache_15, candle_cache_3m, baseline_vix

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

        atm_strikes = {s:v for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP}
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

        atm_v = atm_strikes.get(atm, {})
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

        for s, v in atm_strikes.items():
            cf, pf, nf = classify_strike_oi_flow(v, prev_spot, spot)
            v["call_flow"], v["put_flow"], v["net_flow"] = cf, pf, nf
            
        vwap_val = get_vwap(candle_cache)
        vix_matrix = analyze_vix_price(spot, vwap_val, vix, baseline_vix)
        
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
            "market_state": "TRENDING UP" if pcr > 1.2 else "TRENDING DOWN" if pcr < 0.8 else "RANGING",
            "pcr_zone": "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.8 else "NEUTRAL",
            "alerts": alerts, 
            "gex_profile": gex_data[:11], "gex_flip": gex_flip, "iv_skew": iv_skew,
            "index_technicals": {
                "3min": compute_tf_signals(candle_cache_3m, "3min", 1, 2.0),
                "5min": compute_tf_signals(candle_cache, "5min", 1, 2.0),
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
            "indicators": get_indicators(candle_cache), "intelligence": intelligence,
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