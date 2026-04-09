"""
====================================================
  NIFTY50 OI Server — Full Intelligence Mode v4
  Includes: Straddle Decay, Vol/OI, Telegram Pro Layout
====================================================
"""

import os, csv, time, math, threading, json, urllib.parse
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
SNAPSHOT_DIR = "snapshots"
TOKEN_FILE   = "token_data.json"
DATA_FILE    = "data_cache.json"  

token_store     = {"access_token": None}
oi_cache        = {"data": None}
prev_oi         = {}
baseline_oi     = {}
prev_pcr        = None
prev_spot       = None
candle_cache    = []
candle_cache_3m = []
candle_cache_15 = []          
ltp_history     = {}          

sent_alerts = {}
last_5min_summary = 0 

# ══════════════════════════════════════════════════
#  TELEGRAM BOT ENGINE (PRO LAYOUT)
# ══════════════════════════════════════════════════

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print("[TELEGRAM ERROR]", e)

def generate_5min_summary(data, atm_strikes, atm):
    spot = data.get("spot", 0)
    pcr = data.get("pcr", 0)
    intel = data.get("intelligence", {})
    net_flow_l = intel.get("cumulative_net_flow_l", 0)
    flow_bias = "🟢 BULLISH" if net_flow_l > 0 else "🔴 BEARISH" if net_flow_l < 0 else "⚪ NEUTRAL"
    
    atm_v = atm_strikes.get(atm, {})
    straddle = atm_v.get("call_ltp", 0) + atm_v.get("put_ltp", 0)
    s_low = int(atm - straddle) if straddle else 0
    s_high = int(atm + straddle) if straddle else 0
    
    msg = (
        f"⏱ <b>5-MIN NIFTY SCANNER</b>\n"
        f"🎯 <b>Spot:</b> ₹{spot} | <b>PCR:</b> {pcr}\n"
        f"⚖️ <b>Straddle:</b> ₹{straddle:.1f} (Range: {s_low} - {s_high})\n"
        f"🌊 <b>Smart Flow:</b> {flow_bias} ({net_flow_l:+.1f}L Net)\n"
        f"🧭 <b>State:</b> {intel.get('market_state', 'N/A')}\n"
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

    # Exact ATM +/- 2 restriction
    for s in sorted(atm_strikes.keys(), reverse=True):
        if abs(s - atm) > 2 * STRIKE_STEP:
            continue
            
        v = atm_strikes[s]
        marker = " ◄ ATM" if s == atm else ""
        
        c_ltp = v.get("call_ltp", 0)
        c_ltp_5m = v.get("call_ltp_chg", 0)
        c_ltp_d = v.get("call_ltp_chg_day", 0)
        c_oi_5m = v.get("call_oi_chg", 0) / 100000
        c_oi_d = v.get("call_oi_chg_day", 0) / 100000
        c_cond = get_short_cond(v.get("call_flow", {}))
        
        p_ltp = v.get("put_ltp", 0)
        p_ltp_5m = v.get("put_ltp_chg", 0)
        p_ltp_d = v.get("put_ltp_chg_day", 0)
        p_oi_5m = v.get("put_oi_chg", 0) / 100000
        p_oi_d = v.get("put_oi_chg_day", 0) / 100000
        p_cond = get_short_cond(v.get("put_flow", {}))

        msg += f"🎯 <b>{int(s)}{marker}</b>\n"
        msg += f"🔴 <b>CE | {c_cond}</b>\n"
        msg += f"LTP: ₹{c_ltp:.1f}  (5m: {c_ltp_5m:+.1f} | Day: {c_ltp_d:+.1f})\n"
        msg += f"OI : 5m: {c_oi_5m:+.2f}L | Day: {c_oi_d:+.2f}L\n\n"
        
        msg += f"🟢 <b>PE | {p_cond}</b>\n"
        msg += f"LTP: ₹{p_ltp:.1f}  (5m: {p_ltp_5m:+.1f} | Day: {p_ltp_d:+.1f})\n"
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
            
    except Exception as e:
        print("[TELEGRAM PROCESSING ERROR]", e)

# ══════════════════════════════════════════════════
#  AUTH
# ══════════════════════════════════════════════════

def save_token(token):
    token_store["access_token"] = token
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump({"access_token": token}, f)
    except Exception as e: pass

def load_token():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
                token_store["access_token"] = data.get("access_token")
        except Exception as e: pass

load_token()

def hdrs():
    load_token()
    return {
        "Authorization": f"Bearer {token_store['access_token']}", 
        "Accept": "application/json",
        "Api-Version": "2.0"  
    }

@app.route("/login")
def login():
    return redirect(
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URI}"
    )

@app.route("/callback")
def callback():
    code = request.args.get("code")
    resp = requests.post(
        "https://api.upstox.com/v2/login/authorization/token",
        data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET,
              "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"},
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    )
    data = resp.json()
    save_token(data.get("access_token"))
    send_telegram_alert("✅ <b>Upstox Login Successful!</b> Server is now tracking NIFTY 50.")
    refresh()
    return """<html><body style="font-family:sans-serif;background:#0a0c10;color:#00e676;padding:40px">
    <h2>✅ Login Successful!</h2><p><a href="/" style="color:#40c4ff">→ Open Dashboard</a></p></body></html>"""

@app.route("/get_token")
def get_token():
    if not token_store["access_token"]: return jsonify({"error": "No token"})
    return jsonify({"token": token_store["access_token"]})

# ══════════════════════════════════════════════════
#  FETCHERS & RESAMPLER
# ══════════════════════════════════════════════════

def fetch_spot():
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp",
                         params={"symbol": NIFTY_KEY}, headers=hdrs(), timeout=10)
        d = r.json().get("data", {})
        key = list(d.keys())[0] if d else None
        return float(d[key].get("last_price", 0)) if key else 0
    except Exception as e: return 0

def get_futures_symbol():
    from datetime import date
    now = date.today()
    months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
    for delta in [0, 1]:
        m = (now.month - 1 + delta) % 12
        y = str(now.year)[2:] 
        if now.month + delta > 12:
            y = str(now.year + 1)[2:]
        sym = f"NSE_FO|NIFTY{y}{months[m]}FUT"
        try:
            r = requests.get("https://api.upstox.com/v2/market-quote/ltp",
                             params={"symbol": sym}, headers=hdrs(), timeout=5)
            if r.status_code == 200:
                d = r.json().get("data", {})
                if d:
                    key = list(d.keys())[0]
                    p = d[key].get("last_price") or d[key].get("ltp") or 0
                    if p: return sym, float(p)
        except: pass
    return None, None

def fetch_futures(spot):
    try:
        sym, price = get_futures_symbol()
        if price: return price
    except: pass
    return round(spot * 1.005, 2)

def fetch_vix():
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp",
                         params={"symbol": "NSE_INDEX|India VIX"}, headers=hdrs(), timeout=10)
        if r.status_code == 200:
            d = r.json().get("data", {})
            if d:
                key = list(d.keys())[0]
                return float(d[key].get("last_price") or d[key].get("ltp") or 0)
    except: pass
    return 0

def fetch_base_1m_candles():
    try:
        safe_key = urllib.parse.quote(NIFTY_KEY)
        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        url = f"https://api.upstox.com/v2/historical-candle/{safe_key}/1minute/{to_date}/{from_date}"
        r = requests.get(url, headers=hdrs(), timeout=10)
        
        if r.status_code == 200:
            raw = r.json()
            cr  = raw.get("data", {}).get("candles", [])
            result = []
            for c in cr:
                if len(c) >= 5:
                    result.append({"time": c[0], "open": float(c[1]), "high": float(c[2]),
                                   "low": float(c[3]), "close": float(c[4]), "vol": float(c[5]) if len(c)>5 else 0})
            result.sort(key=lambda x: x["time"])
            return result
    except Exception as e: pass
    return []

def resample_candles(candles_1m, timeframe_mins):
    if not candles_1m: return []
    resampled = []
    curr_group = []
    curr_time = None
    
    for c in candles_1m:
        try:
            dt_str = c["time"][:16]
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M")
            minute_rounded = (dt.minute // timeframe_mins) * timeframe_mins
            group_time = dt.replace(minute=minute_rounded, second=0, microsecond=0)
            
            if curr_time is None: curr_time = group_time
            
            if group_time == curr_time:
                curr_group.append(c)
            else:
                resampled.append({
                    "time": curr_time.isoformat(),
                    "open": curr_group[0]["open"],
                    "high": max(x["high"] for x in curr_group),
                    "low": min(x["low"] for x in curr_group),
                    "close": curr_group[-1]["close"],
                    "vol": sum(x.get("vol", 0) for x in curr_group)
                })
                curr_group = [c]
                curr_time = group_time
        except: pass
            
    if curr_group:
        resampled.append({
            "time": curr_time.isoformat(),
            "open": curr_group[0]["open"],
            "high": max(x["high"] for x in curr_group),
            "low": min(x["low"] for x in curr_group),
            "close": curr_group[-1]["close"],
            "vol": sum(x.get("vol", 0) for x in curr_group)
        })
    return resampled

def get_expiry():
    try:
        r = requests.get("https://api.upstox.com/v2/option/contract",
                         params={"instrument_key": NIFTY_KEY}, headers=hdrs(), timeout=10)
        if r.status_code == 200:
            items = r.json().get("data", [])
            expiries = []
            for item in items:
                if isinstance(item, str): expiries.append(item)
                elif isinstance(item, dict):
                    e = item.get("expiry") or item.get("expiry_date")
                    if e: expiries.append(e)
            today = datetime.today().strftime("%Y-%m-%d")
            for exp in sorted(expiries):
                if exp >= today: return exp
    except Exception as e: pass
    today = date.today()
    days  = (3 - today.weekday()) % 7
    if days == 0: days = 7
    return (today + timedelta(days=days)).strftime("%Y-%m-%d")

def fetch_chain(expiry):
    try:
        r = requests.get("https://api.upstox.com/v2/option/chain",
                         params={"instrument_key": NIFTY_KEY, "expiry_date": expiry}, headers=hdrs(), timeout=15)
        return r.json().get("data", []) if r.status_code == 200 else []
    except: return []

# ══════════════════════════════════════════════════
#  INDICATOR MATH & VWAP
# ══════════════════════════════════════════════════

def calc_rsi(closes, p=14):
    if len(closes) < p+1: return None
    gains=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    losses=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    ag=sum(gains[:p])/p; al=sum(losses[:p])/p
    for i in range(p, len(gains)):
        ag=(ag*(p-1)+gains[i])/p; al=(al*(p-1)+losses[i])/p
    if al==0: return 100.0
    return round(100-100/(1+ag/al), 2)

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
    adx=sum(x[0] for x in dxl[-p:])/min(p,len(dxl))
    return round(adx,2),round(dxl[-1][1],2),round(dxl[-1][2],2)

def get_indicators(candles):
    if not candles or len(candles)<16:
        return {"rsi":None,"adx":None,"pdi":None,"ndi":None,"rsi_signal":"N/A","adx_signal":"N/A","adx_trend":"N/A"}
    closes=[c["close"] for c in candles]
    rsi=calc_rsi(closes,14)
    adx,pdi,ndi=calc_adx(candles,14)
    rs = "N/A" if rsi is None else "OVERBOUGHT" if rsi>=70 else "OVERSOLD" if rsi<=30 else "BULLISH" if rsi>=60 else "BEARISH" if rsi<=40 else "NEUTRAL"
    if adx is None: as2=at="N/A"
    elif adx>=25: as2="STRONG TREND"; at="BULLISH" if (pdi or 0)>(ndi or 0) else "BEARISH"
    elif adx>=20: as2="DEVELOPING";   at="BULLISH" if (pdi or 0)>(ndi or 0) else "BEARISH"
    else: as2="SIDEWAYS"; at="RANGING"
    return {"rsi":rsi,"adx":adx,"pdi":pdi,"ndi":ndi,"rsi_signal":rs,"adx_signal":as2,"adx_trend":at}

def calc_ema(prices, period):
    if not prices or len(prices) < period: return None
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]: ema = p * k + ema * (1 - k)
    return round(ema, 2)

def calc_supertrend(candles, period=7, multiplier=3.0):
    if len(candles) < period + 1: return None, None, "Not enough candles"
    highs, lows, closes = [c["high"] for c in candles], [c["low"] for c in candles], [c["close"] for c in candles]
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(candles))]
    if len(trs) < period: return None, None, "Not enough candles"
    atr = sum(trs[-period:]) / period
    hl2 = (highs[-1] + lows[-1]) / 2
    upper, lower, close = hl2 + multiplier * atr, hl2 - multiplier * atr, closes[-1]
    direction = "BULLISH" if close > lower else "BEARISH"
    st_val = round(lower if direction == "BULLISH" else upper, 2)
    return direction, st_val, ""

def compute_tf_signals(candles, label, st_period, st_multiplier):
    if not candles: return {"label": label, "candle_count": 0, "error": "No data"}
    
    today_date = datetime.now().strftime("%Y-%m-%d")
    cum_vol = 0
    cum_pv = 0
    vwap = None
    for c in candles:
        if c.get("time", "").startswith(today_date):
            typ_p = (c['high'] + c['low'] + c['close']) / 3
            v = c.get('vol', 0)
            cum_vol += v
            cum_pv += typ_p * v
    if cum_vol > 0: vwap = round(cum_pv / cum_vol, 2)

    closes = [c["close"] for c in candles]
    ema7, ema15, price = calc_ema(closes, 7), calc_ema(closes, 15), closes[-1] if closes else None
    
    price_above_ema7  = price > ema7  if (price and ema7)  else None
    price_above_ema15 = price > ema15 if (price and ema15) else None
    ema7_above_ema15  = ema7  > ema15 if (ema7  and ema15) else None
    price_above_vwap  = price > vwap  if (price and vwap)  else None

    crossover = None
    if len(closes) >= 9 and ema7 and ema15:
        prev_ema7, prev_ema15 = calc_ema(closes[:-1], 7), calc_ema(closes[:-1], 15)
        if prev_ema7 and prev_ema15:
            if prev_ema7 <= prev_ema15 and ema7 > ema15: crossover = "GOLDEN CROSS"
            elif prev_ema7 >= prev_ema15 and ema7 < ema15: crossover = "DEATH CROSS"

    if price_above_ema7 and ema7_above_ema15: trend = "STRONG BULLISH"
    elif price_above_ema7 and ema7_above_ema15 == False: trend = "RECOVERING"
    elif price_above_ema7 == False and ema7_above_ema15: trend = "MILD BEARISH"
    elif price_above_ema7 == False and ema7_above_ema15 == False: trend = "STRONG BEARISH"
    else: trend = "N/A"

    st_dir, st_val, _ = calc_supertrend(candles, period=st_period, multiplier=st_multiplier)
    return {"label": label, "candle_count": len(candles), "current_price": round(price, 2) if price else None,
            "ema7": ema7, "ema15": ema15, "vwap": vwap, 
            "price_above_ema7": price_above_ema7, "price_above_ema15": price_above_ema15, 
            "ema7_above_ema15": ema7_above_ema15, "price_above_vwap": price_above_vwap,
            "crossover": crossover, "trend": trend, "supertrend": st_dir, "supertrend_val": st_val,
            "rsi": calc_rsi(closes, 14) if len(closes) >= 15 else None}

def compute_index_technicals(candles_3m, candles_5m, candles_15m):
    return {
        "3min":  compute_tf_signals(candles_3m,  "3min",  st_period=1, st_multiplier=2.0),
        "5min":  compute_tf_signals(candles_5m,  "5min",  st_period=1, st_multiplier=2.0),
        "15min": compute_tf_signals(candles_15m, "15min", st_period=1, st_multiplier=1.0)
    }

def compute_strike_technicals(atm_strikes):
    global ltp_history
    result = {}
    for s, v in atm_strikes.items():
        if s not in ltp_history: ltp_history[s] = {"call": [], "put": []}
        if v.get("call_ltp"): ltp_history[s]["call"] = (ltp_history[s]["call"] + [float(v["call_ltp"])])[-25:]
        if v.get("put_ltp"): ltp_history[s]["put"] = (ltp_history[s]["put"] + [float(v["put_ltp"])])[-25:]
        def side_info(prices):
            if not prices: return {"trend": "N/A", "ema7": None, "ema15": None}
            e7, e15, price = calc_ema(prices, 7), calc_ema(prices, 15), prices[-1]
            if e7 and e15:
                if price > e7 and e7 > e15: trend = "STRONG BULLISH"
                elif price > e7 and e7 < e15: trend = "RECOVERING"
                elif price < e7 and e7 > e15: trend = "MILD BEARISH"
                else: trend = "STRONG BEARISH"
            else: trend = "N/A"
            return {"trend": trend, "ema7": e7, "ema15": e15, "price_above_ema7": price > e7 if e7 else None}
        result[s] = {"data_points": len(ltp_history[s]["call"]), "call": side_info(ltp_history[s]["call"]), "put": side_info(ltp_history[s]["put"])}
    return result

def classify_strike_oi_flow(strike, v, prev_spot, spot):
    price_up = spot > prev_spot + 5 if prev_spot else False
    price_dn = spot < prev_spot - 5 if prev_spot else False
    THRESH = 100_000   
    c_chg, c_ltp_c, c_oi = v.get("call_oi_chg", 0) or 0, v.get("call_ltp_chg", 0) or 0, v.get("call_oi", 0) or 0
    if price_up and c_chg > THRESH and c_ltp_c > 0: cf = ("CALL LONG BUILDUP", "BULLISH", "🟢")
    elif price_up and c_chg < -THRESH and c_ltp_c > 0: cf = ("SHORT COVERING", "WEAK BULLISH", "📈")
    elif price_dn and c_chg > THRESH and c_ltp_c < 0: cf = ("FRESH CALL WRITING", "BEARISH", "🔴")
    elif price_dn and c_chg < -THRESH: cf = ("CALL LONG UNWINDING", "WEAK BEARISH", "🟡")
    elif c_chg > 500_000: cf = ("HEAVY CALL ADDITION", "WATCH", "🔴")
    elif c_chg < -500_000: cf = ("HEAVY CALL EXIT", "BULLISH", "✅")
    else: cf = ("STABLE / NO CHANGE", "NEUTRAL", "⚪")

    p_chg, p_ltp_c, p_oi = v.get("put_oi_chg", 0) or 0, v.get("put_ltp_chg", 0) or 0, v.get("put_oi", 0) or 0
    if price_dn and p_chg > THRESH and p_ltp_c > 0: pf = ("PUT LONG BUILDUP", "BEARISH", "🔴")
    elif price_dn and p_chg < -THRESH and p_ltp_c > 0: pf = ("PUT SHORT COVERING", "WEAK BEARISH", "🟡")
    elif price_up and p_chg > THRESH and p_ltp_c < 0: pf = ("FRESH PUT WRITING", "BULLISH", "✅")
    elif price_up and p_chg < -THRESH: pf = ("PUT LONG UNWINDING", "WEAK BULLISH", "📈")
    elif p_chg > 500_000: pf = ("HEAVY PUT ADDITION", "WATCH", "✅")
    elif p_chg < -500_000: pf = ("HEAVY PUT EXIT", "BEARISH", "🔴")
    else: pf = ("STABLE / NO CHANGE", "NEUTRAL", "⚪")

    return (
        {"condition": cf[0], "signal": cf[1], "emoji": cf[2], "oi_chg_l": round(c_chg/100000,2)},
        {"condition": pf[0], "signal": pf[1], "emoji": pf[2], "oi_chg_l": round(p_chg/100000,2)},
        {"net_note": "—"}
    )

def round_to_strike(price, step=50): return round(round(price/step)*step, 2)

def process_chain(raw):
    global prev_oi, baseline_oi
    result={}
    is_first=len(prev_oi)==0
    for item in raw:
        strike=float(item.get("strike_price",0))
        if not strike: continue
        ce, pe = item.get("call_options",{}), item.get("put_options",{})
        ce_md, pe_md = ce.get("market_data",{}), pe.get("market_data",{})
        ce_gk, pe_gk = ce.get("option_greeks",{}), pe.get("option_greeks",{})

        call_oi, put_oi = float(ce_md.get("oi",0) or 0), float(pe_md.get("oi",0) or 0)
        call_vol, put_vol = float(ce_md.get("volume",0) or 0), float(pe_md.get("volume",0) or 0)
        call_ltp, put_ltp = float(ce_md.get("ltp",0) or ce_md.get("last_price",0) or 0), float(pe_md.get("ltp",0) or pe_md.get("last_price",0) or 0)
        raw_civ, raw_piv = float(ce_gk.get("iv",0) or 0), float(pe_gk.get("iv",0) or 0)
        call_iv, put_iv = raw_civ*100 if raw_civ<=5 else raw_civ, raw_piv*100 if raw_piv<=5 else raw_piv
        
        prev = prev_oi.get(strike, {})
        call_oi_chg = call_oi - prev.get("call_oi", call_oi) if prev else 0
        put_oi_chg = put_oi - prev.get("put_oi", put_oi) if prev else 0
        call_ltp_chg = call_ltp - prev.get("call_ltp", call_ltp) if prev else 0
        put_ltp_chg = put_ltp - prev.get("put_ltp", put_ltp) if prev else 0
        
        base = baseline_oi.get(strike, {})
        call_oi_chg_day = call_oi - base.get("call_oi", call_oi) if base else 0
        put_oi_chg_day = put_oi - base.get("put_oi", put_oi) if base else 0
        call_ltp_chg_day = call_ltp - base.get("call_ltp", call_ltp) if base else 0
        put_ltp_chg_day = put_ltp - base.get("put_ltp", put_ltp) if base else 0

        result[strike] = {
            "strike": strike,
            "call_oi": call_oi, "call_oi_chg": call_oi_chg, "call_oi_chg_day": call_oi_chg_day,
            "call_vol": call_vol, "call_vol_oi": round(call_vol/call_oi,2) if call_oi else 0,
            "call_iv": round(call_iv,2), "call_ltp": call_ltp, 
            "call_ltp_chg": round(call_ltp_chg,2), "call_ltp_chg_day": round(call_ltp_chg_day,2),
            "call_delta": float(ce_gk.get("delta",0) or 0), "call_gamma": float(ce_gk.get("gamma",0) or 0), 
            "call_gex": float(ce_gk.get("gamma",0) or 0) * call_oi * 25,
            
            "put_oi": put_oi, "put_oi_chg": put_oi_chg, "put_oi_chg_day": put_oi_chg_day,
            "put_vol": put_vol, "put_vol_oi": round(put_vol/put_oi,2) if put_oi else 0,
            "put_iv": round(put_iv,2), "put_ltp": put_ltp, 
            "put_ltp_chg": round(put_ltp_chg,2), "put_ltp_chg_day": round(put_ltp_chg_day,2),
            "put_delta": float(pe_gk.get("delta",0) or 0), "put_gamma": float(pe_gk.get("gamma",0) or 0), 
            "put_gex": float(pe_gk.get("gamma",0) or 0) * put_oi * 25,
            "pcr": round(put_oi/call_oi,2) if call_oi else 0
        }
        
    if is_first and result: 
        baseline_oi = {s: {"call_oi": v["call_oi"], "put_oi": v["put_oi"], "call_ltp": v["call_ltp"], "put_ltp": v["put_ltp"]} for s,v in result.items()}
    return result

def compute_max_pain(chain):
    strikes=sorted(chain.keys())
    if not strikes: return 0
    min_loss,mp=float("inf"),strikes[0]
    for s in strikes:
        loss=sum(v["call_oi"]*(s-k) if k<s else v["put_oi"]*(k-s) if k>s else 0 for k,v in chain.items())
        if loss<min_loss: min_loss=loss; mp=s
    return mp

def price_oi_matrix(spot, prev_spot, chain, atm):
    if prev_spot is None or len(prev_oi)==0: return "INITIALIZING","—","Waiting for second data cycle"
    price_up, price_dn = spot > prev_spot, spot < prev_spot
    total_oi_chg = sum(v["call_oi_chg"]+v["put_oi_chg"] for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP)
    if price_up and total_oi_chg > 0: return "FRESH LONG BUILD","BULLISH","New buyers entering — strong upward momentum. Hold longs."
    elif price_up and total_oi_chg < 0: return "SHORT COVERING","WEAK BULLISH","Bears exiting, not fresh bulls. Rally may lack strength."
    elif price_dn and total_oi_chg > 0: return "FRESH SHORT BUILD","BEARISH","New sellers entering — strong downward momentum. Hold shorts."
    elif price_dn and total_oi_chg < 0: return "LONG UNWINDING","WEAK BEARISH","Bulls exiting. Fall may slow — no new shorts yet."
    else: return "NO CHANGE","NEUTRAL","OI unchanged this cycle."

def pcr_zone_analysis(pcr, prev_pcr):
    if pcr < 0.50:   zone, signal, note="EXTREME FEAR", "REVERSAL UP LIKELY"
    elif pcr < 0.70: zone, signal, note="BEARISH ZONE", "BEARISH — CALLS DOMINATE"
    elif pcr < 0.85: zone, signal, note="MILD BEARISH", "SLIGHT BEARISH BIAS"
    elif pcr <= 1.15:zone, signal, note="NEUTRAL ZONE", "BALANCED"
    elif pcr <= 1.30:zone, signal, note="MILD BULLISH", "SLIGHT BULLISH BIAS"
    elif pcr <= 1.50:zone, signal, note="BULLISH ZONE", "BULLISH — PUTS DOMINATE"
    else:            zone, signal, note="EXTREME GREED", "REVERSAL DOWN LIKELY"
    return {"zone":zone,"signal":signal,"note":note}

def compute_gex_profile(chain, atm):
    gex_data = [{"strike":s,"call_gex":v["call_gex"],"put_gex":v["put_gex"],"net_gex":v["call_gex"] - v["put_gex"]} for s,v in sorted(chain.items()) if abs(s-atm) <= 10*STRIKE_STEP]
    return gex_data, min(gex_data, key=lambda x:abs(x["net_gex"])) if gex_data else None

def compute_iv_skew(chain, atm):
    skew_data=[]
    for dist in [1,2,3]:
        c_strike, p_strike = atm+dist*STRIKE_STEP, atm-dist*STRIKE_STEP
        ce, pe = chain.get(c_strike,{}), chain.get(p_strike,{})
        if ce.get("call_iv") and pe.get("put_iv"): skew_data.append({"dist":dist,"call_strike":c_strike,"call_iv":ce["call_iv"],"put_strike":p_strike,"put_iv":pe["put_iv"],"skew":round(pe["put_iv"]-ce["call_iv"],2)})
    avg_skew=round(sum(x["skew"] for x in skew_data)/len(skew_data),2) if skew_data else 0
    return {"data":skew_data,"avg_skew":avg_skew,"signal":"BEARISH SKEW — put IV elevated" if avg_skew>3 else "BULLISH SKEW — call IV elevated" if avg_skew<-3 else "NEUTRAL SKEW — balanced"}

def compute_oi_concentration(chain, atm):
    all_c = sorted([(s,v["call_oi"]) for s,v in chain.items() if s>=atm], key=lambda x:x[1], reverse=True)
    all_p = sorted([(s,v["put_oi"])  for s,v in chain.items() if s<=atm], key=lambda x:x[1], reverse=True)
    total_c, total_p = sum(x[1] for x in all_c), sum(x[1] for x in all_p)
    call_conc = round(sum(x[1] for x in all_c[:3])/total_c*100,1) if total_c else 0
    put_conc = round(sum(x[1] for x in all_p[:3])/total_p*100,1) if total_p else 0
    return {"avg_concentration":round((call_conc+put_conc)/2,1)}

def detect_breakout_alerts(chain, atm, spot):
    alerts=[]
    for s,v in chain.items():
        dist=s-spot
        if 0 < dist <= 3*STRIKE_STEP and v["call_oi_chg"]<0 and abs(v["call_oi_chg"])>v["call_oi"]*0.05: alerts.append({"type":"BREAKOUT UP","strike":s,"urgency":"HIGH","icon":"⚡","message":f"₹{int(s)} resistance OI dropping {(v['call_oi_chg']/100000):.2f}L"})
        if -3*STRIKE_STEP <= dist < 0 and v["put_oi_chg"]<0 and abs(v["put_oi_chg"])>v["put_oi"]*0.05: alerts.append({"type":"BREAKOUT DOWN","strike":s,"urgency":"HIGH","icon":"⚡","message":f"₹{int(s)} support OI dropping {(v['put_oi_chg']/100000):.2f}L"})
    return alerts[:5]

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

# ══════════════════════════════════════════════════
#  MAIN REFRESH
# ══════════════════════════════════════════════════

def refresh():
    global prev_oi, prev_pcr, prev_spot, candle_cache, candle_cache_15, candle_cache_3m

    load_token()
    if not token_store.get("access_token"): 
        print("[REFRESH] No token found in memory or disk.")
        return

    try:
        spot   = fetch_spot()
        expiry = get_expiry()
        raw    = fetch_chain(expiry)
        
        if not raw:
            err_msg = f"Upstox API returned no chain data for Expiry [{expiry}]. Token might be expired or IP rate-limited."
            if oi_cache.get("data"): oi_cache["data"]["backend_error"] = err_msg
            else: oi_cache["data"] = {"backend_error": err_msg, "timestamp": datetime.now().isoformat()}
            return

        atm   = round_to_strike(spot, STRIKE_STEP)
        chain = process_chain(raw)
        if not chain: return

        atm_strikes = {s:v for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP}
        total_call  = sum(v["call_oi"] for v in chain.values())
        total_put   = sum(v["put_oi"]  for v in chain.values())
        pcr         = round(total_put/total_call,2) if total_call else 0
        pcr_chg     = round(pcr-prev_pcr,3) if prev_pcr is not None else 0
        max_pain    = compute_max_pain(chain)
        futures     = fetch_futures(spot)
        vix         = fetch_vix()

        candles_1m  = fetch_base_1m_candles()
        candles_3m  = resample_candles(candles_1m, 3)[-60:] if candles_1m else candle_cache_3m
        candles_5m  = resample_candles(candles_1m, 5)[-60:] if candles_1m else candle_cache
        candles_15m = resample_candles(candles_1m, 15)[-40:] if candles_1m else candle_cache_15
        
        if candles_3m: candle_cache_3m = candles_3m
        if candles_5m: candle_cache = candles_5m
        if candles_15m: candle_cache_15 = candles_15m
        
        ind   = get_indicators(candle_cache)

        cum_put_add = sum(v["put_oi_chg_day"] for v in chain.values())
        cum_call_add = sum(v["call_oi_chg_day"] for v in chain.values())
        cum_net_flow = cum_put_add - cum_call_add

        # 🔥 Track Straddle Decay
        atm_v = atm_strikes.get(atm, {})
        current_straddle = atm_v.get("call_ltp", 0) + atm_v.get("put_ltp", 0)
        
        old_data = oi_cache.get("data") or {}
        old_intel = old_data.get("intelligence", {})
        morning_straddle = old_intel.get("morning_straddle")
        
        if morning_straddle is None and current_straddle > 0:
            morning_straddle = current_straddle
            
        straddle_decay = 0
        if morning_straddle and morning_straddle > 0:
            straddle_decay = ((current_straddle - morning_straddle) / morning_straddle) * 100

        oi_cond, oi_signal, oi_desc        = price_oi_matrix(spot, prev_spot, chain, atm)
        pcr_analysis                       = pcr_zone_analysis(pcr, prev_pcr)
        gex_data, gex_flip                 = compute_gex_profile(chain, atm)
        iv_skew                            = compute_iv_skew(chain, atm)
        concentration                      = compute_oi_concentration(chain, atm)
        alerts                             = detect_breakout_alerts(chain, atm, spot)
        mkt_state                          = market_state(pcr, ind.get("adx"), oi_signal, alerts, vix)

        for s, v in atm_strikes.items():
            cf, pf, nf = classify_strike_oi_flow(s, v, prev_spot, spot)
            v["call_flow"], v["put_flow"], v["net_flow"] = cf, pf, nf

        index_tech = compute_index_technicals(candle_cache_3m, candle_cache, candle_cache_15)
        strike_tech = compute_strike_technicals(atm_strikes)
        for s, tech in strike_tech.items():
            if s in atm_strikes: atm_strikes[s]["ltp_technicals"] = tech

        intelligence = {
            "market_state": mkt_state,
            "oi_matrix_condition": oi_cond, "oi_matrix_signal": oi_signal, "oi_matrix_desc": oi_desc,
            "pcr_zone": pcr_analysis["zone"], "pcr_signal": pcr_analysis["signal"], "pcr_note": pcr_analysis["note"],
            "gex_profile": gex_data[:11], "gex_flip": gex_flip, "iv_skew": iv_skew,
            "concentration": concentration, "alerts": alerts, "index_technicals": index_tech,
            "cumulative_net_flow_l": round(cum_net_flow / 100000, 2),
            "morning_straddle": morning_straddle,
            "straddle_decay": round(straddle_decay, 2)
        }

        data = {
            "backend_error": None, 
            "spot": spot, "futures": futures, "premium": round(futures-spot,2),
            "atm": atm, "pcr": pcr, "pcr_chg": pcr_chg, "vix": vix,
            "max_pain": max_pain, "expiry": expiry,
            "indicators": ind, "intelligence": intelligence,
            "atm_strikes": atm_strikes, "chain": chain,
            "timestamp": datetime.now().isoformat()
        }

        oi_cache["data"] = data

        try:
            with open(DATA_FILE, "w") as f: json.dump(data, f)
        except: pass

        prev_oi   = {s:{"call_oi":v["call_oi"],"put_oi":v["put_oi"],"call_ltp":v["call_ltp"],"put_ltp":v["put_ltp"]} for s,v in chain.items()}
        prev_pcr  = pcr
        prev_spot = spot
        
        process_telegram_alerts(alerts, data, atm_strikes, atm)
        print(f"[OI REFRESHED] Spot={spot} | PCR={pcr}({pcr_chg:+.3f}) | ATM={atm} | Error: None")
        
    except Exception as e:
        print("[REFRESH EXCEPTION ERROR]", e)
        if oi_cache.get("data"):
            oi_cache["data"]["backend_error"] = f"Internal Server Error: {str(e)}"

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
    if not d: return jsonify({"error":"No data — login at /login"})
    try:
        last_upd = datetime.fromisoformat(d["timestamp"])
        if (datetime.now() - last_upd).total_seconds() > 180:
            refresh()
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, "r") as f: d = json.load(f)
    except: pass
    return jsonify(d)

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