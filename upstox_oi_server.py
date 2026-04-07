"""
====================================================
  NIFTY50 OI Server — Full Intelligence Mode v2
  Includes: Render Gunicorn Fix + Candle API Fixes
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

CACHE_TTL    = 120  
STRIKE_STEP  = 50
ATM_RANGE    = 5
SNAPSHOT_DIR = "snapshots"
TOKEN_FILE   = "token_data.json"

token_store     = {"access_token": None}
oi_cache        = {"data": None}
prev_oi         = {}
baseline_oi     = {}
prev_pcr        = None
prev_spot       = None
candle_cache    = []
candle_cache_15 = []          
ltp_history     = {}          

def save_token(token):
    token_store["access_token"] = token
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump({"access_token": token}, f)
    except Exception as e:
        print("[TOKEN SAVE ERROR]", e)

def load_token():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
                token_store["access_token"] = data.get("access_token")
        except Exception as e:
            print("[TOKEN LOAD ERROR]", e)

load_token()

def hdrs():
    load_token()
    return {
        "Authorization": f"Bearer {token_store['access_token']}", 
        "Accept": "application/json",
        "Api-Version": "2.0"  # 🔥 FIX: Required by Upstox historical endpoints
    }

# ══════════════════════════════════════════════════
#  AUTH ROUTES
# ══════════════════════════════════════════════════

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
    print("[LOGIN] Successfully generated and saved new token.")
    refresh()
    return """<html><body style="font-family:sans-serif;background:#0a0c10;color:#00e676;padding:40px">
    <h2>✅ Login Successful!</h2><p><a href="/" style="color:#40c4ff">→ Open Dashboard</a></p></body></html>"""

@app.route("/get_token")
def get_token():
    if not token_store["access_token"]: return jsonify({"error": "No token"})
    return jsonify({"token": token_store["access_token"]})


# ══════════════════════════════════════════════════
#  FETCHERS
# ══════════════════════════════════════════════════

def fetch_spot():
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp",
                         params={"symbol": NIFTY_KEY}, headers=hdrs(), timeout=10)
        d = r.json().get("data", {})
        key = list(d.keys())[0] if d else None
        return float(d[key].get("last_price", 0)) if key else 0
    except Exception as e: print("[SPOT ERROR]", e); return 0

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

def fetch_candles():
    global candle_cache
    try:
        # 🔥 FIX: Safely encode the symbol to prevent Upstox API crashes
        safe_key = urllib.parse.quote(NIFTY_KEY)
        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=6)).strftime("%Y-%m-%d")
        
        url = f"https://api.upstox.com/v2/historical-candle/{safe_key}/5minute/{to_date}/{from_date}"
        r = requests.get(url, headers=hdrs(), timeout=10)
        
        if r.status_code == 200:
            raw = r.json()
            cr  = raw.get("data", {}).get("candles", [])
            result = []
            for c in cr:
                if len(c) >= 5:
                    result.append({"time": c[0], "open": float(c[1]), "high": float(c[2]),
                                   "low": float(c[3]), "close": float(c[4])})
            # 🔥 FIX: Sort chronological (oldest first) for accurate EMA/RSI math
            result.sort(key=lambda x: x["time"])
            if result:
                candle_cache = result[-60:]
            return candle_cache
        else:
            print(f"[CANDLES 5M ERROR] HTTP {r.status_code}: {r.text}")
    except Exception as e: print("[CANDLES 5M EXCEPTION]", e)
    return candle_cache

def fetch_candles_15min():
    global candle_cache_15
    try:
        safe_key = urllib.parse.quote(NIFTY_KEY)
        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=12)).strftime("%Y-%m-%d")
        
        url = f"https://api.upstox.com/v2/historical-candle/{safe_key}/15minute/{to_date}/{from_date}"
        r = requests.get(url, headers=hdrs(), timeout=10)
        
        if r.status_code == 200:
            raw = r.json()
            cr  = raw.get("data", {}).get("candles", [])
            result = []
            for c in cr:
                if len(c) >= 5:
                    result.append({"time": c[0], "open": float(c[1]), "high": float(c[2]),
                                   "low": float(c[3]), "close": float(c[4])})
            result.sort(key=lambda x: x["time"])
            if result:
                candle_cache_15 = result[-40:]
            return candle_cache_15
        else:
            print(f"[CANDLES 15M ERROR] HTTP {r.status_code}: {r.text}")
    except Exception as e: print("[CANDLES 15M EXCEPTION]", e)
    return candle_cache_15

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
    except Exception as e: print("[EXPIRY ERROR]", e)
    today = date.today()
    days  = (3 - today.weekday()) % 7
    if days == 0: days = 7
    fb = (today + timedelta(days=days)).strftime("%Y-%m-%d")
    return fb

def fetch_chain(expiry):
    try:
        r = requests.get("https://api.upstox.com/v2/option/chain",
                         params={"instrument_key": NIFTY_KEY, "expiry_date": expiry},
                         headers=hdrs(), timeout=15)
        if r.status_code != 200:
            print(f"[CHAIN API REJECTED] HTTP {r.status_code}: {r.text}")
        data = r.json().get("data", [])
        return data
    except Exception as e: 
        print("[CHAIN ERROR]", e)
        return []

# ══════════════════════════════════════════════════
#  INDICATOR MATH
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
        return {"rsi":None,"adx":None,"pdi":None,"ndi":None,
                "rsi_signal":"N/A","adx_signal":"N/A","adx_trend":"N/A",
                "candle_count": len(candles) if candles else 0}
    closes=[c["close"] for c in candles]
    rsi=calc_rsi(closes,14)
    adx,pdi,ndi=calc_adx(candles,14)
    rs = "N/A" if rsi is None else "OVERBOUGHT" if rsi>=70 else "OVERSOLD" if rsi<=30 else "BULLISH" if rsi>=60 else "BEARISH" if rsi<=40 else "NEUTRAL"
    if adx is None: as2=at="N/A"
    elif adx>=25: as2="STRONG TREND"; at="BULLISH" if (pdi or 0)>(ndi or 0) else "BEARISH"
    elif adx>=20: as2="DEVELOPING";   at="BULLISH" if (pdi or 0)>(ndi or 0) else "BEARISH"
    else: as2="SIDEWAYS"; at="RANGING"
    return {"rsi":rsi,"adx":adx,"pdi":pdi,"ndi":ndi,
            "rsi_signal":rs,"adx_signal":as2,"adx_trend":at,
            "candle_count": len(candles)}

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

def compute_tf_signals(candles, label):
    if not candles: return {"label": label, "candle_count": 0, "error": "No data"}
    closes = [c["close"] for c in candles]
    ema7, ema15, ema21, price = calc_ema(closes, 7), calc_ema(closes, 15), calc_ema(closes, 21), closes[-1] if closes else None
    price_above_ema7  = price > ema7  if (price and ema7)  else None
    price_above_ema15 = price > ema15 if (price and ema15) else None
    ema7_above_ema15  = ema7  > ema15 if (ema7  and ema15) else None

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

    st_dir, st_val, _ = calc_supertrend(candles, period=7, multiplier=3.0)
    return {"label": label, "candle_count": len(candles), "current_price": round(price, 2) if price else None,
            "ema7": ema7, "ema15": ema15, "ema21": ema21, "price_above_ema7": price_above_ema7,
            "price_above_ema15": price_above_ema15, "ema7_above_ema15": ema7_above_ema15,
            "crossover": crossover, "trend": trend, "supertrend": st_dir, "supertrend_val": st_val,
            "rsi": calc_rsi(closes, 14) if len(closes) >= 15 else None}

def compute_index_technicals(candles_5m, candles_15m):
    return {"5min": compute_tf_signals(candles_5m, "5min"), "15min": compute_tf_signals(candles_15m, "15min")}

def compute_strike_technicals(atm_strikes):
    global ltp_history
    result = {}
    for s, v in atm_strikes.items():
        if s not in ltp_history: ltp_history[s] = {"call": [], "put": []}
        if v.get("call_ltp"): ltp_history[s]["call"] = (ltp_history[s]["call"] + [float(v["call_ltp"])])[-25:]
        if v.get("put_ltp"): ltp_history[s]["put"] = (ltp_history[s]["put"] + [float(v["put_ltp"])])[-25:]

        def side_info(prices):
            if not prices: return {"trend": "N/A", "ema7": None, "ema15": None, "crossover": None, "price_above_ema7": None, "ema7_above_ema15": None}
            e7, e15, price, xover = calc_ema(prices, 7), calc_ema(prices, 15), prices[-1], None
            if len(prices) >= 9 and e7 and e15:
                pe7, pe15 = calc_ema(prices[:-1], 7), calc_ema(prices[:-1], 15)
                if pe7 and pe15:
                    if pe7 <= pe15 and e7 > e15: xover = "GOLDEN CROSS"
                    elif pe7 >= pe15 and e7 < e15: xover = "DEATH CROSS"
            if e7 and e15:
                if price > e7 and e7 > e15: trend = "STRONG BULLISH"
                elif price > e7 and e7 < e15: trend = "RECOVERING"
                elif price < e7 and e7 > e15: trend = "MILD BEARISH"
                else: trend = "STRONG BEARISH"
            else: trend = "N/A"
            return {"trend": trend, "ema7": e7, "ema15": e15, "crossover": xover, "price_above_ema7": price > e7 if e7 else None, "ema7_above_ema15": e7 > e15 if (e7 and e15) else None}

        result[s] = {"data_points": len(ltp_history[s]["call"]), "call": side_info(ltp_history[s]["call"]), "put": side_info(ltp_history[s]["put"])}
    return result

def classify_strike_oi_flow(strike, v, prev_spot, spot):
    price_up = spot > prev_spot + 5 if prev_spot else False
    price_dn = spot < prev_spot - 5 if prev_spot else False
    THRESH = 100_000   

    c_chg, c_ltp_c, c_oi = v.get("call_oi_chg", 0) or 0, v.get("call_ltp_chg", 0) or 0, v.get("call_oi", 0) or 0
    if price_up and c_chg > THRESH and c_ltp_c > 0: cf = ("CALL LONG BUILDUP", "BULLISH", "🟢", "Price ↑ + CE OI ↑")
    elif price_up and c_chg < -THRESH and c_ltp_c > 0: cf = ("SHORT COVERING", "WEAK BULLISH", "📈", "Price ↑ + CE OI ↓")
    elif price_dn and c_chg > THRESH and c_ltp_c < 0: cf = ("FRESH CALL WRITING", "BEARISH", "🔴", "Price ↓ + CE OI ↑")
    elif price_dn and c_chg < -THRESH: cf = ("CALL LONG UNWINDING", "WEAK BEARISH", "🟡", "Price ↓ + CE OI ↓")
    elif c_chg > 500_000: cf = ("HEAVY CALL ADDITION", "WATCH", "🔴", "Large CE OI build")
    elif c_chg < -500_000: cf = ("HEAVY CALL EXIT", "BULLISH", "✅", "Large CE OI exit")
    else: cf = ("STABLE / NO CHANGE", "NEUTRAL", "⚪", "CE OI unchanged")

    p_chg, p_ltp_c, p_oi = v.get("put_oi_chg", 0) or 0, v.get("put_ltp_chg", 0) or 0, v.get("put_oi", 0) or 0
    if price_dn and p_chg > THRESH and p_ltp_c > 0: pf = ("PUT LONG BUILDUP", "BEARISH", "🔴", "Price ↓ + PE OI ↑")
    elif price_dn and p_chg < -THRESH and p_ltp_c > 0: pf = ("PUT SHORT COVERING", "WEAK BEARISH", "🟡", "Price ↓ + PE OI ↓")
    elif price_up and p_chg > THRESH and p_ltp_c < 0: pf = ("FRESH PUT WRITING", "BULLISH", "✅", "Price ↑ + PE OI ↑")
    elif price_up and p_chg < -THRESH: pf = ("PUT LONG UNWINDING", "WEAK BULLISH", "📈", "Price ↑ + PE OI ↓")
    elif p_chg > 500_000: pf = ("HEAVY PUT ADDITION", "WATCH", "✅", "Large PE OI build")
    elif p_chg < -500_000: pf = ("HEAVY PUT EXIT", "BEARISH", "🔴", "Large PE OI exit")
    else: pf = ("STABLE / NO CHANGE", "NEUTRAL", "⚪", "PE OI unchanged")

    total_chg = c_chg + p_chg
    if c_chg > THRESH and p_chg < -THRESH: net_note = "🔄 OI SHIFT: Money moving to CALL side."
    elif p_chg > THRESH and c_chg < -THRESH: net_note = "🔄 OI SHIFT: Money moving to PUT side."
    elif c_chg > THRESH and p_chg > THRESH: net_note = "💥 BOTH SIDES ADDING OI: High uncertainty."
    elif c_chg < -THRESH and p_chg < -THRESH: net_note = "🌀 BOTH SIDES EXITING: Position squareoff."
    else: net_note = "— No significant OI flow this cycle."

    return (
        {"condition": cf[0], "signal": cf[1], "emoji": cf[2], "desc": cf[3], "oi_chg_l": round(c_chg/100000,2), "oi_total_l": round(c_oi/100000,2), "ltp_chg": round(c_ltp_c, 2)},
        {"condition": pf[0], "signal": pf[1], "emoji": pf[2], "desc": pf[3], "oi_chg_l": round(p_chg/100000,2), "oi_total_l": round(p_oi/100000,2), "ltp_chg": round(p_ltp_c, 2)},
        {"total_oi_chg_l": round(total_chg/100000,2), "total_call_oi_l": round(c_oi/100000,2), "total_put_oi_l": round(p_oi/100000,2), "total_oi_l": round((c_oi+p_oi)/100000,2), "net_note": net_note}
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
        base = baseline_oi.get(strike, {})

        result[strike] = {
            "strike": strike,
            "call_oi": call_oi, "call_oi_chg": call_oi_chg, "call_oi_chg_day": call_oi - base.get("call_oi", call_oi) if base else 0,
            "call_vol": call_vol, "call_vol_oi": round(call_vol/call_oi,2) if call_oi else 0,
            "call_iv": round(call_iv,2), "call_ltp": call_ltp, "call_ltp_chg": round(call_ltp - prev.get("call_ltp", call_ltp) if prev else 0, 2),
            "call_delta": float(ce_gk.get("delta",0) or 0), "call_gamma": float(ce_gk.get("gamma",0) or 0), 
            "call_theta": float(ce_gk.get("theta",0) or 0), "call_vega": float(ce_gk.get("vega",0) or 0), "call_gex": float(ce_gk.get("gamma",0) or 0) * call_oi * 25,
            "put_oi": put_oi, "put_oi_chg": put_oi_chg, "put_oi_chg_day": put_oi - base.get("put_oi", put_oi) if base else 0,
            "put_vol": put_vol, "put_vol_oi": round(put_vol/put_oi,2) if put_oi else 0,
            "put_iv": round(put_iv,2), "put_ltp": put_ltp, "put_ltp_chg": round(put_ltp - prev.get("put_ltp", put_ltp) if prev else 0, 2),
            "put_delta": float(pe_gk.get("delta",0) or 0), "put_gamma": float(pe_gk.get("gamma",0) or 0), 
            "put_theta": float(pe_gk.get("theta",0) or 0), "put_vega": float(pe_gk.get("vega",0) or 0), "put_gex": float(pe_gk.get("gamma",0) or 0) * put_oi * 25,
            "pcr": round(put_oi/call_oi,2) if call_oi else 0, "net_oi": put_oi - call_oi,
        }
    if is_first and result: baseline_oi = {s:{"call_oi":v["call_oi"],"put_oi":v["put_oi"]} for s,v in result.items()}
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
    if pcr < 0.50:   zone, signal, note="EXTREME FEAR", "REVERSAL UP LIKELY", "PCR below 0.50. Too many calls."
    elif pcr < 0.70: zone, signal, note="BEARISH ZONE", "BEARISH — CALLS DOMINATE", "Call writers active."
    elif pcr < 0.85: zone, signal, note="MILD BEARISH", "SLIGHT BEARISH BIAS", "Calls slightly dominant."
    elif pcr <= 1.15:zone, signal, note="NEUTRAL ZONE", "BALANCED", "PCR balanced."
    elif pcr <= 1.30:zone, signal, note="MILD BULLISH", "SLIGHT BULLISH BIAS", "Puts slightly dominant."
    elif pcr <= 1.50:zone, signal, note="BULLISH ZONE", "BULLISH — PUTS DOMINATE", "Put writers active."
    else:            zone, signal, note="EXTREME GREED", "REVERSAL DOWN LIKELY", "PCR above 1.50. Too many puts."
    chg_note = f" | PCR shifted {round(pcr-prev_pcr,3):+.3f}" if prev_pcr is not None and abs(pcr-prev_pcr)>0.05 else ""
    return {"zone":zone,"signal":signal,"note":note+chg_note}

def compute_sr_strength(strike_data, is_call, total_oi):
    oi = strike_data["call_oi"] if is_call else strike_data["put_oi"]
    oi_chg = strike_data["call_oi_chg"] if is_call else strike_data["put_oi_chg"]
    voi = strike_data["call_vol_oi"] if is_call else strike_data["put_vol_oi"]
    if total_oi==0 or oi==0: return 0
    return round(max(0, min(100, min(50, (oi/total_oi)*100*3) + (20 if oi_chg>0 else (-10 if oi_chg<0 else 0)) + min(15, voi*3) + (15 if oi_chg>0 else 0))), 1)

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
    avg_conc = (call_conc+put_conc)/2
    return {"call_concentration":call_conc,"put_concentration":put_conc,"avg_concentration":round(avg_conc,1),"signal":"HIGH CONCENTRATION" if avg_conc>65 else "MODERATE" if avg_conc>45 else "LOW CONCENTRATION","top_call_strike":all_c[0][0] if all_c else None,"top_put_strike": all_p[0][0] if all_p else None}

def detect_breakout_alerts(chain, atm, spot):
    alerts=[]
    for s,v in chain.items():
        dist=s-spot
        if 0 < dist <= 3*STRIKE_STEP and v["call_oi_chg"]<0 and abs(v["call_oi_chg"])>v["call_oi"]*0.05: alerts.append({"type":"BREAKOUT UP","strike":s,"urgency":"HIGH","icon":"⚡","message":f"₹{int(s)} resistance OI dropping {(v['call_oi_chg']/100000):.2f}L"})
        if -3*STRIKE_STEP <= dist < 0 and v["put_oi_chg"]<0 and abs(v["put_oi_chg"])>v["put_oi"]*0.05: alerts.append({"type":"BREAKOUT DOWN","strike":s,"urgency":"HIGH","icon":"⚡","message":f"₹{int(s)} support OI dropping {(v['put_oi_chg']/100000):.2f}L"})
        if abs(dist)<=STRIKE_STEP:
            if v["call_oi_chg"]>v["call_oi"]*0.05: alerts.append({"type":"WALL BUILDING","strike":s,"urgency":"MEDIUM","icon":"🔴","message":f"₹{int(s)} call OI surging +{(v['call_oi_chg']/100000):.2f}L"})
            if v["put_oi_chg"]>v["put_oi"]*0.05: alerts.append({"type":"FLOOR BUILDING","strike":s,"urgency":"MEDIUM","icon":"✅","message":f"₹{int(s)} put OI surging +{(v['put_oi_chg']/100000):.2f}L"})
    return alerts[:5]

def market_state(pcr, adx, oi_matrix_signal, alerts, vix, concentration):
    signals=[]
    if oi_matrix_signal in ("BULLISH","WEAK BULLISH"):   signals.append(1)
    elif oi_matrix_signal in ("BEARISH","WEAK BEARISH"): signals.append(-1)
    if adx and adx>=25: signals.append(1 if oi_matrix_signal=="BULLISH" else -1)
    elif adx and adx<20: signals.append(0)
    if pcr<0.6 or pcr>1.5: signals.append(0)
    elif pcr<0.8: signals.append(-1)
    elif pcr>1.2: signals.append(1)
    if any(a["type"]=="BREAKOUT UP" for a in alerts): return "BREAKOUT IMMINENT","⚡","OI unwinding at resistance — upside breakout possible."
    if any(a["type"]=="BREAKOUT DOWN" for a in alerts): return "BREAKOUT IMMINENT","⚡","OI unwinding at support — downside breakdown possible."
    if pcr<0.6: return "REVERSAL ZONE","🔄","Extreme PCR — contrarian reversal up likely."
    if pcr>1.5: return "REVERSAL ZONE","🔄","Extreme PCR — contrarian reversal down likely."
    score, conc = sum(signals), concentration.get("avg_concentration",50)
    if score>=2: return "TRENDING UP","📈","Strong bullish OI." if conc>55 else "TRENDING UP (UNCONFIRMED)","📈?","Bullish signals but scattered OI."
    elif score<=-2: return "TRENDING DOWN","📉","Strong bearish OI." if conc>55 else "TRENDING DOWN (UNCONFIRMED)","📉?","Bearish signals but scattered OI."
    elif adx and adx<20: return "RANGING","↔","ADX below 20 — no clear trend."
    elif vix>25: return "HIGH VOLATILITY RANGE","⚡↔","High VIX + neutral OI = range."
    else: return "NEUTRAL / WAIT","⏳","Mixed signals."

def analyse_trend(atm_strikes, atm):
    if not atm_strikes: return "NEUTRAL","Insufficient data",50
    calls, puts = [(s,v) for s,v in atm_strikes.items() if s>atm], [(s,v) for s,v in atm_strikes.items() if s<atm]
    tc, tp = sum(v["call_oi"] for _,v in calls), sum(v["put_oi"] for _,v in puts)
    ca, pa = sum(v["call_oi_chg"] for _,v in calls if v["call_oi_chg"]>0), sum(v["put_oi_chg"] for _,v in puts if v["put_oi_chg"]>0)
    ce, pe = abs(sum(v["call_oi_chg"] for _,v in calls if v["call_oi_chg"]<0)), abs(sum(v["put_oi_chg"] for _,v in puts if v["put_oi_chg"]<0))
    pcr_atm = tp/tc if tc else 1.0
    score, reasons = 0, []
    if tc>tp*1.2: score-=2; reasons.append("Call OI dominates")
    elif tp>tc*1.2: score+=2; reasons.append("Put OI dominates")
    if ca>pa*1.3: score-=2; reasons.append("Fresh call writing")
    elif pa>ca*1.3: score+=2; reasons.append("Fresh put writing")
    if ce>pe*1.3: score+=1; reasons.append("Call unwinding")
    elif pe>ce*1.3: score-=1; reasons.append("Put unwinding")
    if pcr_atm>1.2: score+=1; reasons.append(f"PCR {pcr_atm:.2f} bullish")
    elif pcr_atm<0.8: score-=1; reasons.append(f"PCR {pcr_atm:.2f} bearish")
    
    if score>=3: return "STRONGLY BULLISH"," | ".join(reasons),90
    elif score==2: return "BULLISH"," | ".join(reasons),70
    elif score==1: return "MILD BULLISH"," | ".join(reasons),60
    elif score==-1: return "MILD BEARISH"," | ".join(reasons),40
    elif score==-2: return "BEARISH"," | ".join(reasons),30
    elif score<=-3: return "STRONGLY BEARISH"," | ".join(reasons),10
    else: return "NEUTRAL / SIDEWAYS","OI balanced",50


# ══════════════════════════════════════════════════
#  MAIN REFRESH
# ══════════════════════════════════════════════════

def refresh():
    global prev_oi, prev_pcr, prev_spot, candle_cache, candle_cache_15

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
            print(f"[REFRESH FAILED] {err_msg}")
            if oi_cache.get("data"):
                oi_cache["data"]["backend_error"] = err_msg
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

        candles_5m  = fetch_candles()
        candles_15m = fetch_candles_15min()
        ind   = get_indicators(candles_5m)
        trend, trend_reason, trend_strength = analyse_trend(atm_strikes, atm)

        oi_cond, oi_signal, oi_desc       = price_oi_matrix(spot, prev_spot, chain, atm)
        pcr_analysis                       = pcr_zone_analysis(pcr, prev_pcr)
        gex_data, gex_flip                 = compute_gex_profile(chain, atm)
        iv_skew                            = compute_iv_skew(chain, atm)
        concentration                      = compute_oi_concentration(chain, atm)
        alerts                             = detect_breakout_alerts(chain, atm, spot)
        mkt_state, mkt_icon, mkt_note      = market_state(pcr, ind.get("adx"), oi_signal, alerts, vix, concentration)

        total_c_atm = sum(v["call_oi"] for v in atm_strikes.values())
        total_p_atm = sum(v["put_oi"]  for v in atm_strikes.values())
        for s, v in atm_strikes.items():
            v["sr_strength_call"] = compute_sr_strength(v, True,  total_c_atm)
            v["sr_strength_put"]  = compute_sr_strength(v, False, total_p_atm)
            cf, pf, nf = classify_strike_oi_flow(s, v, prev_spot, spot)
            v["call_flow"], v["put_flow"], v["net_flow"] = cf, pf, nf

        index_tech = compute_index_technicals(candles_5m, candles_15m)
        strike_tech = compute_strike_technicals(atm_strikes)
        for s, tech in strike_tech.items():
            if s in atm_strikes: atm_strikes[s]["ltp_technicals"] = tech

        intelligence = {
            "market_state": mkt_state, "market_icon": mkt_icon, "market_note": mkt_note,
            "oi_matrix_condition": oi_cond, "oi_matrix_signal": oi_signal, "oi_matrix_desc": oi_desc,
            "pcr_zone": pcr_analysis["zone"], "pcr_signal": pcr_analysis["signal"], "pcr_note": pcr_analysis["note"],
            "gex_profile": gex_data[:11], "gex_flip": gex_flip, "iv_skew": iv_skew,
            "concentration": concentration, "alerts": alerts, "index_technicals": index_tech,
        }

        data = {
            "backend_error": None, 
            "spot": spot, "futures": futures, "premium": round(futures-spot,2),
            "atm": atm, "pcr": pcr, "pcr_chg": pcr_chg, "vix": vix,
            "max_pain": max_pain, "expiry": expiry,
            "trend": trend, "trend_reason": trend_reason, "trend_strength": trend_strength,
            "indicators": ind, "intelligence": intelligence,
            "atm_strikes": atm_strikes, "chain": chain,
            "timestamp": datetime.now().isoformat()
        }

        oi_cache["data"] = data

        prev_oi   = {s:{"call_oi":v["call_oi"],"put_oi":v["put_oi"],"call_ltp":v["call_ltp"],"put_ltp":v["put_ltp"]} for s,v in chain.items()}
        prev_pcr  = pcr
        prev_spot = spot

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
def dashboard():
    return send_file("dashboard.html")

@app.route("/oi/json")
def oi_json():
    if not oi_cache.get("data"): 
        return jsonify({"error":"No data — login at /login"})
    
    try:
        last_upd = datetime.fromisoformat(oi_cache["data"]["timestamp"])
        age = (datetime.now() - last_upd).total_seconds()
        if age > 180:
            print(f"[WARN] Data is {age}s old. Background thread asleep. Forcing manual refresh...")
            refresh()
    except Exception as e:
        print("[SELF-HEAL ERROR]", e)
        
    return jsonify(oi_cache["data"])

@app.route("/oi/histogram")
def histogram():
    if not oi_cache.get("data") or not oi_cache["data"].get("chain"): return jsonify([])
    chain=oi_cache["data"]["chain"]; atm=oi_cache["data"]["atm"]
    return jsonify(sorted([v for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP], key=lambda x:x["strike"]))

@app.route("/oi/status")
def oi_status():
    d=oi_cache.get("data")
    return jsonify({
        "token":        bool(token_store.get("access_token")),
        "has_data":     d is not None,
        "spot":         d.get("spot") if d else None,
        "backend_error":d.get("backend_error") if d else None,
        "updated":      d.get("timestamp") if d else None
    })

if __name__ == "__main__":
    print("=" * 55)
    print("  NIFTY OI Server — Full Intelligence Mode v2")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5000, debug=False)