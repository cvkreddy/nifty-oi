"""
====================================================
  NIFTY50 OI Server — Full Market Intelligence
  Added:
    - Price+OI trend matrix (4 conditions)
    - PCR extreme zones with reversal signals
    - GEX (Gamma Exposure) per strike
    - OI Concentration ratio
    - IV Skew (call vs put IV)
    - Support/Resistance strength score (0-100)
    - Breakout Alert when OI drops at key strike
    - Market state: TRENDING / RANGING / BREAKOUT IMMINENT
====================================================
"""

import os, csv, time, math, threading
from datetime import datetime, date, timedelta
from flask import Flask, jsonify, request, redirect, render_template
from flask_cors import CORS
import requests

app  = Flask(__name__)
CORS(app)

API_KEY      = "dc927c0f-918a-4c21-ae03-493acaa0608a"
API_SECRET   = "21ebqgxrft"
REDIRECT_URI = "https://nifty-oi.onrender.com/callback"
NIFTY_KEY    = "NSE_INDEX|Nifty 50"
CACHE_TTL    = 300
STRIKE_STEP  = 50
ATM_RANGE    = 5
SNAPSHOT_DIR = "snapshots"

token_store    = {"access_token": None}
oi_cache       = {"data": None}
prev_oi        = {}
baseline_oi    = {}
prev_pcr       = None
prev_spot      = None   # for price+OI matrix
candle_cache   = []


# ══════════════════════════════════════════════════
#  AUTH
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
    token_store["access_token"] = data.get("access_token")
    print("[LOGIN] Token:", (token_store["access_token"] or "")[:20])
    refresh()
    return """<html><body style="font-family:sans-serif;background:#0a0c10;color:#00e676;padding:40px">
    <h2>✅ Login Successful!</h2><p><a href="/" style="color:#40c4ff">→ Open Dashboard</a></p></body></html>"""

@app.route("/get_token")
def get_token():
    if not token_store["access_token"]: return jsonify({"error": "No token"})
    return jsonify({"token": token_store["access_token"]})

def hdrs():
    return {"Authorization": f"Bearer {token_store['access_token']}", "Accept": "application/json"}


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

def fetch_futures(spot):
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp",
                         params={"symbol": "NSE_FO|NIFTY25APRFUT"}, headers=hdrs(), timeout=10)
        if r.status_code == 200:
            d = r.json().get("data", {})
            if d:
                key = list(d.keys())[0]
                p = d[key].get("last_price") or d[key].get("ltp") or 0
                if p: return float(p)
    except Exception as e: print("[FUT ERROR]", e)
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
    except Exception as e: print("[VIX ERROR]", e)
    return 0

def fetch_candles():
    global candle_cache
    try:
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{NIFTY_KEY}/5minute"
        r   = requests.get(url, headers=hdrs(), timeout=10)
        if r.status_code != 200:
            today = date.today().strftime("%Y-%m-%d")
            url   = f"https://api.upstox.com/v2/historical-candle/{NIFTY_KEY}/5minute/{today}/{today}"
            r     = requests.get(url, headers=hdrs(), timeout=10)
        if r.status_code == 200:
            raw = r.json()
            cr  = raw.get("data", {})
            if isinstance(cr, dict): cr = cr.get("candles", [])
            result = []
            for c in (cr if isinstance(cr, list) else []):
                if len(c) >= 5:
                    result.append({"time": c[0], "open": float(c[1]), "high": float(c[2]),
                                   "low": float(c[3]), "close": float(c[4]),
                                   "volume": float(c[5]) if len(c) > 5 else 0})
            result.sort(key=lambda x: x["time"])
            candle_cache = result[-30:]
            print(f"[CANDLES] {len(candle_cache)} candles")
            return candle_cache
    except Exception as e: print("[CANDLES ERROR]", e)
    return candle_cache

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
                if exp >= today: print("[EXPIRY]", exp); return exp
    except Exception as e: print("[EXPIRY ERROR]", e)
    today = date.today()
    days  = (3 - today.weekday()) % 7
    if days == 0: days = 7
    fb = (today + timedelta(days=days)).strftime("%Y-%m-%d")
    print("[EXPIRY FALLBACK]", fb); return fb

def fetch_chain(expiry):
    try:
        r    = requests.get("https://api.upstox.com/v2/option/chain",
                            params={"instrument_key": NIFTY_KEY, "expiry_date": expiry},
                            headers=hdrs(), timeout=15)
        data = r.json().get("data", [])
        print(f"[CHAIN] Expiry={expiry} Records={len(data)}")
        return data
    except Exception as e: print("[CHAIN ERROR]", e); return []


# ══════════════════════════════════════════════════
#  RSI + ADX
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
                "candle_count": len(candles)}
    closes=[c["close"] for c in candles]
    rsi=calc_rsi(closes,14)
    adx,pdi,ndi=calc_adx(candles,14)
    if rsi is None: rs="N/A"
    elif rsi>=70: rs="OVERBOUGHT"
    elif rsi<=30: rs="OVERSOLD"
    elif rsi>=60: rs="BULLISH"
    elif rsi<=40: rs="BEARISH"
    else: rs="NEUTRAL"
    if adx is None: as2=at="N/A"
    elif adx>=25: as2="STRONG TREND"; at="BULLISH" if (pdi or 0)>(ndi or 0) else "BEARISH"
    elif adx>=20: as2="DEVELOPING";   at="BULLISH" if (pdi or 0)>(ndi or 0) else "BEARISH"
    else: as2="SIDEWAYS"; at="RANGING"
    return {"rsi":rsi,"adx":adx,"pdi":pdi,"ndi":ndi,
            "rsi_signal":rs,"adx_signal":as2,"adx_trend":at,
            "candle_count": len(candles)}


# ══════════════════════════════════════════════════
#  CHAIN PROCESSING
# ══════════════════════════════════════════════════

def round_to_strike(price, step=50):
    return round(round(price/step)*step, 2)

def process_chain(raw):
    global prev_oi, baseline_oi
    result={}
    is_first=len(prev_oi)==0
    for item in raw:
        strike=float(item.get("strike_price",0))
        if not strike: continue
        ce=item.get("call_options",{}); pe=item.get("put_options",{})
        ce_md=ce.get("market_data",{}); pe_md=pe.get("market_data",{})
        ce_gk=ce.get("option_greeks",{}); pe_gk=pe.get("option_greeks",{})

        call_oi  =float(ce_md.get("oi",0) or 0)
        put_oi   =float(pe_md.get("oi",0) or 0)
        call_vol =float(ce_md.get("volume",0) or 0)
        put_vol  =float(pe_md.get("volume",0) or 0)
        call_ltp =float(ce_md.get("ltp",0) or ce_md.get("last_price",0) or 0)
        put_ltp  =float(pe_md.get("ltp",0) or pe_md.get("last_price",0) or 0)
        raw_civ  =float(ce_gk.get("iv",0) or 0)
        raw_piv  =float(pe_gk.get("iv",0) or 0)
        call_iv  =raw_civ*100 if raw_civ<=5 else raw_civ
        put_iv   =raw_piv*100 if raw_piv<=5 else raw_piv
        call_delta=float(ce_gk.get("delta",0) or 0)
        put_delta =float(pe_gk.get("delta",0) or 0)
        call_gamma=float(ce_gk.get("gamma",0) or 0)
        put_gamma =float(pe_gk.get("gamma",0) or 0)
        call_theta=float(ce_gk.get("theta",0) or 0)
        put_theta =float(pe_gk.get("theta",0) or 0)
        call_vega =float(ce_gk.get("vega",0) or 0)
        put_vega  =float(pe_gk.get("vega",0) or 0)

        prev=prev_oi.get(strike,{})
        call_oi_chg    =call_oi -prev.get("call_oi", call_oi) if prev else 0
        put_oi_chg     =put_oi  -prev.get("put_oi",  put_oi)  if prev else 0
        call_ltp_chg   =call_ltp-prev.get("call_ltp",call_ltp) if prev else 0
        put_ltp_chg    =put_ltp -prev.get("put_ltp", put_ltp)  if prev else 0
        base=baseline_oi.get(strike,{})
        call_oi_chg_day=call_oi-base.get("call_oi",call_oi) if base else 0
        put_oi_chg_day =put_oi -base.get("put_oi", put_oi)  if base else 0

        # GEX = gamma * OI * lot_size * spot  (simplified, lot=25)
        call_gex = call_gamma * call_oi * 25
        put_gex  = put_gamma  * put_oi  * 25

        result[strike]={
            "strike": strike,
            "call_oi": call_oi,"call_oi_chg": call_oi_chg,"call_oi_chg_day": call_oi_chg_day,
            "call_vol": call_vol,"call_vol_oi": round(call_vol/call_oi,2) if call_oi else 0,
            "call_iv": round(call_iv,2),"call_ltp": call_ltp,"call_ltp_chg": round(call_ltp_chg,2),
            "call_delta": call_delta,"call_gamma": call_gamma,"call_theta": call_theta,"call_vega": call_vega,"call_gex": call_gex,
            "put_oi": put_oi,"put_oi_chg": put_oi_chg,"put_oi_chg_day": put_oi_chg_day,
            "put_vol": put_vol,"put_vol_oi": round(put_vol/put_oi,2) if put_oi else 0,
            "put_iv": round(put_iv,2),"put_ltp": put_ltp,"put_ltp_chg": round(put_ltp_chg,2),
            "put_delta": put_delta,"put_gamma": put_gamma,"put_theta": put_theta,"put_vega": put_vega,"put_gex": put_gex,
            "pcr": round(put_oi/call_oi,2) if call_oi else 0,
            "net_oi": put_oi-call_oi,
        }
    if is_first and result:
        baseline_oi={s:{"call_oi":v["call_oi"],"put_oi":v["put_oi"]} for s,v in result.items()}
        print(f"[OI] Baseline set — {len(baseline_oi)} strikes")
    return result

def compute_max_pain(chain):
    strikes=sorted(chain.keys())
    if not strikes: return 0
    min_loss,mp=float("inf"),strikes[0]
    for s in strikes:
        loss=sum(v["call_oi"]*(s-k) if k<s else v["put_oi"]*(k-s) if k>s else 0 for k,v in chain.items())
        if loss<min_loss: min_loss=loss; mp=s
    return mp


# ══════════════════════════════════════════════════
#  INTELLIGENCE ENGINE
# ══════════════════════════════════════════════════

def price_oi_matrix(spot, prev_spot, chain, atm):
    """
    Four-condition Price+OI matrix.
    Returns condition name, signal, and description.
    """
    if prev_spot is None or len(prev_oi)==0:
        return "INITIALIZING","—","Waiting for second data cycle"

    price_up  = spot > prev_spot
    price_dn  = spot < prev_spot

    # Total OI change in ATM±5 zone
    total_oi_chg = sum(
        v["call_oi_chg"]+v["put_oi_chg"]
        for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP
    )
    oi_up = total_oi_chg > 0
    oi_dn = total_oi_chg < 0

    if price_up and oi_up:
        return "FRESH LONG BUILD","BULLISH","New buyers entering — strong upward momentum. Hold longs."
    elif price_up and oi_dn:
        return "SHORT COVERING","WEAK BULLISH","Bears exiting, not fresh bulls. Rally may lack strength."
    elif price_dn and oi_up:
        return "FRESH SHORT BUILD","BEARISH","New sellers entering — strong downward momentum. Hold shorts."
    elif price_dn and oi_dn:
        return "LONG UNWINDING","WEAK BEARISH","Bulls exiting. Fall may slow — no new shorts yet."
    else:
        return "NO CHANGE","NEUTRAL","OI unchanged this cycle."


def pcr_zone_analysis(pcr, prev_pcr):
    """PCR extreme zones with reversal signals."""
    if pcr < 0.50:
        zone="EXTREME FEAR"; signal="REVERSAL UP LIKELY"
        note="PCR below 0.50 is extreme. Too many calls — market often surprises by going up."
    elif pcr < 0.70:
        zone="BEARISH ZONE"; signal="BEARISH — CALLS DOMINATE"
        note="Call writers active. Resistance strong. Watch for rejection at resistance levels."
    elif pcr < 0.85:
        zone="MILD BEARISH"; signal="SLIGHT BEARISH BIAS"
        note="Calls slightly dominant. Market cautious. Range-bound with downward bias."
    elif pcr <= 1.15:
        zone="NEUTRAL ZONE"; signal="BALANCED"
        note="PCR balanced. Market uncertain. Wait for breakout confirmation."
    elif pcr <= 1.30:
        zone="MILD BULLISH"; signal="SLIGHT BULLISH BIAS"
        note="Puts slightly dominant. Market cautious. Range-bound with upward bias."
    elif pcr <= 1.50:
        zone="BULLISH ZONE"; signal="BULLISH — PUTS DOMINATE"
        note="Put writers active. Support strong. Watch for bounce at support levels."
    else:
        zone="EXTREME GREED"; signal="REVERSAL DOWN LIKELY"
        note="PCR above 1.50 is extreme. Too many puts — market often surprises by going down."

    chg_note=""
    if prev_pcr is not None:
        chg=round(pcr-prev_pcr,3)
        if abs(chg)>0.05: chg_note=f" | PCR shifted {chg:+.3f} — {'puts adding fast = support building' if chg>0 else 'calls adding fast = resistance building'}"
    return {"zone":zone,"signal":signal,"note":note+chg_note}


def compute_sr_strength(strike_data, is_call, total_oi):
    """
    Strength score 0–100 for a support/resistance level.
    Based on: OI size, OI change direction, Vol/OI, proximity to ATM.
    """
    oi     = strike_data["call_oi"]    if is_call else strike_data["put_oi"]
    oi_chg = strike_data["call_oi_chg"] if is_call else strike_data["put_oi_chg"]
    voi    = strike_data["call_vol_oi"] if is_call else strike_data["put_vol_oi"]

    if total_oi==0 or oi==0: return 0

    # OI weight (50 pts max)
    oi_score = min(50, (oi/total_oi)*100*3)

    # OI adding = stronger level (20 pts)
    chg_score = 20 if oi_chg>0 else (-10 if oi_chg<0 else 0)

    # High vol/OI = active strike (15 pts max)
    vol_score = min(15, voi*3)

    # OI change direction same as OI = confirming (15 pts)
    confirm_score = 15 if oi_chg>0 else 0

    total = max(0, min(100, oi_score + chg_score + vol_score + confirm_score))
    return round(total, 1)


def compute_gex_profile(chain, atm):
    """
    Net GEX per strike. Positive = call GEX dominates (acts as magnet/resistance).
    Negative = put GEX dominates (acts as magnet/support).
    Net zero GEX strike = flip zone (price accelerates through it).
    """
    gex_data=[]
    for s,v in sorted(chain.items()):
        if abs(s-atm) > 10*STRIKE_STEP: continue
        net_gex = v["call_gex"] - v["put_gex"]
        gex_data.append({"strike":s,"call_gex":v["call_gex"],"put_gex":v["put_gex"],"net_gex":net_gex})
    # Find GEX flip zone (closest net_gex to 0)
    flip=None
    if gex_data:
        flip=min(gex_data,key=lambda x:abs(x["net_gex"]))
    return gex_data, flip


def compute_iv_skew(chain, atm):
    """
    IV Skew: compare call IV vs put IV at equal distances from ATM.
    If put_iv > call_iv → protective puts expensive → bearish sentiment.
    If call_iv > put_iv → protective calls expensive → bullish (or squeeze expected).
    """
    skew_data=[]
    for dist in [1,2,3]:
        c_strike=atm+dist*STRIKE_STEP
        p_strike=atm-dist*STRIKE_STEP
        ce=chain.get(c_strike,{}); pe=chain.get(p_strike,{})
        if ce and pe and ce.get("call_iv") and pe.get("put_iv"):
            diff=pe["put_iv"]-ce["call_iv"]
            skew_data.append({
                "dist": dist,
                "call_strike": c_strike,"call_iv": ce["call_iv"],
                "put_strike":  p_strike,"put_iv":  pe["put_iv"],
                "skew": round(diff,2)   # positive = put IV higher = bearish skew
            })
    avg_skew=round(sum(x["skew"] for x in skew_data)/len(skew_data),2) if skew_data else 0
    if avg_skew>3:   skew_signal="BEARISH SKEW — put IV elevated, market fears fall"
    elif avg_skew<-3: skew_signal="BULLISH SKEW — call IV elevated, market expects rise"
    else:             skew_signal="NEUTRAL SKEW — balanced"
    return {"data":skew_data,"avg_skew":avg_skew,"signal":skew_signal}


def compute_oi_concentration(chain, atm):
    """
    OI Concentration: how focused is the OI.
    High concentration = market knows where levels are.
    Low concentration = uncertain, scattered positions.
    """
    all_call_oi=[(s,v["call_oi"]) for s,v in chain.items() if s>=atm]
    all_put_oi =[(s,v["put_oi"])  for s,v in chain.items() if s<=atm]
    all_call_oi.sort(key=lambda x:x[1],reverse=True)
    all_put_oi.sort(key=lambda x: x[1],reverse=True)
    total_c=sum(x[1] for x in all_call_oi); total_p=sum(x[1] for x in all_put_oi)
    top3_c=sum(x[1] for x in all_call_oi[:3]); top3_p=sum(x[1] for x in all_put_oi[:3])
    call_conc=round(top3_c/total_c*100,1) if total_c else 0
    put_conc =round(top3_p/total_p*100,1) if total_p else 0
    avg_conc =(call_conc+put_conc)/2
    if avg_conc>65:   conc_sig="HIGH CONCENTRATION — clear levels, market has conviction"
    elif avg_conc>45: conc_sig="MODERATE — reasonably focused positioning"
    else:             conc_sig="LOW CONCENTRATION — scattered OI, uncertain direction"
    return {"call_concentration":call_conc,"put_concentration":put_conc,
            "avg_concentration":round(avg_conc,1),"signal":conc_sig,
            "top_call_strike":all_call_oi[0][0] if all_call_oi else None,
            "top_put_strike": all_put_oi[0][0]  if all_put_oi  else None}


def detect_breakout_alerts(chain, atm, spot):
    """
    Breakout alert: OI dropping sharply at key strike while price approaches.
    OI drop at resistance + price rising → resistance breaking → BUY signal.
    OI drop at support + price falling → support breaking → SELL signal.
    """
    alerts=[]
    for s,v in chain.items():
        dist=s-spot
        # Resistance near (above spot, within 150pts)
        if 0 < dist <= 3*STRIKE_STEP:
            if v["call_oi_chg"]<0 and abs(v["call_oi_chg"])>v["call_oi"]*0.05:
                alerts.append({
                    "type":"BREAKOUT UP","strike":s,"urgency":"HIGH",
                    "msg":f"₹{int(s)} resistance OI dropping {(v['call_oi_chg']/100000):.2f}L while price approaches — resistance weakening, upside breakout possible"
                })
        # Support near (below spot, within 150pts)
        if -3*STRIKE_STEP <= dist < 0:
            if v["put_oi_chg"]<0 and abs(v["put_oi_chg"])>v["put_oi"]*0.05:
                alerts.append({
                    "type":"BREAKOUT DOWN","strike":s,"urgency":"HIGH",
                    "msg":f"₹{int(s)} support OI dropping {(v['put_oi_chg']/100000):.2f}L while price approaches — support weakening, downside breakdown possible"
                })
        # Heavy fresh OI building near ATM = wall being built
        if abs(dist)<=STRIKE_STEP:
            if v["call_oi_chg"]>v["call_oi"]*0.05:
                alerts.append({
                    "type":"WALL BUILDING","strike":s,"urgency":"MEDIUM",
                    "msg":f"₹{int(s)} call OI surging +{(v['call_oi_chg']/100000):.2f}L — fresh resistance wall at ATM strike"
                })
            if v["put_oi_chg"]>v["put_oi"]*0.05:
                alerts.append({
                    "type":"FLOOR BUILDING","strike":s,"urgency":"MEDIUM",
                    "msg":f"₹{int(s)} put OI surging +{(v['put_oi_chg']/100000):.2f}L — fresh support floor at ATM strike"
                })
    return alerts[:5]  # top 5


def market_state(pcr, adx, oi_matrix_signal, alerts, vix, concentration):
    """
    Final market state: TRENDING UP / TRENDING DOWN / RANGING / BREAKOUT IMMINENT / REVERSAL ZONE.
    """
    signals=[]

    if oi_matrix_signal in ("BULLISH","WEAK BULLISH"): signals.append(1)
    elif oi_matrix_signal in ("BEARISH","WEAK BEARISH"): signals.append(-1)

    if adx and adx>=25: signals.append(1 if oi_matrix_signal=="BULLISH" else -1)
    elif adx and adx<20: signals.append(0)  # ranging

    if pcr<0.6 or pcr>1.5: signals.append(0)  # reversal zone
    elif pcr<0.8: signals.append(-1)
    elif pcr>1.2: signals.append(1)

    # Breakout alerts push toward breakout state
    breakout_up  =any(a["type"]=="BREAKOUT UP"   for a in alerts)
    breakout_down=any(a["type"]=="BREAKOUT DOWN" for a in alerts)

    if breakout_up or breakout_down:
        return "BREAKOUT IMMINENT","⚡","Major OI unwinding at key level detected. Prepare for fast move."

    if pcr<0.6:   return "REVERSAL ZONE","🔄","Extreme PCR — contrarian reversal up likely. Market may surprise bulls."
    if pcr>1.5:   return "REVERSAL ZONE","🔄","Extreme PCR — contrarian reversal down likely. Market may surprise bears."

    score=sum(signals)
    conc =concentration.get("avg_concentration",50)

    if score>=2:
        if conc>55: return "TRENDING UP","📈","Strong bullish OI + momentum + concentrated levels. Trend likely to continue."
        else: return "TRENDING UP (UNCONFIRMED)","📈?","Bullish signals but scattered OI — confirm with price action."
    elif score<=-2:
        if conc>55: return "TRENDING DOWN","📉","Strong bearish OI + momentum + concentrated levels. Trend likely to continue."
        else: return "TRENDING DOWN (UNCONFIRMED)","📉?","Bearish signals but scattered OI — confirm with price action."
    elif adx and adx<20:
        return "RANGING","↔","ADX below 20 — no clear trend. Trade range: sell resistance, buy support."
    elif vix>25:
        return "HIGH VOLATILITY RANGE","⚡↔","High VIX + neutral OI = range with sharp swings. Use wide stops."
    else:
        return "NEUTRAL / WAIT","⏳","Mixed signals. Wait for OI to build at a clear level before trading."


def analyse_trend(atm_strikes, atm):
    if not atm_strikes: return "NEUTRAL","Insufficient data",50
    calls=[(s,v) for s,v in atm_strikes.items() if s>atm]
    puts =[(s,v) for s,v in atm_strikes.items() if s<atm]
    tc=sum(v["call_oi"] for _,v in calls); tp=sum(v["put_oi"] for _,v in puts)
    ca=sum(v["call_oi_chg"] for _,v in calls if v["call_oi_chg"]>0)
    pa=sum(v["put_oi_chg"]  for _,v in puts  if v["put_oi_chg"]>0)
    ce=abs(sum(v["call_oi_chg"] for _,v in calls if v["call_oi_chg"]<0))
    pe=abs(sum(v["put_oi_chg"]  for _,v in puts  if v["put_oi_chg"]<0))
    pcr_atm=tp/tc if tc else 1.0
    score,reasons=0,[]
    if tc>tp*1.2: score-=2; reasons.append("Call OI dominates — resistance above")
    elif tp>tc*1.2: score+=2; reasons.append("Put OI dominates — support below")
    if ca>pa*1.3: score-=2; reasons.append("Fresh call writing — resistance building")
    elif pa>ca*1.3: score+=2; reasons.append("Fresh put writing — support building")
    if ce>pe*1.3: score+=1; reasons.append("Call unwinding — resistance easing")
    elif pe>ce*1.3: score-=1; reasons.append("Put unwinding — support easing")
    if pcr_atm>1.2: score+=1; reasons.append(f"PCR {pcr_atm:.2f} — bullish near ATM")
    elif pcr_atm<0.8: score-=1; reasons.append(f"PCR {pcr_atm:.2f} — bearish near ATM")
    if   score>=3: t,s="STRONGLY BULLISH",90
    elif score==2: t,s="BULLISH",70
    elif score==1: t,s="MILD BULLISH",60
    elif score==-1: t,s="MILD BEARISH",40
    elif score==-2: t,s="BEARISH",30
    elif score<=-3: t,s="STRONGLY BEARISH",10
    else: t,s="NEUTRAL / SIDEWAYS",50
    return t," | ".join(reasons) if reasons else "OI balanced — range-bound expected",s


# ══════════════════════════════════════════════════
#  SNAPSHOT
# ══════════════════════════════════════════════════

def save_snapshot(data, atm_strikes):
    try:
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        now=datetime.now(); ts=now.strftime("%Y-%m-%d %H:%M:%S"); ds=now.strftime("%Y-%m-%d")
        daily=os.path.join(SNAPSHOT_DIR,f"nifty_oi_{ds}.csv")
        latest=os.path.join(SNAPSHOT_DIR,"latest.csv")
        ind=data.get("indicators",{}); intel=data.get("intelligence",{})
        fields=["timestamp","spot","futures","premium","atm","pcr","pcr_chg","vix","max_pain",
                "market_state","oi_matrix","trend","trend_strength",
                "rsi","rsi_signal","adx","adx_signal","adx_trend","pdi","ndi",
                "strike","call_oi","call_oi_chg","call_oi_chg_day","call_vol","call_vol_oi",
                "call_iv","call_ltp","call_ltp_chg","call_delta","call_gamma","call_theta","call_vega","call_gex",
                "put_oi","put_oi_chg","put_oi_chg_day","put_vol","put_vol_oi",
                "put_iv","put_ltp","put_ltp_chg","put_delta","put_gamma","put_theta","put_vega","put_gex",
                "pcr_strike","net_oi","sr_strength_call","sr_strength_put"]
        total_c=sum(v["call_oi"] for v in atm_strikes.values())
        total_p=sum(v["put_oi"]  for v in atm_strikes.values())
        rows=[]
        for strike in sorted(atm_strikes.keys()):
            v=atm_strikes[strike]
            rows.append({
                "timestamp":ts,"spot":data["spot"],"futures":data["futures"],
                "premium":data["premium"],"atm":data["atm"],
                "pcr":data["pcr"],"pcr_chg":data.get("pcr_chg",0),
                "vix":data["vix"],"max_pain":data["max_pain"],
                "market_state":intel.get("market_state",""),"oi_matrix":intel.get("oi_matrix_condition",""),
                "trend":data["trend"],"trend_strength":data["trend_strength"],
                "rsi":ind.get("rsi"),"rsi_signal":ind.get("rsi_signal"),
                "adx":ind.get("adx"),"adx_signal":ind.get("adx_signal"),
                "adx_trend":ind.get("adx_trend"),"pdi":ind.get("pdi"),"ndi":ind.get("ndi"),
                "strike":int(strike),
                "call_oi":v["call_oi"],"call_oi_chg":v["call_oi_chg"],"call_oi_chg_day":v["call_oi_chg_day"],
                "call_vol":v["call_vol"],"call_vol_oi":v["call_vol_oi"],
                "call_iv":v["call_iv"],"call_ltp":v["call_ltp"],"call_ltp_chg":v["call_ltp_chg"],
                "call_delta":v["call_delta"],"call_gamma":v["call_gamma"],"call_theta":v["call_theta"],
                "call_vega":v["call_vega"],"call_gex":round(v["call_gex"],2),
                "put_oi":v["put_oi"],"put_oi_chg":v["put_oi_chg"],"put_oi_chg_day":v["put_oi_chg_day"],
                "put_vol":v["put_vol"],"put_vol_oi":v["put_vol_oi"],
                "put_iv":v["put_iv"],"put_ltp":v["put_ltp"],"put_ltp_chg":v["put_ltp_chg"],
                "put_delta":v["put_delta"],"put_gamma":v["put_gamma"],"put_theta":v["put_theta"],
                "put_vega":v["put_vega"],"put_gex":round(v["put_gex"],2),
                "pcr_strike":v["pcr"],"net_oi":v["net_oi"],
                "sr_strength_call":compute_sr_strength(v,True,total_c),
                "sr_strength_put":compute_sr_strength(v,False,total_p),
            })
        write_hdr=not os.path.exists(daily)
        with open(daily,"a",newline="") as f:
            w=csv.DictWriter(f,fieldnames=fields)
            if write_hdr: w.writeheader()
            w.writerows(rows)
        with open(latest,"w",newline="") as f:
            w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(rows)
        print(f"[SNAPSHOT] {len(rows)} rows → {daily}")
    except Exception as e: print("[SNAPSHOT ERROR]",e)


# ══════════════════════════════════════════════════
#  MAIN REFRESH
# ══════════════════════════════════════════════════

def refresh():
    global prev_oi, prev_pcr, prev_spot, candle_cache

    if not token_store["access_token"]: print("[REFRESH] No token"); return
    try:
        spot   =fetch_spot()
        expiry =get_expiry()
        raw    =fetch_chain(expiry)
        if not raw: print("[REFRESH] Empty chain"); return

        atm   =round_to_strike(spot, STRIKE_STEP)
        chain =process_chain(raw)
        if not chain: return

        atm_strikes={s:v for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP}
        total_call=sum(v["call_oi"] for v in chain.values())
        total_put =sum(v["put_oi"]  for v in chain.values())
        pcr       =round(total_put/total_call,2) if total_call else 0
        pcr_chg   =round(pcr-prev_pcr,3) if prev_pcr is not None else 0
        max_pain  =compute_max_pain(chain)
        futures   =fetch_futures(spot)
        vix       =fetch_vix()
        candles   =fetch_candles()
        ind       =get_indicators(candles)
        trend,trend_reason,trend_strength=analyse_trend(atm_strikes,atm)

        # Intelligence
        oi_cond,oi_signal,oi_desc = price_oi_matrix(spot,prev_spot,chain,atm)
        pcr_analysis              = pcr_zone_analysis(pcr,prev_pcr)
        gex_data,gex_flip         = compute_gex_profile(chain,atm)
        iv_skew                   = compute_iv_skew(chain,atm)
        concentration             = compute_oi_concentration(chain,atm)
        alerts                    = detect_breakout_alerts(chain,atm,spot)
        mkt_state,mkt_icon,mkt_note = market_state(pcr,ind.get("adx"),oi_signal,alerts,vix,concentration)

        # SR Strength for ATM±5
        total_c_atm=sum(v["call_oi"] for v in atm_strikes.values())
        total_p_atm=sum(v["put_oi"]  for v in atm_strikes.values())
        for s,v in atm_strikes.items():
            v["sr_strength_call"]=compute_sr_strength(v,True,total_c_atm)
            v["sr_strength_put"] =compute_sr_strength(v,False,total_p_atm)

        intelligence={
            "market_state":      mkt_state,
            "market_icon":       mkt_icon,
            "market_note":       mkt_note,
            "oi_matrix_condition": oi_cond,
            "oi_matrix_signal":  oi_signal,
            "oi_matrix_desc":    oi_desc,
            "pcr_zone":          pcr_analysis["zone"],
            "pcr_signal":        pcr_analysis["signal"],
            "pcr_note":          pcr_analysis["note"],
            "gex_profile":       gex_data[:11],
            "gex_flip":          gex_flip,
            "iv_skew":           iv_skew,
            "concentration":     concentration,
            "alerts":            alerts,
        }

        data={
            "spot":spot,"futures":futures,"premium":round(futures-spot,2),
            "atm":atm,"pcr":pcr,"pcr_chg":pcr_chg,"vix":vix,
            "max_pain":max_pain,"expiry":expiry,
            "trend":trend,"trend_reason":trend_reason,"trend_strength":trend_strength,
            "indicators":ind,"intelligence":intelligence,
            "atm_strikes":atm_strikes,"chain":chain,
            "timestamp":datetime.now().isoformat()
        }

        oi_cache["data"]=data
        save_snapshot(data,atm_strikes)

        prev_oi ={s:{"call_oi":v["call_oi"],"put_oi":v["put_oi"],
                     "call_ltp":v["call_ltp"],"put_ltp":v["put_ltp"]}
                  for s,v in chain.items()}
        prev_pcr=pcr; prev_spot=spot

        print(f"[OI] ✅ Spot={spot} | Fut={futures} | VIX={vix} | PCR={pcr}({pcr_chg:+.3f}) | ATM={atm} | State={mkt_state}")
        print(f"[IND] RSI={ind['rsi']} | ADX={ind['adx']} | OI Matrix={oi_cond} | Alerts={len(alerts)}")

    except Exception as e:
        import traceback; print("[REFRESH ERROR]",e); traceback.print_exc()

def loop():
    while True:
        time.sleep(CACHE_TTL); refresh()


# ══════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════

@app.route("/")
def dashboard(): return render_template("dashboard.html")

@app.route("/oi/json")
def oi_json():
    if not oi_cache["data"]: return jsonify({"error":"No data — login at /login"})
    return jsonify(oi_cache["data"])

@app.route("/oi/histogram")
def histogram():
    if not oi_cache["data"]: return jsonify([])
    chain=oi_cache["data"]["chain"]; atm=oi_cache["data"]["atm"]
    return jsonify(sorted([v for s,v in chain.items() if abs(s-atm)<=ATM_RANGE*STRIKE_STEP],key=lambda x:x["strike"]))

@app.route("/oi/status")
def oi_status():
    d=oi_cache["data"]; ind=d["indicators"] if d else {}; intel=d.get("intelligence",{}) if d else {}
    return jsonify({"token":bool(token_store["access_token"]),"has_data":d is not None,
                    "spot":d["spot"] if d else None,"pcr":d["pcr"] if d else None,
                    "market_state":intel.get("market_state"),"rsi":ind.get("rsi"),
                    "adx":ind.get("adx"),"updated":d["timestamp"] if d else None})

if __name__=="__main__":
    print("="*55)
    print("  NIFTY OI Server — Full Intelligence Mode")
    print("  Step 1: http://localhost:5000/login")
    print("  Step 2: http://localhost:5000")
    print(f"  Snapshots: ./{SNAPSHOT_DIR}/")
    print("="*55)
    threading.Thread(target=loop,daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=False)
