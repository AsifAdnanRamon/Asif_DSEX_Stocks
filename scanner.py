#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DSE Canvas Scanner – v1.31
================================================================================
CHANGES FROM v1.30 → v1.31:
  • HistoricalDataCollector now uses the NEW DSE JSON API:
        https://dsebd.org/api/live/data-archive/day-end
    (Old day_end_archive.php is permanently retired.)
  • Weekend handling: volume projection returns 'WEEKEND' on Fri/Sat so no
    false VOL_IGNITION is generated during closed markets.
  • Self-healing cache no longer clobbers authoritative EOD rows from the API.
  • Full 6-tab Bangla HTML generator restored (no stub).
  • Diagnostic probes for live API and history sources at batch-scan start.
  • FIX v1.31a: added get_dsex_historical() (was referenced but undefined).
================================================================================
"""

# ==================== IMPORTS & AUTO-INSTALL ====================
import sys, os, time, datetime as dt, warnings, urllib3, ssl, glob, re
import math, json, threading, sqlite3, random, traceback, subprocess
from typing import Optional, List, Dict, Tuple, Any
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, time as dtime, timedelta
from urllib.parse import urlparse
from pathlib import Path
from collections import Counter

def install(pkg, extra_args=None):
    args = [sys.executable, "-m", "pip", "install"]
    if extra_args: args.extend(extra_args)
    args.append(pkg)
    try: subprocess.check_call(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception: pass

for pkg in ["pandas", "numpy", "requests", "beautifulsoup4", "lxml", "urllib3", "scipy"]:
    try: __import__(pkg.replace("-", "_"))
    except ImportError:
        print(f"📦 Installing {pkg} ..."); install(pkg)

try: import bdshare as _bdshare_probe
except ImportError:
    print("📦 Installing bdshare ..."); install("bdshare", extra_args=["--no-deps"])

try:
    import site as _site_mod
    for _sp in {_site_mod.getusersitepackages()} | set(getattr(_site_mod, 'getsitepackages', lambda: [])()):
        if _sp and _sp not in sys.path: sys.path.append(_sp)
except Exception: pass

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

ssl._create_default_https_context = ssl._create_unverified_context
warnings.filterwarnings('ignore')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== CONSTANTS ====================
DATA_DIR    = "realtime_data"
CACHE_DIR   = "historical_cache"
RESULTS_DIR = "Stock_Report"
BANGLA_DIR  = f"{RESULTS_DIR}/bangla_summary"
DIAG_DIR    = f"{RESULTS_DIR}/diagnostics"
for d in [DATA_DIR, CACHE_DIR, RESULTS_DIR, BANGLA_DIR, DIAG_DIR]: os.makedirs(d, exist_ok=True)

HEALTH_CACHE_PATH = Path(CACHE_DIR) / "data_health.json"
MAX_WORKERS = 4
ACCOUNT_EQUITY = 1000000.0
LIQUIDITY_THRESHOLD = 10_000_000

LIVE_PRICES_TTL = 30
LIVE_MARKET_TTL = 60

MCMC_WINDOW = 20
Z_BUY, Z_SELL, Z_STOP = -1.036, 1.036, -2.326
TRADING_DAYS_1W, SHRINK_K, Z_80, Z_50 = 5, 4, 1.28, 0.70
VOLUME_CONF_CORRUPT, MCMC_SIGMA_MAX, MCMC_SIGMA_PAD = 30.0, 0.15, 1.15
LIQ_TRADE_TIER, LIQ_WATCH_TIER = 10_000_000, 5_000_000
BAND_SOFT_WATCH_FACTOR, A_MQS_SOFT_WATCH, PRIOR_SHRINK = 1.40, 0.70, 0.12

RADAR_VOL_IGNITION_MULT, RADAR_MIN_TURNOVER = 1.8, 500_000
RADAR_SQUEEZE_BB, RADAR_DRYUP_FACTOR, RADAR_NEAR_HIGH_FACTOR = 0.12, 0.60, 0.97
RADAR_RSI_LOW, RADAR_RSI_HIGH, RADAR_SCORE_MIN = 45, 70, 3

FUZZY_V3_C_CENTER, FUZZY_V3_C_STEEP = 0.60, 2.50
FUZZY_V3_W_M, FUZZY_V3_W_P, FUZZY_V3_W_C, FUZZY_V3_W_R = 0.35, 0.15, 0.30, 0.15
FUZZY_V3_L_FLOOR, FUZZY_V3_TV_LARGE = 0.50, 0.005
FUZZY_V3_TV_MID, FUZZY_V3_TV_SMALL = 0.008, 0.015
FUZZY_V3_TH_STRONG_BASE, FUZZY_V3_TH_MILD_BASE, FUZZY_V3_TH_WATCH_BASE = 0.60, 0.45, 0.30
FUZZY_V3_TH_REGIME_BOOST = 0.15

GATE_FLOOR_STRONG, GATE_FLOOR_MILD, GATE_FLOOR_WATCH = 0.38, 0.30, 0.25
SCORE_BUY, SCORE_HOLD, SCORE_CANDIDATE, STALE_WAIT_BELOW = 0.55, 0.40, 0.40, 0.40
BUY_THRESHOLD = {"Bull": 0.45, "Sideways": 0.48, "Bear": 0.55}
DEFAULT_BUY_THRESHOLD = 0.48
ADTV_HARD_FAIL, ADTV_SOFT_WARN = 500_000, 2_000_000
HEALTH_FRESH_MAX_HOUR, HEALTH_SUSPECT_MAX_HR = 24, 72
FINAL_W_FUZZY, FINAL_W_RADAR, FINAL_W_RR, FINAL_W_SME_L = 0.55, 0.20, 0.15, 0.10
FINAL_SCORE_THRESHOLD = 0.30

MCAP_LARGE, MCAP_MID = 5_000_000_000, 500_000_000
LIQ_GATE_LARGE_TURNOVER, LIQ_GATE_LARGE_VOL = 5_000_000, 100_000
LIQ_GATE_MID_TURNOVER, LIQ_GATE_MID_VOL = 1_500_000, 30_000
LIQ_GATE_SMALL_TURNOVER, LIQ_GATE_SMALL_VOL = 400_000, 10_000

AB_MAX_RISK_PCT, AB_SHORT_MODE_DAYS, AB_LONG_MODE_DAYS = 0.012, 10, 20
AB_VARIANT_A_MAX_DAYS, AB_VARIANT_B_MAX_DAYS = 10, 20
AB_VARIANT_A_TP_ATR, AB_VARIANT_A_SL_ATR = 2.0, 1.0
AB_VARIANT_B_TP_ATR, AB_VARIANT_B_SL_ATR = 3.0, 1.5
AB_MIN_FUZZY_FOR_RANK = 0.50

SME_L_SCORE_MIN, SME_L_SCORE_FLOOR_NO_MCAP = 0.30, 0.30
SME_VOLUME_MULT, SME_ADTV_MIN, SME_FREE_FLOAT_MIN = 1.25, 400_000, 1_000_000
SME_SPREAD_MAX, SME_MARKET_CAP_MIN = 0.03, 50_000_000
SME_MAX_POSITIONS, SME_MAX_CAPITAL_DEPLOYED = 6, 0.60
SME_MAX_CAPITAL_PER_TRADE, SME_HOLDING_MIN, SME_HOLDING_MAX = 0.10, 3, 12
SME_PRICE_VS_EMA20_PCT, SME_CLOSE_POSITION_MIN = 5.0, 0.50
SME_QUALITY_FUZZY_MIN, SME_QUALITY_T_STAT_MIN, SME_HALT_DAYS_MAX = 0.20, 1.40, 3

DSE_SESSION_START, DSE_SESSION_END = dtime(10, 0), dtime(14, 30)
DSE_MIN_MINUTES_FOR_PROJECTION = 5
SME_LIQ_TIERS = [(0.70, 1.00), (0.50, 0.85), (0.30, 0.65)]
SME_LIQ_MIN = 0.00

EXTENSION_BAND_NONE, EXTENSION_BAND_EARLY = 4.0, 10.0
EXTENSION_BAND_MID, EXTENSION_BAND_LATE = 20.0, 32.0
VOL_DIVERGENCE_OVERRIDE, VOL_DIVERGENCE_SOFT = 3, 2
VOL_DIVERGENCE_RSI_HOT, VOL_DIVERGENCE_LOOKBACK = 78.0, 10
CLOSE_POS_MIN_LAUNCH = 0.70
ATR_TP2_MULT, RR_FINAL_CAP = 2.8, 3.0
LATE_POSITION_MULT, LATE_RANK_PENALTY = 0.55, 0.85

TIER_NONE, TIER_EARLY, TIER_MID, TIER_LATE, TIER_EXHAUSTION = "NONE", "EARLY_SETUP", "MID_HIKE", "LATE_HIKE", "EXHAUSTION_WATCH"
TIER_ORDER = [TIER_NONE, TIER_EARLY, TIER_MID, TIER_LATE, TIER_EXHAUSTION]
TIER_BN = {
    TIER_NONE: ("⚪ লঞ্চ হয়নি", "none"), TIER_EARLY: ("🟢 প্রাথমিক সেটআপ", "early"),
    TIER_MID: ("🟡 মধ্যম হাইক", "mid"), TIER_LATE: ("🟠 উচ্চ হাইক – সতর্ক", "late"),
    TIER_EXHAUSTION: ("🔴 ক্লান্তি – প্রবেশ নয়", "exhaust"),
}

# ==================== HELPERS ====================
def compute_data_health(realtime, last_fetch_utc, now=None):
    now = now or datetime.now(timezone.utc)
    if (realtime or {}).get("source") == "live": return "LIVE"
    if last_fetch_utc is None: return "STALE"
    if isinstance(last_fetch_utc, str):
        try: last_fetch_utc = datetime.fromisoformat(last_fetch_utc)
        except ValueError: return "STALE"
    if last_fetch_utc.tzinfo is None: last_fetch_utc = last_fetch_utc.replace(tzinfo=timezone.utc)
    age = now - last_fetch_utc
    if age <= timedelta(hours=HEALTH_FRESH_MAX_HOUR): return "FRESH"
    if age <= timedelta(hours=HEALTH_SUSPECT_MAX_HR): return "SUSPECT"
    return "STALE"

def write_data_health_cache(cache_path: Path, symbol: str, fetch_utc: datetime, source: str = "live") -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        if cache_path.exists():
            try: data = json.loads(cache_path.read_text())
            except json.JSONDecodeError: data = {}
        data[symbol.upper()] = {"last_fetch_utc": fetch_utc.astimezone(timezone.utc).isoformat(), "source": source}
        cache_path.write_text(json.dumps(data, indent=2))
    except Exception: pass

def read_data_health_cache(cache_path: Path, symbol: str) -> Tuple[Optional[datetime], str]:
    if not cache_path.exists(): return None, "unknown"
    try: data = json.loads(cache_path.read_text())
    except json.JSONDecodeError: return None, "unknown"
    entry = data.get(symbol.upper())
    if not entry: return None, "unknown"
    ts = entry.get("last_fetch_utc")
    try: dt_obj = datetime.fromisoformat(ts) if ts else None
    except (ValueError, TypeError): dt_obj = None
    return dt_obj, entry.get("source", "unknown")

def get_session_minutes_elapsed() -> int:
    now = datetime.now().time()
    return (now.hour * 60 + now.minute) - (DSE_SESSION_START.hour * 60 + DSE_SESSION_START.minute)

def get_session_minutes_total() -> int:
    return ((DSE_SESSION_END.hour * 60 + DSE_SESSION_END.minute) - (DSE_SESSION_START.hour * 60 + DSE_SESSION_START.minute))

def project_full_day_volume(live_vol: float) -> Tuple[float, str]:
    """v1.31: returns 'WEEKEND' on Fri/Sat to prevent false VOL_IGNITION."""
    today = dt.date.today()
    if today.weekday() in (4, 5):  # Friday=4, Saturday=5
        return live_vol, 'WEEKEND'
    raw_min, total_min = get_session_minutes_elapsed(), get_session_minutes_total()
    if raw_min < 0: return live_vol, 'PRE_OPEN'
    if raw_min >= total_min: return live_vol, 'CLOSED'
    if raw_min < DSE_MIN_MINUTES_FOR_PROJECTION: return live_vol, 'EARLY'
    return live_vol * (total_min / raw_min), 'OK'

def safe_bool(v):
    try: return bool(v)
    except Exception: return False

def safe_float(v, decimals=4, default=0.0):
    try:
        if v is None: return default
        f = float(v)
        return default if math.isnan(f) or math.isinf(f) else round(f, decimals)
    except Exception: return default

def safe_int(v, default=0):
    try: return int(v)
    except Exception: return default

def safe_str(v, default=''):
    try: return str(v)
    except Exception: return default

def json_default_handler(obj):
    if isinstance(obj, np.bool_): return bool(obj)
    if isinstance(obj, np.integer): return int(obj)
    if isinstance(obj, np.floating):
        f = float(obj)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(obj, np.ndarray): return obj.tolist()
    if isinstance(obj, (dt.date, dt.datetime)): return obj.isoformat()
    return str(obj)

def safe_json_dumps(data, ensure_ascii=False):
    return json.dumps(data, ensure_ascii=False, default=json_default_handler)

def get_forecast_thresholds(regime):
    if regime == 'Bull': return {'T_TRADE': 1.80, 'BAND_TRADE': 9.0, 'T_WATCH': 1.30, 'BAND_WATCH': 16.0}
    if regime == 'Sideways': return {'T_TRADE': 1.65, 'BAND_TRADE': 10.5, 'T_WATCH': 1.25, 'BAND_WATCH': 18.0}
    return {'T_TRADE': 1.95, 'BAND_TRADE': 8.5, 'T_WATCH': 1.40, 'BAND_WATCH': 15.0}

# ==================== DB ====================
DB_PATH = "market_data.db"
DB_LOCK = threading.Lock()

class MarketDatabase:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS stock_data (symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY (symbol, date))''')
        c.execute('''CREATE TABLE IF NOT EXISTS analysis_cache (symbol TEXT, timestamp DATETIME, price REAL, rsi REAL, macd_status TEXT, adx REAL, atr REAL, signal_type TEXT, wyckoff_phase TEXT, vpoc REAL, vah REAL, val REAL, PRIMARY KEY (symbol, timestamp))''')
        self.conn.commit()
    def save_stock_data(self, symbol, df):
        with DB_LOCK:
            c = self.conn.cursor()
            for _, row in df.iterrows():
                try:
                    ds = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])
                    c.execute('INSERT OR REPLACE INTO stock_data VALUES (?,?,?,?,?,?,?)',
                              (symbol, ds, float(row['open']), float(row['high']), float(row['low']), float(row['close']), int(row['volume'])))
                except Exception: pass
            self.conn.commit()
    def close(self):
        if self.conn: self.conn.close()

# ==================== HTTP SESSION ====================
SESSION = requests.Session()
SESSION.verify = False

@dataclass
class DSEConfig:
    verify_ssl: bool = False
    min_request_interval: float = 0.4
    max_retries: int = 3
    backoff_base: float = 1.5
    user_agent: str = "Mozilla/5.0 (compatible; dse-swing-analyzer/3.4)"
    connect_timeout: float = 5.0
    read_timeout: float = 20.0

class PoliteSession:
    def __init__(self, cfg: DSEConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.verify = cfg.verify_ssl
        self.session.headers.update({"User-Agent": cfg.user_agent, "Accept": "application/json, text/html, */*", "Accept-Language": "en-US,en;q=0.5"})
        self._last_hit, self._lock = {}, threading.Lock()
    def get(self, url, **kw):
        host = urlparse(url).netloc
        for attempt in range(self.cfg.max_retries):
            if attempt > 0: time.sleep(self.cfg.backoff_base ** (attempt - 1))
            with self._lock:
                last = self._last_hit.get(host, 0.0)
                wait = self.cfg.min_request_interval - (time.monotonic() - last)
                if wait > 0: time.sleep(wait)
                self._last_hit[host] = time.monotonic()
            try:
                r = self.session.get(url, timeout=(self.cfg.connect_timeout, self.cfg.read_timeout), **kw)
                if r.status_code == 200 and len(r.text) > 5: return r
            except requests.RequestException: continue
        return None

_cfg_interval = 0.4
try:
    _env_interval = os.environ.get("BDSHARE_MIN_INTERVAL")
    if _env_interval: _cfg_interval = float(_env_interval)
except Exception: pass
_CFG = DSEConfig(min_request_interval=_cfg_interval)
_HTTP = PoliteSession(_CFG)

# ==================== bdshare BINDINGS ====================
_BDSHARE_MODULE = None
try: import bdshare as _BDSHARE_MODULE
except Exception: _BDSHARE_MODULE = None

def _bd_fn(name): return getattr(_BDSHARE_MODULE, name, None) if _BDSHARE_MODULE else None
_BDSHARE_HIST = _bd_fn("get_historical_data")
_BDSHARE_HIST_BASIC = _bd_fn("get_basic_historical_data")
_BDSHARE_MKT_HIST_MORE = _bd_fn("get_market_info_more_data")
_BDSHARE_MKT_HIST = _bd_fn("get_market_info")
_BDSHARE_COMPANY_INFO = _bd_fn("get_company_info")

# ==================== LIVE MARKET CACHE ====================
DSE_LIVE_PRICES_URLS = ["https://www.dse.com.bd/api/live/prices", "https://dsebd.org/api/live/prices"]
DSE_LIVE_MARKET_URLS = ["https://www.dse.com.bd/api/live/market", "https://dsebd.org/api/live/market"]
_LIVE_NUMERIC_COLS = ("ltp", "ycp", "open", "high", "low", "close", "volume", "value", "trades", "percent")

class LiveMarketCache:
    def __init__(self, http):
        self.http = http
        self._lock = threading.RLock()
        self._prices_df, self._prices_ts = None, 0.0
        self._market, self._market_ts = None, 0.0
        self._fail_count = 0

    def _refresh_prices_locked(self):
        for url in DSE_LIVE_PRICES_URLS:
            r = self.http.get(url)
            if not r: continue
            try: payload = r.json()
            except Exception: continue
            cols, rows = payload.get("cols") or [], payload.get("rows") or []
            if not cols or not rows: continue
            try:
                df = pd.DataFrame(rows, columns=cols)
                if "code" not in df.columns: continue
                df["code"] = df["code"].astype(str).str.upper().str.strip()
                for c in _LIVE_NUMERIC_COLS:
                    if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
                if "close" in df.columns and "ltp" in df.columns:
                    m = df["close"] <= 0; df.loc[m, "close"] = df.loc[m, "ltp"]
                for c in ("sector", "assetType", "category", "board"):
                    if c in df.columns: df[c] = df[c].astype(str).fillna("")
                self._prices_df, self._prices_ts, self._fail_count = df, time.monotonic(), 0
                return
            except Exception: continue
        self._fail_count += 1

    def refresh_prices(self):
        with self._lock: self._refresh_prices_locked()

    def get_all(self):
        with self._lock:
            if self._prices_df is None or (time.monotonic() - self._prices_ts) > LIVE_PRICES_TTL:
                self._refresh_prices_locked()
            return self._prices_df

    def get_symbol(self, symbol):
        df = self.get_all()
        if df is None or df.empty: return None
        sym = symbol.upper().strip()
        sub = df[df["code"] == sym]
        if sub.empty: return None
        r = sub.iloc[0]
        ltp = float(r.get("ltp", 0) or 0)
        return {"ltp": ltp, "ycp": float(r.get("ycp", 0) or 0), "open": float(r.get("open", 0) or 0) or float(r.get("ycp", 0) or ltp),
                "high": float(r.get("high", 0) or 0) or ltp, "low": float(r.get("low", 0) or 0) or ltp,
                "close": float(r.get("close", 0) or 0) or ltp, "volume": int(r.get("volume", 0) or 0),
                "value": float(r.get("value", 0) or 0), "change_pct": float(r.get("percent", 0) or 0),
                "sector": str(r.get("sector", "") or ""), "asset_type": str(r.get("assetType", "") or ""),
                "board": str(r.get("board", "") or ""), "source": "live", "_ohlc_source": True,
                "date": dt.date.today().strftime("%Y-%m-%d")}

    def get_symbols(self):
        df = self.get_all()
        return sorted(df["code"].dropna().unique().tolist()) if df is not None and not df.empty else []

    def _refresh_market_locked(self):
        for url in DSE_LIVE_MARKET_URLS:
            r = self.http.get(url)
            if r:
                try:
                    payload = r.json()
                    if isinstance(payload, dict):
                        self._market, self._market_ts = payload, time.monotonic()
                        return
                except Exception: continue

    def get_market(self):
        with self._lock:
            if self._market is None or (time.monotonic() - self._market_ts) > LIVE_MARKET_TTL:
                self._refresh_market_locked()
            return self._market or {}

    def prefetch(self):
        self.refresh_prices(); self.get_market()

_LIVE_CACHE = LiveMarketCache(_HTTP)

# ==================== HISTORICAL DATA (v1.31 — new JSON API) ====================
class HistoricalDataCollector:
    def __init__(self, http):
        self.http = http
        self._lock = threading.Lock()

    @staticmethod
    def _normalize(df, symbol):
        if df is None or len(df) == 0: return None
        try:
            d = df.copy()
            d.columns = [str(c).strip().lower() for c in d.columns]
            d = d.rename(columns={"openp":"open","closep":"close","ltp":"close","date_":"date",
                                   "trade_date":"date","tradingcode":"symbol","code":"symbol","tradedate":"date"})
            if "date" not in d.columns:
                d = d.reset_index(); d.columns = [str(c).strip().lower() for c in d.columns]
                if "index" in d.columns and "date" not in d.columns:
                    d = d.rename(columns={"index": "date"})
            if "date" not in d.columns: return None
            d["date"] = pd.to_datetime(d["date"], errors="coerce")
            d = d.dropna(subset=["date"])
            if d.empty: return None
            for col in ["open","high","low","close","volume"]:
                if col not in d.columns: d[col] = 0.0
                d[col] = pd.to_numeric(d[col], errors="coerce").fillna(0.0)
            for col in ["open","high","low"]:
                m = (d[col] <= 0) & (d["close"] > 0); d.loc[m, col] = d.loc[m, "close"]
            d = d[d["close"] > 0]
            if len(d) < 20: return None
            return d[["date","open","high","low","close","volume"]].sort_values("date").reset_index(drop=True)
        except Exception: return None

    def _from_new_json_api(self, symbol, start_date, end_date):
        """v1.31 — the CURRENT working endpoint."""
        urls = [
            "https://dsebd.org/api/live/data-archive/day-end",
            "https://www.dsebd.org/api/live/data-archive/day-end",
            "https://www.dse.com.bd/api/live/data-archive/day-end",
        ]
        for url in urls:
            try:
                r = self.http.get(url, params={"from": start_date, "to": end_date, "inst": symbol.upper()})
                if not r: continue
                try: payload = r.json()
                except Exception: continue
                rows = payload if isinstance(payload, list) else (payload.get("data") or payload.get("rows") or [])
                if not rows: continue
                df = pd.DataFrame(rows)
                out = self._normalize(df, symbol)
                if out is not None and len(out) >= 20: return out
            except Exception: continue
        return None

    def _from_bdshare(self, symbol, start_date, end_date):
        if _BDSHARE_HIST:
            try:
                out = self._normalize(_BDSHARE_HIST(start_date, end_date, symbol.upper()), symbol)
                if out is not None: return out
            except Exception: pass
        if _BDSHARE_HIST_BASIC:
            try:
                out = self._normalize(_BDSHARE_HIST_BASIC(start_date, end_date, symbol.upper()), symbol)
                if out is not None: return out
            except Exception: pass
        return None

    def fetch(self, symbol, start_date, end_date):
        symbol = symbol.upper().strip()
        if symbol in ("DSEX","DSES","DS30","DGEN"): return None
        with self._lock:
            df = self._from_new_json_api(symbol, start_date, end_date)
            if df is not None: return df
            return self._from_bdshare(symbol, start_date, end_date)

_HIST_COLLECTOR = HistoricalDataCollector(_HTTP)

# ==================== DSEX + BREADTH ====================
DSEX_CACHE_FILE = f"{CACHE_DIR}/DSEX_historical.csv"
BREADTH_CACHE_FILE = f"{CACHE_DIR}/breadth_history.csv"
_BREADTH_HISTORY = None

def _extract_dsex_from_market(market):
    if not market: return None
    try:
        for key in ("indices","index","market","summary"):
            node = market.get(key)
            if isinstance(node, list):
                for entry in node:
                    if isinstance(entry, dict) and str(entry.get("code") or entry.get("name") or "").upper() == "DSEX":
                        return {"close": float(entry.get("ltp") or entry.get("close") or 0), "change_pct": float(entry.get("percent") or entry.get("change") or 0), "high": float(entry.get("high") or 0), "low": float(entry.get("low") or 0), "advances": int(entry.get("advances") or 0), "declines": int(entry.get("declines") or 0)}
            elif isinstance(node, dict) and "DSEX" in node:
                v = node["DSEX"]
                if isinstance(v, dict): return {"close": float(v.get("ltp") or v.get("close") or 0), "change_pct": float(v.get("percent") or 0), "high": float(v.get("high") or 0), "low": float(v.get("low") or 0), "advances": int(v.get("advances") or 0), "declines": int(v.get("declines") or 0)}
        adv, dec = market.get("advances") or market.get("advancing"), market.get("declines") or market.get("declining")
        if adv is not None and dec is not None: return {"close": 0.0, "change_pct": 0.0, "high": 0.0, "low": 0.0, "advances": int(adv), "declines": int(dec)}
    except Exception: pass
    return None

def get_dsex_historical():
    """
    v1.31a — returns a pandas Series of DSEX closes (DatetimeIndex),
    or None if not enough history. Loads from local CSV cache and appends
    today's close from the live market API when available.
    """
    closes = None
    if os.path.exists(DSEX_CACHE_FILE):
        try:
            df = pd.read_csv(DSEX_CACHE_FILE)
            if 'date' in df.columns and 'close' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date', 'close']).sort_values('date').drop_duplicates('date', keep='last')
                if not df.empty:
                    closes = df.set_index('date')['close']
        except Exception:
            closes = None

    # Try to append / refresh from live market
    try:
        market = _LIVE_CACHE.get_market()
        dsex = _extract_dsex_from_market(market)
        if dsex and dsex.get("close", 0) > 0:
            today = pd.Timestamp(dt.date.today())
            new_row = {"date": today, "close": float(dsex["close"]),
                       "high": float(dsex.get("high") or 0), "low": float(dsex.get("low") or 0)}
            if closes is not None and today in closes.index:
                closes.loc[today] = float(dsex["close"])
            elif closes is not None:
                closes.loc[today] = float(dsex["close"])
            else:
                closes = pd.Series([float(dsex["close"])], index=pd.DatetimeIndex([today]))
            # Persist full cache
            try:
                out = closes.reset_index(); out.columns = ['date', 'close']
                if 'high' in locals() and 'low' in locals():
                    pass
                out.to_csv(DSEX_CACHE_FILE, index=False)
            except Exception:
                pass
    except Exception:
        pass

    if closes is None or len(closes) == 0:
        return None
    closes = closes.sort_index()
    return closes

def load_breadth_history():
    global _BREADTH_HISTORY
    if _BREADTH_HISTORY is not None: return _BREADTH_HISTORY
    if os.path.exists(BREADTH_CACHE_FILE):
        try:
            df = pd.read_csv(BREADTH_CACHE_FILE); df['date'] = pd.to_datetime(df['date'])
            _BREADTH_HISTORY = df.to_dict('records'); return _BREADTH_HISTORY
        except Exception: pass
    _BREADTH_HISTORY = []; return _BREADTH_HISTORY

def save_breadth_data(date, advances, declines):
    global _BREADTH_HISTORY
    if _BREADTH_HISTORY is None: load_breadth_history()
    ds = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
    ex = [r for r in _BREADTH_HISTORY if str(r.get('date')) == ds]
    if ex: ex[0]['advances'], ex[0]['declines'] = advances, declines
    else: _BREADTH_HISTORY.append({'date': ds, 'advances': advances, 'declines': declines})
    _BREADTH_HISTORY = sorted(_BREADTH_HISTORY, key=lambda x: str(x['date']))
    try: pd.DataFrame(_BREADTH_HISTORY).to_csv(BREADTH_CACHE_FILE, index=False)
    except Exception: pass

def compute_breadth_metrics():
    h = load_breadth_history()
    if not h or len(h) < 40: return {'theta': 0.0, 'mcclellan': 0.0, 'condition': 'Neutral'}
    df = pd.DataFrame(h); df['date'] = pd.to_datetime(df['date']); df = df.sort_values('date').reset_index(drop=True)
    df['net'] = df['advances'] - df['declines']; df['ad_line'] = df['net'].cumsum()
    df['ad_roc_21'] = df['ad_line'] - df['ad_line'].shift(21)
    df['kappa'] = df['ad_roc_21'].rolling(252).std()
    df['theta'] = np.tanh(df['ad_roc_21'] / df['kappa'].replace(0, np.nan))
    df['ema19'] = df['net'].ewm(span=19, adjust=False).mean()
    df['ema39'] = df['net'].ewm(span=39, adjust=False).mean()
    df['mcclellan'] = df['ema19'] - df['ema39']
    last = df.iloc[-1]; theta = last.get('theta', 0.0)
    if pd.isna(theta): theta = 0.0
    cond = ('Strong Bullish' if theta > 0.7 else 'Moderate Bullish' if theta > 0.3 else 'Neutral' if theta > -0.3 else 'Moderate Bearish' if theta > -0.7 else 'Strong Bearish')
    mc = last.get('mcclellan', 0.0)
    return {'theta': theta, 'mcclellan': 0.0 if pd.isna(mc) else mc, 'condition': cond}

def determine_regime(dsex):
    if dsex is None or len(dsex) < 200: return {'regime': 'NEUTRAL', 'factor': 0.65, 'above_200': False}
    close, sma200 = dsex.iloc[-1], dsex.rolling(200).mean().iloc[-1]
    rets = dsex.pct_change().dropna()
    vol = rets.tail(30).std() * np.sqrt(252) if len(rets) >= 30 else 0.20
    pvm = (close / sma200 - 1) * 100
    adx_proxy = min(60, max(10, 20 + abs(pvm) * 0.5))
    above, vh = close > sma200, vol >= 0.20
    if above and adx_proxy >= 25 and not vh: r, f = 'Bull', 1.0
    elif above and adx_proxy >= 25 and vh: r, f = 'Bull', 0.85
    elif above and adx_proxy < 25 and not vh: r, f = 'Sideways', 0.65
    elif above and adx_proxy < 25 and vh: r, f = 'Sideways', 0.50
    elif not above and not vh: r, f = 'Bear', 0.35
    else: r, f = 'Bear', 0.20
    return {'regime': r, 'factor': f, 'above_200': above}

# ==================== STOCK UNIVERSE ====================
_FALLBACK_SYMBOLS = ['1JANATAMF','1STPRIMFMF','AAMRANET','AAMRATECH','ABB1STMF','ABBANK','ACFL','ACI','ACIFORMULA','ACMELAB','ACMEPL','ACTIVEFINE','ADNTEL','ADVENT','AFCAGRO','AFTABAUTO','AGNISYSL','AGRANINS','AIBL1STIMF','AIL','AL-HAJTEX','ALARABANK','ALIF','ALLTEX','AMANFEED','AMBEEPHA','AMCL(PRAN)','ANLIMAYARN','ANWARGALV','AOL','APEXFOODS','APEXFOOT','APEXSPINN','APEXTANRY','APOLOISPAT','ARAMIT','ARAMITCEM','ARGONDENIM','ASIAINS','ASIAPACINS','ASIATICLAB','ATLASBANG','AZIZPIPES','BANGAS','BANKASIA','BARKAPOWER','BATASHOE','BATBC','BAYLEASING','BBS','BBSCABLES','BDAUTOCA','BDCOM','BDFINANCE','BDLAMPS','BDSERVICE','BDTHAI','BDTHAIFOOD','BDWELDING','BEACHHATCH','BEACONPHAR','BENGALWTL','BERGERPBL','BESTHLDNG','BEXIMCO','BGIC','BIFC','BNICL','BPML','BPPL','BRACBANK','BSC','BSCPLC','BSRMLTD','BSRMSTEEL','BXPHARMA','CAPITECGBF','CAPMBDBLMF','CAPMIBBLMF','CENTRALINS','CENTRALPHL','CITYBANK','CITYGENINS','CLICL','CNATEX','CONFIDCEM','CONTININS','COPPERTECH','CROWNCEMNT','CRYSTALINS','CVOPRL','DACCADYE','DAFODILCOM','DBH','DBH1STMF','DELTALIFE','DELTASPINN','DESCO','DESHBANDHU','DGIC','DHAKABANK','DHAKAINS','DOMINAGE','DOREENPWR','DSHGARME','DSSL','DULAMIACOT','DUTCHBANGL','EASTERNINS','EASTLAND','EASTRNLUB','EBL','EBL1STMF','EBLNRBMF','ECABLES','EGEN','EHL','EIL','EMERALDOIL','ENVOYTEX','EPGL','ESQUIRENIT','ETL','EXIM1STMF','EXIMBANK','FAMILYTEX','FARCHEM','FAREASTFIN','FAREASTLIF','FASFIN','FBFIF','FEDERALINS','FEKDIL','FINEFOODS','FIRSTFIN','FIRSTSBANK','FORTUNE','FUWANGCER','FUWANGFOOD','GBBPOWER','GEMINISEA','GENEXIL','GENNEXT','GHAIL','GHCL','GIB','GLDNJMF','GLOBALINS','GOLDENSON','GP','GPHISPAT','GQBALLPEN','GRAMEENS2','GREENDELMF','GREENDELT','GSPFINANCE','HAKKANIPUL','HAMI','HEIDELBCEM','HFL','HRTEX','HWAWELLTEX','IBNSINA','IBP','ICB','ICB3RDNRB','ICBAGRANI1','ICBAMCL2ND','ICBEPMF1S1','ICBIBANK','ICBSONALI1','ICICL','IDLC','IFADAUTOS','IFIC','IFIC1STMF','IFILISLMF1','ILFSL','INDEXAGRO','INTECH','INTRACO','IPDC','ISLAMIBANK','ISLAMICFIN','ISLAMIINS','ISNLTD','ITC','JAMUNABANK','JAMUNAOIL','JANATAINS','JHRML','JMISMDL','JUTESPINN','KARNAPHULI','KAY&QUE','KBPPWBIL','KDSALTD','KEYACOSMET','KOHINOOR','KPCL','KPPL','KTL','LANKABAFIN','LEGACYFOOT','LHB','LIBRAINFU','LINDEBD','LOVELLO','LRBDL','LRGLOBMF1','MAGURAPLEX','MAKSONSPIN','MALEKSPIN','MARICO','MATINSPINN','MBL1STMF','MEGCONMILK','MEGHNACEM','MEGHNAINS','MEGHNALIFE','MEGHNAPET','MERCANBANK','MERCINS','METROSPIN','MHSML','MIDASFIN','MIDLANDBNK','MIRACLEIND','MIRAKHTER','MITHUNKNIT','MJLBD','MLDYEING','MONNOAGML','MONNOCERA','MONNOFABR','MONOSPOOL','MPETROLEUM','MTB','NAHEEACP','NATLIFEINS','NAVANACNG','NAVANAPHAR','NBL','NCCBANK','NCCBLMF1','NEWLINE','NFML','NHFIL','NITOLINS','NORTHERN','NORTHRNINS','NPOLYMER','NRBBANK','NRBCBANK','NTC','NTLTUBES','NURANI','OAL','OIMEX','OLYMPIC','ONEBANKPLC','ORIONINFU','ORIONPHARM','PADMALIFE','PADMAOIL','PARAMOUNT','PDL','PENINSULA','PEOPLESINS','PF1STMF','PHARMAID','PHENIXINS','PHOENIXFIN','PHPMF1','PIONEERINS','PLFSL','POPULAR1MF','POPULARLIF','POWERGRID','PRAGATIINS','PRAGATILIF','PREMIERBAN','PREMIERCEM','PREMIERLEA','PRIME1ICBA','PRIMEBANK','PRIMEFIN','PRIMEINSUR','PRIMELIFE','PRIMETEX','PROGRESLIF','PROVATIINS','PTL','PUBALIBANK','PURABIGEN','QUASEMIND','QUEENSOUTH','RAHIMAFOOD','RAHIMTEXT','RAKCERAMIC','RANFOUNDRY','RDFOOD','RECKITTBEN','REGENTTEX','RELIANCE1','RELIANCINS','RENATA','RENWICKJA','REPUBLIC','RINGSHINE','ROBI','RSRMSTEEL','RUNNERAUTO','RUPALIBANK','RUPALIINS','RUPALILIFE','SAFKOSPINN','SAIFPOWER','SAIHAMCOT','SAIHAMTEX','SALAMCRST','SALVO','SAMATALETH','SAMORITA','SANDHANINS','SAPORTL','SAVAREFR','SBACBANK','SEAPEARL','SEMLFBSLGF','SEMLIBBLSF','SHAHJABANK','SHARPIND','SHASHADNIM','SHEPHERD','SHURWID','SHYAMPSUG','SIBL','SICL','SILCOPHL','SILVAPHL','SIMTEX','SINGERBD','SINOBANGLA','SIPLC','SKTRIMS','SONALIANSH','SONALILIFE','SONALIPAPR','SONARBAINS','SONARGAON','SOUTHEASTB','SPCERAMICS','SPCL','SQUARETEXT','SQURPHARMA','SSSTEEL','STANCERAM','STANDARINS','STANDBANKL','STYLECRAFT','SUMITPOWER','SUNLIFEINS','TAKAFULINS','TALLUSPIN','TAMIJTEX','TECHNODRUG','TILIL','TITASGAS','TOSRIFA','TRUSTB1MF','TRUSTBANK','TUNGHAI','UCB','UNILEVERCL','UNIONBANK','UNIONCAP','UNIONINS','UNIQUEHRL','UNITEDFIN','UNITEDINS','UPGDCL','USMANIAGL','UTTARABANK','UTTARAFIN','VAMLBDMF1','VAMLRBBF','VFSTDL','WALTONHIL','WATACHEM','WMSHIPYARD','YPL','ZAHEENSPIN','ZAHINTEX','ZEALBANGLA']

def get_dse_stock_list(force_refresh=False):
    try:
        if force_refresh: _LIVE_CACHE.refresh_prices()
        syms = _LIVE_CACHE.get_symbols()
        if syms and len(syms) >= 50:
            filtered = [s for s in syms if s and not s.isdigit() and s not in ("DSEX","DSES","DS30","DGEN")]
            if len(filtered) >= 50:
                print(f"✅ Live API returned {len(filtered)} symbols")
                return filtered
        print(f"⚠️ Live API returned only {len(syms) if syms else 0} symbols")
    except Exception as e: print(f"⚠️ Live API symbol fetch failed: {e}")
    print("⚠️ Using fallback symbol list")
    return list(_FALLBACK_SYMBOLS)

# ==================== COMPANY INFO ====================
_COMPANY_INFO_CACHE = {}; _COMPANY_INFO_LOCK = threading.Lock()
def parse_company_page(symbol):
    sym = symbol.upper().strip()
    with _COMPANY_INFO_LOCK:
        if sym in _COMPANY_INFO_CACHE: return _COMPANY_INFO_CACHE[sym]
    info = {}
    if _BDSHARE_COMPANY_INFO:
        try:
            res = _BDSHARE_COMPANY_INFO(sym)
            if isinstance(res, pd.DataFrame) and not res.empty:
                r = res.iloc[0]
                for key, aliases in {"market_cap":["market_cap","market capitalization","mktcap"],"sector":["sector","industry"],"pe_ratio":["pe_ratio","p/e","pe"],"ltp":["ltp","last trading price"],"volume":["volume","total volume"]}.items():
                    for a in aliases:
                        for c in res.columns:
                            if a.lower() in str(c).lower():
                                v = r[c]
                                if key in ("market_cap","pe_ratio","ltp","volume"):
                                    try: info[key] = float(str(v).replace(",", ""))
                                    except Exception: pass
                                else: info[key] = str(v)
                                break
                        if key in info: break
            elif isinstance(res, dict): info.update(res)
        except Exception: pass
    with _COMPANY_INFO_LOCK: _COMPANY_INFO_CACHE[sym] = info
    return info or None

# ==================== SELF-HEALING CACHE (v1.31) ====================
def _append_live_snapshot_to_cache(symbol, payload):
    if not payload: return
    try:
        price = float(payload.get("close") or payload.get("ltp") or 0)
        if price <= 0: return
        row = {"date": pd.to_datetime(payload.get("date") or dt.date.today()),
               "open": float(payload.get("open") or price), "high": float(payload.get("high") or price),
               "low": float(payload.get("low") or price), "close": price,
               "volume": int(payload.get("volume") or 0)}
        cache_file = f"{CACHE_DIR}/{symbol.upper()}_archive.csv"
        if os.path.exists(cache_file):
            df = pd.read_csv(cache_file)
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            # v1.31: don't overwrite an authoritative EOD row that already has volume
            existing = df[df["date"].dt.date == row["date"].date()]
            if not existing.empty and int(existing.iloc[0].get("volume", 0) or 0) > 0:
                return
            df = df[df["date"].dt.date != row["date"].date()]
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        else: df = pd.DataFrame([row])
        df.sort_values("date").reset_index(drop=True).to_csv(cache_file, index=False)
    except Exception: pass

# ==================== REALTIME DATA ====================
def get_realtime_data(symbol):
    symbol = symbol.upper().strip()
    payload = _LIVE_CACHE.get_symbol(symbol)
    if payload and payload.get("ltp", 0) > 0:
        company = parse_company_page(symbol) or {}
        vol = float(payload.get("volume") or 0)
        if vol == 0 and company.get("volume"): vol = float(company.get("volume") or 0)
        try: write_data_health_cache(HEALTH_CACHE_PATH, symbol, datetime.now(timezone.utc), source="live")
        except Exception: pass
        try: _append_live_snapshot_to_cache(symbol, {"date": payload.get("date"), "open": payload.get("open"), "high": payload.get("high"), "low": payload.get("low"), "close": payload.get("close") or payload.get("ltp"), "volume": vol})
        except Exception: pass
        return {"date": payload.get("date", dt.date.today().strftime("%Y-%m-%d")), "open": float(payload.get("open") or 0) or float(payload.get("ltp") or 0), "high": float(payload.get("high") or 0) or float(payload.get("ltp") or 0), "low": float(payload.get("low") or 0) or float(payload.get("ltp") or 0), "close": float(payload.get("close") or 0) or float(payload.get("ltp") or 0), "volume": vol, "change_percent": float(payload.get("change_pct") or 0), "market_cap": float(company.get("market_cap", 0) or 0), "sector": str(company.get("sector", payload.get("sector", "")) or ""), "pe_ratio": company.get("pe_ratio"), "source": "live"}
    local_df = load_local_data(symbol)
    if local_df is not None and not local_df.empty:
        last = local_df.iloc[-1]
        return {"date": last['date'].strftime('%Y-%m-%d'), "open": float(last['open']), "high": float(last['high']), "low": float(last['low']), "close": float(last['close']), "volume": int(last['volume']), "change_percent": 0, "market_cap": 0, "sector": "", "pe_ratio": None, "source": "local"}
    return None

# ==================== LOCAL DATA LOADER ====================
def load_local_data(symbol):
    patterns = [f"{DATA_DIR}/{symbol}.csv", f"{DATA_DIR}/{symbol}_analysis.csv", f"{CACHE_DIR}/{symbol}_archive.csv", f"{DATA_DIR}/_{symbol}.csv"]
    for pat in patterns:
        files = glob.glob(pat)
        if files:
            latest = max(files, key=os.path.getmtime)
            try:
                df = pd.read_csv(latest)
                if 'date' in df.columns: df['date'] = pd.to_datetime(df['date'])
                elif 'Date' in df.columns: df['date'] = pd.to_datetime(df['Date'])
                else:
                    for c in df.columns:
                        if 'date' in c.lower(): df['date'] = pd.to_datetime(df[c]); break
                for col in ['open','high','low','close','volume']:
                    if col not in df.columns:
                        for c in df.columns:
                            if col.lower() in c.lower(): df[col] = df[c]; break
                if 'date' in df.columns and 'close' in df.columns:
                    df = df.sort_values('date').reset_index(drop=True)
                    df = df[df['date'].notna()]
                    if len(df) >= 20: return df
            except Exception: pass
    return None

# ==================== HISTORY ORCHESTRATOR ====================
def get_historical_data(symbol, realtime):
    symbol = symbol.upper().strip()
    local = load_local_data(symbol)
    if local is not None and len(local) >= 20:
        try:
            db = MarketDatabase(); db.save_stock_data(symbol, local); db.close()
        except Exception: pass
        return local
    end, start = dt.date.today(), dt.date.today() - timedelta(days=2*365)
    try: df = _HIST_COLLECTOR.fetch(symbol, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    except Exception: df = None
    if df is not None and len(df) >= 20:
        df = df.copy(); df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date']); df = df[df['date'].dt.year > 1990]
        for col in ['open','high','low','close','volume']:
            if col not in df.columns: df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        df = df[df['close'] > 0]
        if len(df) >= 20:
            df = df[['date','open','high','low','close','volume']].sort_values('date').reset_index(drop=True)
            try: df.to_csv(f"{CACHE_DIR}/{symbol}_archive.csv", index=False)
            except Exception: pass
            try:
                db = MarketDatabase(); db.save_stock_data(symbol, df); db.close()
            except Exception: pass
            return df
    return None

def seed_all_historical_cache():
    symbols = get_dse_stock_list(force_refresh=True); total = len(symbols)
    print(f"\n📦 Seeding historical cache for {total} symbols...")
    cached = failed = already = 0
    end = dt.date.today(); start = end - timedelta(days=2*365)
    start_str, end_str = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    for i, sym in enumerate(symbols, 1):
        cache_file = f"{CACHE_DIR}/{sym}_archive.csv"
        if os.path.exists(cache_file):
            try:
                if len(pd.read_csv(cache_file)) >= 20: already += 1; print(f"{sym:12s} ({i:3d}/{total}) ✓ cached"); continue
            except Exception: pass
        try:
            df = _HIST_COLLECTOR.fetch(sym, start_str, end_str)
            if df is not None and len(df) >= 20:
                df.to_csv(cache_file, index=False); cached += 1; print(f"{sym:12s} ({i:3d}/{total}) ✅ {len(df)} rows")
            else: failed += 1; print(f"{sym:12s} ({i:3d}/{total}) ⚠️ no data")
        except Exception as e: failed += 1; print(f"{sym:12s} ({i:3d}/{total}) ❌ {str(e)[:40]}")
        time.sleep(0.4)
    print(f"\n📦 Seeding complete — cached: {cached}, already: {already}, failed: {failed}")

# ==================== FAILURE CLASSIFIER ====================
def classify_failure(symbol, exception=None):
    sym = symbol.upper().strip()
    live = _LIVE_CACHE.get_symbol(sym)
    if live is None: return {"reason": "SYMBOL_NOT_FOUND", "details": "Not present in /api/live/prices"}
    if float(live.get("ltp") or 0) <= 0: return {"reason": "ZERO_PRICE", "details": f"ltp={live.get('ltp')}"}
    hist = load_local_data(sym)
    if hist is None: return {"reason": "NO_HISTORY_LOCAL", "details": "No cached CSV"}
    if len(hist) < 20: return {"reason": "INSUFFICIENT_HISTORY", "details": f"Only {len(hist)} rows"}
    if exception is not None: return {"reason": "ANALYSIS_ERROR", "details": f"{type(exception).__name__}: {str(exception)[:120]}"}
    return {"reason": "UNKNOWN_ERROR", "details": "Analysis returned None"}

# ==================== INDICATORS ====================
def calculate_rsi(data, period=14):
    if len(data) < period + 1: return 50.0
    delta = data.diff(); gain = delta.where(delta > 0, 0); loss = -delta.where(delta < 0, 0)
    ag = gain.ewm(alpha=1/period, adjust=False).mean(); al = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = ag / (al + 1e-9); rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not rsi.empty else 50.0

def calculate_adx(df, period=14):
    if len(df) < period: return 0.0
    high, low, close = df['high'], df['low'], df['close']
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(period, min_periods=1).mean()
    pdm, mdm = high.diff(), low.diff(); pdm[pdm < 0] = 0; mdm[mdm > 0] = 0
    mdm = mdm.abs(); pdm[pdm < mdm] = 0
    pdi = 100 * (pdm.rolling(period, min_periods=1).mean() / (atr + 1e-9))
    mdi = 100 * (mdm.rolling(period, min_periods=1).mean() / (atr + 1e-9))
    dx = (pdi - mdi).abs() / (pdi + mdi + 1e-9) * 100
    adx = dx.rolling(period, min_periods=1).mean()
    return float(adx.iloc[-1]) if not adx.empty else 0.0

def calculate_atr(df, period=14):
    if len(df) < period: return float(df['close'].mean() * 0.02) if not df.empty else 1.0
    tr = np.maximum(df['high'] - df['low'], np.maximum((df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()))
    atr = tr.rolling(period).mean()
    return float(atr.iloc[-1]) if not atr.empty else float(df['close'].mean() * 0.02)

def calculate_macd(data, fast=12, slow=26, signal=9):
    ef = data.ewm(span=fast, adjust=False).mean(); es = data.ewm(span=slow, adjust=False).mean()
    line = ef - es; sig = line.ewm(span=signal, adjust=False).mean()
    return line, sig, line - sig

# ==================== HIKE-TIER ====================
def compute_extension_from_sma20(df, price):
    try:
        if df is None or len(df) < 20 or price is None or price <= 0: return 0.0, float(price or 0.0)
        sma20 = float(df['close'].rolling(20).mean().iloc[-1])
        if not sma20 or sma20 <= 0: return 0.0, float(price)
        return float((float(price) / sma20 - 1.0) * 100.0), sma20
    except Exception: return 0.0, float(price or 0.0)

def compute_volume_divergence_days(df, lookback=VOL_DIVERGENCE_LOOKBACK):
    try:
        if df is None or len(df) < 3: return 0, 0
        n = min(lookback, len(df) - 1); max_run = cur_run = total = 0
        for i in range(len(df) - n, len(df)):
            c, p = df.iloc[i], df.iloc[i - 1]
            if float(c['close']) > float(p['close']) and float(c['volume']) < float(p['volume']):
                cur_run += 1; total += 1
                if cur_run > max_run: max_run = cur_run
            else: cur_run = 0
        return int(max_run), int(total)
    except Exception: return 0, 0

@dataclass
class HikeClassification:
    tier: str; extension_pct: float; volume_divergence_days: int; volume_divergence_total: int; rsi: float

def classify_tier(extension_pct, vol_div_days, rsi=50.0):
    vd, ext, rsi_v = int(vol_div_days or 0), float(extension_pct or 0.0), float(rsi if rsi is not None else 50.0)
    if vd >= VOL_DIVERGENCE_OVERRIDE or (vd >= VOL_DIVERGENCE_SOFT and rsi_v > VOL_DIVERGENCE_RSI_HOT): tier = TIER_EXHAUSTION
    else:
        if ext < EXTENSION_BAND_NONE: tier = TIER_NONE
        elif ext < EXTENSION_BAND_EARLY: tier = TIER_EARLY
        elif ext < EXTENSION_BAND_MID: tier = TIER_MID
        elif ext < EXTENSION_BAND_LATE: tier = TIER_LATE
        else: tier = TIER_EXHAUSTION
    return HikeClassification(tier=tier, extension_pct=ext, volume_divergence_days=vd, volume_divergence_total=0, rsi=rsi_v)

def tier_entry_allowed(tier): return tier in (TIER_EARLY, TIER_MID)

# ==================== WYCKOFF / RADAR / MCMC ====================
def get_volume_profile(df, bins=None):
    if len(df) < 20: return {'vpoc': None, 'vah': None, 'val': None}
    pmin, pmax = df['low'].min(), df['high'].max()
    if bins is None: bins = min(20, max(5, int((pmax - pmin) / 0.5)))
    if pmax <= pmin: return {'vpoc': None, 'vah': None, 'val': None}
    width = (pmax - pmin) / bins; vbp = {}
    for _, row in df.iterrows():
        low, high, vol = row['low'], row['high'], row['volume']
        if low == high: continue
        for i in range(bins):
            bl = pmin + i * width; bh = bl + width
            if high >= bl and low <= bh:
                ov = min(high, bh) - max(low, bl)
                if ov > 0: vbp[i] = vbp.get(i, 0) + vol * (ov / (high - low))
    if not vbp: return {'vpoc': None, 'vah': None, 'val': None}
    tot = sum(vbp.values()); vpoc_idx = max(vbp, key=vbp.get); vpoc = pmin + vpoc_idx * width + width / 2
    sb = sorted(vbp.items(), key=lambda x: x[1], reverse=True); cum = 0; vb = []
    for idx, v in sb:
        vb.append(idx); cum += v
        if cum / tot >= 0.70: break
    return {'vpoc': vpoc, 'val': pmin + min(vb) * width, 'vah': pmin + (max(vb) + 1) * width}

def detect_wyckoff_smc(df, vp=None):
    if len(df) < 30: return {'phase': 'Unknown', 'event': 'None', 'bias': 'Neutral'}
    sma50 = df['close'].rolling(50).mean().iloc[-1] if len(df) >= 50 else df['close'].iloc[-1]
    sma200 = df['close'].rolling(200).mean().iloc[-1] if len(df) >= 200 else df['close'].iloc[-1]
    close, high, low = df['close'].iloc[-1], df['high'].iloc[-1], df['low'].iloc[-1]
    rh, rl = df['high'].tail(20).max(), df['low'].tail(20).min()
    rp = (rh - rl) / close * 100 if close > 0 else 0
    vpoc = vp.get('vpoc') if vp else None; vah = vp.get('vah') if vp else None; val = vp.get('val') if vp else None
    if vpoc is None: vpoc = df['close'].rolling(20).mean().iloc[-1]; vah = df['high'].tail(20).max() * 0.98; val = df['low'].tail(20).min() * 1.02
    ab50, ab200 = close > sma50, close > sma200
    inv = val <= close <= vah if val and vah else False
    v20, v50 = df['volume'].tail(20).mean(), df['volume'].tail(50).mean() if len(df) >= 50 else df['volume'].tail(20).mean()
    vdec = (v20 / v50 < 1.0) if v50 > 0 else False
    lows = df['low'].tail(10).values; hl = all(lows[i] < lows[i+1] for i in range(len(lows)-1)) if len(lows) > 1 else False
    highs = df['high'].tail(10).values; lh = all(highs[i] > highs[i+1] for i in range(len(highs)-1)) if len(highs) > 1 else False
    av = df['volume'].tail(20).mean(); vs = df['volume'].iloc[-1] / av if av > 0 else 1
    spring = (low < rl * 1.015 and close > low and close > df['low'].tail(10).min() and vs > 1.2)
    utad = (high > rh * 0.985 and close < high and close < df['high'].tail(10).max() and vs > 1.2)
    if not ab200 and close < sma50 and rp < 12 and vdec: phase, bias = 'Accumulation', 'Bullish'
    elif not ab200 and close < sma50 and hl and inv: phase, bias = 'Accumulation', 'Bullish'
    elif ab200 and close > sma50 and rp < 12 and vdec: phase, bias = 'Distribution', 'Bearish'
    elif ab200 and close > sma50 and lh and inv: phase, bias = 'Distribution', 'Bearish'
    elif ab200 and close > sma50 and not lh and close > df['close'].tail(5).max(): phase, bias = 'Markup', 'Bullish'
    elif not ab200 and close < sma50 and not hl and close < df['close'].tail(5).min(): phase, bias = 'Markdown', 'Bearish'
    else: phase, bias = 'Neutral', 'Neutral'
    event = 'None'
    if spring and phase in ['Accumulation', 'Neutral']: event = 'Spring'
    elif utad and phase in ['Distribution', 'Neutral']: event = 'UTAD'
    elif phase == 'Markup' and close > df['close'].tail(5).max(): event = 'BOS (Up)'
    elif phase == 'Markdown' and close < df['close'].tail(5).min(): event = 'BOS (Down)'
    return {'phase': phase, 'event': event, 'bias': bias, 'vol_spike': vs}

def detect_order_flow_signal(df):
    if len(df) < 5: return {'absorption': False, 'initiative': False}
    last = df.iloc[-1]; av = df['volume'].tail(10).mean(); ar = (df['high'] - df['low']).tail(10).mean()
    abs_ = (last['volume'] > av * 1.5 and (last['high'] - last['low']) < ar * 0.6)
    sa = abs_ and last['close'] > last['open']; ba = abs_ and last['close'] < last['open']
    ini = (last['volume'] > av * 1.3 and (last['high'] - last['low']) > ar * 1.2)
    bi = ini and last['close'] > (last['high'] - (last['high'] - last['low']) * 0.33)
    si = ini and last['close'] < (last['low'] + (last['high'] - last['low']) * 0.33)
    return {'absorption': abs_, 'sell_absorption': sa, 'buy_absorption': ba, 'initiative': ini, 'buy_initiative': bi, 'sell_initiative': si}

def evaluate_signal(df, realtime, vp, phase, oflow):
    price = realtime.get('close', df['close'].iloc[-1])
    vpoc, vah, val = vp.get('vpoc', price), vp.get('vah', price*1.05), vp.get('val', price*0.95)
    if phase['phase'] == 'Distribution' and phase['event'] == 'UTAD':
        return {'signal': 'SELL', 'bias': 'Bearish', 'confidence': 75, 'buy_score': 0, 'sell_score': 10, 'buy_reasons': [], 'sell_reasons': ['Distribution + UTAD'], 'phase': phase['phase'], 'event': phase['event']}
    bs = ss = 0; br = []; sr = []
    if phase['phase'] in ['Accumulation', 'Markup']: bs += 2; br.append('Wyckoff Bullish (+2)')
    if price <= val * 1.02 or price < vpoc: bs += 1; br.append('Value Zone (+1)')
    if phase['event'] == 'Spring': bs += 3; br.append('Spring (+3)')
    elif phase['event'] == 'BOS (Up)': bs += 2; br.append('BOS Up (+2)')
    if oflow.get('sell_absorption') or oflow.get('buy_initiative'): bs += 2; br.append('Bull Flow (+2)')
    last = df.iloc[-1]
    if last['close'] > last['open'] and (last['close']-last['open']) > (last['high']-last['low'])*0.5: bs += 1; br.append('Bull Candle (+1)')
    if phase['phase'] in ['Distribution', 'Markdown']: ss += 2; sr.append('Wyckoff Bearish (+2)')
    if price >= vah * 0.98 or price > vpoc: ss += 1; sr.append('Supply Zone (+1)')
    if phase['event'] == 'UTAD': ss += 4; sr.append('UTAD (+4)')
    elif phase['event'] == 'BOS (Down)': ss += 2; sr.append('BOS Down (+2)')
    if oflow.get('buy_absorption') or oflow.get('sell_initiative'): ss += 2; sr.append('Bear Flow (+2)')
    if last['close'] < last['open'] and (last['open']-last['close']) > (last['high']-last['low'])*0.5: ss += 1; sr.append('Bear Candle (+1)')
    if bs >= 5 and bs > ss: sig, bias, conf = 'BUY', 'Bullish', min(85, 50 + bs*5)
    elif ss >= 5 and ss > bs: sig, bias, conf = 'SELL', 'Bearish', min(85, 50 + ss*5)
    elif bs >= 3 and ss <= 3: sig, bias, conf = 'BUY (Watch)', 'Bullish', 45 + bs*5
    elif ss >= 3 and bs <= 3: sig, bias, conf = 'SELL (Watch)', 'Bearish', 45 + ss*5
    else: sig, bias, conf = 'WAIT', 'Neutral', 30
    return {'signal': sig, 'bias': bias, 'confidence': conf, 'buy_score': bs, 'sell_score': ss, 'buy_reasons': br, 'sell_reasons': sr, 'phase': phase['phase'], 'event': phase['event']}

def get_dynamic_stop(df, price, is_long=True):
    atr = calculate_atr(df)
    if is_long: return max(max(price - atr*2.0, df['low'].tail(5).min()*0.98 if len(df) >= 5 else price*0.95), price*0.95)
    return min(min(price + atr*2.0, df['high'].tail(5).max()*1.02 if len(df) >= 5 else price*1.05), price*1.05)

def compute_radar_score(df, realtime, rsi, adx, macd_hist):
    if df is None or len(df) < 60: return 0, '', 0.0
    score = 0; flags = []
    close = float(df['close'].iloc[-1]); vol = float(realtime.get('volume', 0) or 0); turnover = close * vol
    v20 = float(df['volume'].tail(20).mean()) if len(df) >= 20 else 0.0
    v50 = float(df['volume'].tail(50).mean()) if len(df) >= 50 else v20
    h20 = float(df['high'].tail(20).max()) if len(df) >= 20 else close
    ma20 = df['close'].rolling(20).mean(); sd20 = df['close'].rolling(20).std()
    bb = float((sd20.iloc[-1]*4) / ma20.iloc[-1]) if ma20.iloc[-1] else 0.0
    if bb < RADAR_SQUEEZE_BB: score += 1; flags.append('BB_SQUEEZE')
    if v50 > 0 and v20 < RADAR_DRYUP_FACTOR * v50: score += 1; flags.append('VOL_DRYUP')
    if h20 > 0 and close >= RADAR_NEAR_HIGH_FACTOR * h20: score += 1; flags.append('NEAR_HIGH')
    if RADAR_RSI_LOW <= rsi <= RADAR_RSI_HIGH: score += 1; flags.append('RSI_ZONE')
    if len(df) > 25 and adx > calculate_adx(df.iloc[:-5]): score += 1; flags.append('ADX_RISING')
    if macd_hist is not None and len(macd_hist) > 5 and macd_hist.iloc[-1] > macd_hist.iloc[-5]: score += 1; flags.append('MACD_TURN')
    obv = (np.sign(df['close'].diff()).fillna(0) * df['volume']).cumsum()
    if len(obv) > 10 and obv.iloc[-1] > obv.iloc[-10]: score += 1; flags.append('OBV_RISING')
    if ma20.iloc[-1] and close > ma20.iloc[-1]: score += 1; flags.append('ABOVE_MA20')
    if v50 > 0 and vol > RADAR_VOL_IGNITION_MULT * v50 and turnover >= RADAR_MIN_TURNOVER: score += 2; flags.append('VOL_IGNITION')
    if turnover >= RADAR_MIN_TURNOVER: score += 1; flags.append('LIQ_OK')
    return score, ','.join(flags), turnover

def compute_mcmc(symbol, df, current_price, dsex=None):
    invalid = {'quality': 'INVALID', 'mu_hat': 0.0, 'mu_hat_raw': 0.0, 'sigma_adapt': 0.02, 'buy_zone': current_price*0.98, 'target': current_price*1.02, 'stop': current_price*0.95, 't_stat': 0.0, 'avg_turnover': 0.0, 'n_eff': 0, 'signal': 'WAIT', 'P_mu': 0.5}
    vd = df[df['volume'] > 0].copy()
    if len(vd) < 20: return invalid
    vd['r'] = np.log(vd['close'] / vd['open']); rs = vd['r'].dropna(); N = len(rs)
    if N < 5: return invalid
    if dsex is not None and len(dsex) >= 252:
        dr = np.log(dsex / dsex.shift(1)).dropna()
        mu_0, s0sq = (dr.iloc[-252:].mean(), dr.iloc[-252:].var()) if len(dr) >= 252 else (0.0, 0.01)
    else: mu_0, s0sq = 0.0, 0.01
    Nw = min(60, max(20, N)); rw = rs.iloc[-Nw:] if N >= Nw else rs; neff = len(rw)
    smsq = rw.var() if neff > 1 else 0.01; den = s0sq + (neff * smsq)
    mu_hat_raw = ((s0sq * mu_0) + (smsq * rw.sum())) / den if den != 0 else 0.0
    mu_hat = (1.0 - PRIOR_SHRINK) * mu_hat_raw + PRIOR_SHRINK * mu_0
    atr14 = calculate_atr(df, 14); atr_ratio = atr14 / current_price if atr14 and current_price > 0 else 0.02
    sig_adapt = max(0.01, atr_ratio * MCMC_SIGMA_PAD)
    buy_zone = current_price * np.exp(mu_hat + Z_BUY * sig_adapt)
    target = current_price * np.exp(mu_hat + Z_SELL * sig_adapt)
    stop = current_price * np.exp(mu_hat + Z_STOP * sig_adapt)
    t_stat = mu_hat / (sig_adapt / np.sqrt(neff)) if sig_adapt > 0 else 0.0
    avg_turn = (df['close'] * df['volume']).tail(20).mean()
    quality = 'VALID' if avg_turn > LIQUIDITY_THRESHOLD else 'INVALID'
    signal = 'WAIT'
    if quality == 'VALID':
        ma20 = float(df['close'].rolling(20).mean().iloc[-1]) if len(df) >= 20 else current_price
        if current_price <= buy_zone * 1.03 and mu_hat > 0 and t_stat > 1.40: signal = 'BUY'
        elif mu_hat > 0 and t_stat > 1.96 and current_price > ma20: signal = 'BUY'
        elif current_price >= target * 0.97 and mu_hat < 0 and t_stat < -1.40: signal = 'SELL'
        elif mu_hat < 0 and t_stat < -1.96 and current_price < ma20: signal = 'SELL'
    try: p_up = float(1.0 / (1.0 + math.exp(-1.4 * t_stat)))
    except OverflowError: p_up = 1.0 if t_stat > 0 else 0.0
    return {'quality': quality, 'mu_hat': mu_hat, 'mu_hat_raw': mu_hat_raw, 'sigma_adapt': sig_adapt, 'buy_zone': buy_zone, 'target': target, 'stop': stop, 't_stat': t_stat, 'signal': signal, 'avg_turnover': avg_turn, 'n_eff': neff, 'P_mu': p_up}

def compute_forecast_1w(price, mcmc_mu, mcmc_t_stat, mcmc_sigma, volume_conf, verdict, weinstein_stage2, regime='Sideways', avg_turnover=0.0, a_mqs=0.0, use_simplified=False):
    th = get_forecast_thresholds(regime); T_TRADE, BAND_TRADE, T_WATCH, BAND_WATCH = th['T_TRADE'], th['BAND_TRADE'], th['T_WATCH'], th['BAND_WATCH']
    BS = BAND_WATCH * BAND_SOFT_WATCH_FACTOR; t_sq = mcmc_t_stat ** 2; denom = t_sq + SHRINK_K
    mu_shrunk = (mcmc_mu * t_sq / denom) if denom > 0 else 0.0; r_mid = TRADING_DAYS_1W * mu_shrunk
    sigma_1w = mcmc_sigma * math.sqrt(TRADING_DAYS_1W); band_80 = Z_80 * sigma_1w
    if use_simplified or abs(mcmc_t_stat) < 1.8:
        P_mid = price; P_low = price * (1.0 - Z_80*mcmc_sigma*math.sqrt(TRADING_DAYS_1W)); P_high = price * (1.0 + Z_80*mcmc_sigma*math.sqrt(TRADING_DAYS_1W))
        bw = ((P_high - P_low)/price*100.0) if price > 0 else 0.0; r_eff = 0.0; ver = 'SIMPLIFIED'
    else:
        rl, rh = r_mid - band_80, r_mid + band_80
        P_low, P_mid, P_high = price*(1+rl), price*(1+r_mid), price*(1+rh)
        bw = ((P_high - P_low)/price*100.0) if price > 0 else 0.0; r_eff = r_mid; ver = 'DRIFT'
    sig, reason = 'IGNORE', 'Conditions not met'
    if volume_conf > VOLUME_CONF_CORRUPT: sig, reason = 'REJECT', f'Vol {volume_conf:.1f}x > {VOLUME_CONF_CORRUPT:.0f}'
    elif mcmc_sigma > MCMC_SIGMA_MAX: sig, reason = 'REJECT', f'σ {mcmc_sigma:.3f} > {MCMC_SIGMA_MAX}'
    elif (mcmc_t_stat >= T_TRADE and bw <= BAND_TRADE and verdict != 'RED' and weinstein_stage2 == 'YES' and avg_turnover >= LIQ_TRADE_TIER): sig, reason = 'TRADE', 'All TRADE conditions met'
    elif (mcmc_t_stat >= T_WATCH and bw <= BAND_WATCH and avg_turnover >= LIQ_WATCH_TIER):
        sig = 'WATCH'
        if verdict == 'RED': reason = 'Verdict=RED'
        elif weinstein_stage2 != 'YES': reason = 'Not Stage 2'
        elif bw > BAND_TRADE: reason = f'Band {bw:.1f}% > {BAND_TRADE:.1f}%'
        elif avg_turnover < LIQ_TRADE_TIER: reason = 'Turnover low'
        elif mcmc_t_stat < T_TRADE: reason = f't {mcmc_t_stat:.2f} < {T_TRADE}'
        else: reason = 'Conditions not met'
    elif (mcmc_t_stat >= T_WATCH and weinstein_stage2 == 'YES' and bw <= BS and avg_turnover >= LIQ_WATCH_TIER and a_mqs >= A_MQS_SOFT_WATCH): sig = 'WATCH'; reason = 'Soft WATCH (Stage2+aMQS)'
    else:
        sig = 'IGNORE'
        if mcmc_t_stat < T_WATCH: reason = f't {mcmc_t_stat:.2f} < {T_WATCH}'
        elif bw > BAND_WATCH: reason = f'Band {bw:.1f}% > {BAND_WATCH:.0f}%'
        elif verdict == 'RED': reason = 'Verdict RED'
    return {'mu_shrunk': mu_shrunk, 'r_mid': r_mid, 'r_mid_effective': r_eff, 'sigma_1w': sigma_1w, 'band_80': band_80, 'P_low': P_low, 'P_mid': P_mid, 'P_high': P_high, 'band_width_pct': bw, 'signal': sig, 'reason': reason, 'version': ver}

def check_hard_gate(adtv20, market_cap_known):
    adtv = float(adtv20 or 0.0)
    if adtv < ADTV_HARD_FAIL: return (False, f"Hard gate: ADTV < {ADTV_HARD_FAIL:,}", 0.0)
    if adtv < ADTV_SOFT_WARN: return (True, " ", 0.5)
    return (True, " ", 1.0)

def _default_fuzzy_v3(reason='INVALID'):
    return {'M':0.0,'P':0.0,'C':0.0,'R':0.0,'L':0.0,'C_raw':0.0,'S_gate':0.0,'S_rank':0.0,'FinalScore':0.0,'Action':'Avoid','ActionBn':'⛔ এড়িয়ে যান','Size':'Cash (0%)','Color':'avoid','Reasons':[f'❌ {reason}'],'turnover_velocity':0.0,'hard_gate_pass':False,'skip_reason':reason,'size_mult':0.0,'th_strong':FUZZY_V3_TH_STRONG_BASE,'th_mild':FUZZY_V3_TH_MILD_BASE,'th_watch':FUZZY_V3_TH_WATCH_BASE}

def compute_fuzzy_v3(df, mcmc, patterns, breadth_theta, regime_factor, price_vs_ema20, volume_ratio, close_position, rsi, adx, turnover, avg_vol_50, market_cap=0.0, free_float_cap=0.0, adtv20=None, extension_pct=0.0, vol_div_days=0, hike_tier=TIER_NONE):
    def _f(v, default=0.0):
        try:
            if v is None: return default
            f = float(v)
            return default if math.isnan(f) or math.isinf(f) else f
        except (TypeError, ValueError): return default
    def _clip(v, lo=0.0, hi=1.0):
        try: return max(lo, min(hi, float(v)))
        except Exception: return lo
    price_vs_ema20 = _f(price_vs_ema20, 0.0); volume_ratio = _f(volume_ratio, 1.0)
    close_position = _clip(_f(close_position, 0.5)); rsi = _clip(_f(rsi, 50.0), 0.0, 100.0); adx = _clip(_f(adx, 20.0), 0.0, 100.0)
    turnover = max(0.0, _f(turnover, 0.0)); avg_vol_50 = max(0.0, _f(avg_vol_50, 0.0))
    market_cap = max(0.0, _f(market_cap, 0.0)); free_float_cap = max(0.0, _f(free_float_cap, 0.0))
    adtv20 = _f(adtv20, 0.0); breadth_theta = _clip(_f(breadth_theta, 0.0), -1.0, 1.0); regime_factor = _clip(_f(regime_factor, 0.5), 0.20, 2.0)
    if not isinstance(mcmc, dict): mcmc = {}
    t_stat = _f(mcmc.get('t_stat'), 0.0); p_up = _clip(_f(mcmc.get('P_mu'), 0.5))
    if patterns is None or not isinstance(patterns, (list, tuple)): patterns = []
    boost = (1.0 - regime_factor) * FUZZY_V3_TH_REGIME_BOOST
    th_strong = FUZZY_V3_TH_STRONG_BASE + boost; th_mild = FUZZY_V3_TH_MILD_BASE + boost * 0.75; th_watch = FUZZY_V3_TH_WATCH_BASE + boost * 0.50
    hard_gate_pass, skip_reason, size_mult = check_hard_gate(adtv20, market_cap > 0)
    if not hard_gate_pass: return _default_fuzzy_v3(skip_reason)
    if hike_tier == TIER_EXHAUSTION:
        out = _default_fuzzy_v3(f"EXHAUSTION_WATCH (ext={extension_pct:.1f}% vol_div={vol_div_days}d)")
        out['hard_gate_pass'] = True; out['hike_tier'] = TIER_EXHAUSTION; out['extension_pct'] = round(float(extension_pct or 0.0), 3); out['vol_div_days'] = int(vol_div_days or 0)
        return out
    if market_cap >= MCAP_LARGE: tv_ref = FUZZY_V3_TV_LARGE
    elif market_cap >= MCAP_MID: tv_ref = FUZZY_V3_TV_MID
    else: tv_ref = FUZZY_V3_TV_SMALL
    data_missing = free_float_cap < 1_000_000.0
    if data_missing: L = 0.45; turnover_velocity = 0.0
    else: turnover_velocity = turnover / free_float_cap if free_float_cap > 0 else 0.0; L = _clip(turnover_velocity / tv_ref, 0.20, 1.0)
    m_price = _clip((price_vs_ema20 + 8.0) / 16.0); m_vol = _clip((volume_ratio - 0.5) / 2.0)
    m_close = close_position; m_rsi = _clip((rsi - 30.0) / 40.0); m_adx = _clip((adx - 15.0) / 25.0)
    M = _clip(0.30*m_price + 0.25*m_vol + 0.20*m_close + 0.15*m_rsi + 0.10*m_adx)
    bull = [p for p in patterns if isinstance(p, dict) and p.get('direction') == 'bullish']
    P = 0.0
    if any(p.get('tier') == 1 for p in bull): P = 0.90
    elif any(p.get('tier') == 2 for p in bull): P = 0.65
    elif bull: P = 0.40
    if close_position >= CLOSE_POS_MIN_LAUNCH and volume_ratio >= 1.5: P = max(P, 0.75)
    if -3.0 < price_vs_ema20 < 2.0 and close_position <= 0.40: P = max(P, 0.55)
    P = _clip(P)
    t_eff = max(t_stat, 0.0)
    try: C_raw = 1.0 / (1.0 + math.exp(-FUZZY_V3_C_STEEP * (t_eff - FUZZY_V3_C_CENTER)))
    except OverflowError: C_raw = 1.0
    C = _clip(0.60 * C_raw + 0.40 * p_up)
    R = _clip(0.40 + 0.40*breadth_theta + 0.20*regime_factor, 0.20, 1.0)
    L_eff = L if L >= FUZZY_V3_L_FLOOR else FUZZY_V3_L_FLOOR * (L / FUZZY_V3_L_FLOOR) ** 0.5
    product = max(0.0, M * C * R * L_eff); S_gate = _clip(product ** 0.5)
    S_rank = _clip(FUZZY_V3_W_M * M + FUZZY_V3_W_P * P + FUZZY_V3_W_C * C + FUZZY_V3_W_R * R)
    is_late = (hike_tier == TIER_LATE)
    if S_gate >= GATE_FLOOR_STRONG and S_rank >= th_strong and not is_late: action, size, color, action_bn = 'Strong Buy', 'Full (8-10%)', 'buy-strong', '🟢🟢 দৃঢ় ক্রয়'
    elif S_gate >= GATE_FLOOR_MILD and S_rank >= th_mild:
        if is_late and S_gate >= GATE_FLOOR_STRONG and S_rank >= th_strong: action, size, color, action_bn = 'Mild Buy', '~55% · LATE', 'buy-mild', '🟢 মৃদু ক্রয় (উচ্চ হাইক)'
        else: action, size, color, action_bn = 'Mild Buy', 'Half (4-5%)', 'buy-mild', '🟢 মৃদু ক্রয়'
    elif S_rank >= th_watch or S_gate >= GATE_FLOOR_WATCH: action, size, color, action_bn = 'Neutral / Watch', 'Cash / Small (0-2%)', 'neutral', '⏳ নিরপেক্ষ / পর্যবেক্ষণ'
    else: action, size, color, action_bn = 'Avoid', 'Cash (0%)', 'sell-strong', '🔴 এড়িয়ে যান'
    if size_mult == 0.5 and action in ('Strong Buy', 'Mild Buy'): size = size + ' · ADTV×0.5'
    reasons = []
    if M >= 0.70: reasons.append('📈 Strong momentum')
    if P >= 0.65: reasons.append('🕯️ Bullish pattern / price action')
    if C >= 0.60: reasons.append('📊 MCMC confidence OK')
    if R >= 0.70: reasons.append('🌊 Favourable regime + breadth')
    if L < 0.50: reasons.append('⚠️ Low relative liquidity')
    if data_missing: reasons.append('ℹ️ Free-float data missing – used fallback')
    if S_gate < GATE_FLOOR_MILD: reasons.append('⚠️ Gate failed')
    if size_mult == 0.5: reasons.append(f'⚠️ ADTV soft-warn – size ×0.5')
    if hike_tier == TIER_LATE: reasons.append(f'🟠 LATE_HIKE (ext={extension_pct:.1f}%) – size capped')
    elif hike_tier == TIER_MID: reasons.append(f'🟡 MID_HIKE (ext={extension_pct:.1f}%)')
    elif hike_tier == TIER_EARLY: reasons.append(f'🟢 EARLY_SETUP (ext={extension_pct:.1f}%)')
    if vol_div_days >= 1: reasons.append(f'📉 Volume divergence: {vol_div_days} day(s)')
    if not reasons: reasons.append('➖ Mixed / neutral conditions')
    return {'M': round(M, 4), 'P': round(P, 4), 'C': round(C, 4), 'R': round(R, 4), 'L': round(L, 4), 'C_raw': round(C_raw, 4), 'S_gate': round(S_gate, 4), 'S_rank': round(S_rank, 4), 'FinalScore': round(S_rank, 4), 'Action': action, 'ActionBn': action_bn, 'Size': size, 'Color': color, 'Reasons': reasons, 'turnover_velocity': round(turnover_velocity, 6), 'hard_gate_pass': True, 'skip_reason': '', 'size_mult': size_mult, 'th_strong': round(th_strong, 4), 'th_mild': round(th_mild, 4), 'th_watch': round(th_watch, 4), 'hike_tier': hike_tier, 'extension_pct': round(float(extension_pct or 0.0), 3), 'vol_div_days': int(vol_div_days or 0)}

def consensus_signal(gate_pass, data_health, final_score, canvas_signal, hike_tier=TIER_NONE, regime='Sideways'):
    data_stale = (data_health == "STALE")
    if hike_tier == TIER_EXHAUSTION: return "SELL" if canvas_signal in ("SELL", "BUY", "HOLD") else "WAIT"
    if not gate_pass: return "SELL" if canvas_signal == "SELL" else "WAIT"
    if data_stale and final_score < STALE_WAIT_BELOW: return "WAIT"
    if data_stale and final_score >= STALE_WAIT_BELOW: return "HOLD"
    th = BUY_THRESHOLD.get(regime, DEFAULT_BUY_THRESHOLD)
    if final_score >= th and hike_tier != TIER_LATE: return "BUY"
    if final_score >= SCORE_HOLD: return "HOLD"
    return canvas_signal

# ==================== CANDLE DETECTOR ====================
class CandleDetector:
    def _b(self, c): return abs(c['close'] - c['open'])
    def _u(self, c): return c['high'] - max(c['open'], c['close'])
    def _l(self, c): return min(c['open'], c['close']) - c['low']
    def _r(self, c): return c['high'] - c['low']
    def _is_bull(self, c): return c['close'] > c['open']
    def _is_bear(self, c): return c['close'] < c['open']
    def detect_doji(self, df, i):
        c = df.iloc[i]; b, r = self._b(c), self._r(c)
        if r > 0 and b/r < 0.1:
            u, l = self._u(c), self._l(c)
            if u > 0 and l > 0: return {'name':'Long-Legged Doji','direction':'neutral','tier':3}
            if u > b*2: return {'name':'Gravestone Doji','direction':'bearish','tier':2}
            if l > b*2: return {'name':'Dragonfly Doji','direction':'bullish','tier':1}
            return {'name':'Doji','direction':'neutral','tier':2}
        return None
    def detect_hammer(self, df, i):
        if i < 1: return None
        c = df.iloc[i]
        return {'name':'Hammer','direction':'bullish','tier':1} if self._l(c) > self._b(c)*2 and self._u(c) < self._b(c)*0.5 else None
    def detect_shooting_star(self, df, i):
        if i < 1: return None
        c = df.iloc[i]
        return {'name':'Shooting Star','direction':'bearish','tier':2} if self._u(c) > self._b(c)*2 and self._l(c) < self._b(c)*0.5 else None
    def detect_engulfing_bull(self, df, i):
        if i < 1: return None
        c, p = df.iloc[i], df.iloc[i-1]
        return {'name':'Bullish Engulfing','direction':'bullish','tier':1} if self._is_bear(p) and self._is_bull(c) and c['close'] > p['open'] and c['open'] < p['close'] else None
    def detect_engulfing_bear(self, df, i):
        if i < 1: return None
        c, p = df.iloc[i], df.iloc[i-1]
        return {'name':'Bearish Engulfing','direction':'bearish','tier':1} if self._is_bull(p) and self._is_bear(c) and c['close'] < p['open'] and c['open'] > p['close'] else None
    def detect_piercing(self, df, i):
        if i < 1: return None
        c, p = df.iloc[i], df.iloc[i-1]
        return {'name':'Piercing Line','direction':'bullish','tier':1} if self._is_bear(p) and self._is_bull(c) and c['open'] < p['close'] and c['close'] > (p['open']+p['close'])/2 and c['close'] < p['open'] else None
    def detect_dark_cloud(self, df, i):
        if i < 1: return None
        c, p = df.iloc[i], df.iloc[i-1]
        return {'name':'Dark Cloud','direction':'bearish','tier':1} if self._is_bull(p) and self._is_bear(c) and c['open'] > p['close'] and c['close'] < (p['open']+p['close'])/2 and c['close'] > p['open'] else None
    def detect_harami_bull(self, df, i):
        if i < 1: return None
        c, p = df.iloc[i], df.iloc[i-1]
        return {'name':'Bullish Harami','direction':'bullish','tier':2} if self._is_bear(p) and self._is_bull(c) and c['open'] > p['close'] and c['close'] < p['open'] else None
    def detect_harami_bear(self, df, i):
        if i < 1: return None
        c, p = df.iloc[i], df.iloc[i-1]
        return {'name':'Bearish Harami','direction':'bearish','tier':2} if self._is_bull(p) and self._is_bear(c) and c['open'] < p['close'] and c['close'] > p['open'] else None
    def detect_marubozu(self, df, i):
        c = df.iloc[i]; b, r = self._b(c), self._r(c)
        if r > 0 and b/r > 0.9:
            if self._is_bull(c): return {'name':'White Marubozu','direction':'bullish','tier':1}
            return {'name':'Black Marubozu','direction':'bearish','tier':1}
        return None
    def detect_morning_star(self, df, i):
        if i < 2: return None
        c1, c2, c3 = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
        return {'name':'Morning Star','direction':'bullish','tier':1} if self._is_bear(c1) and self._is_bull(c3) and self._r(c2) > 0 and self._b(c2)/self._r(c2) < 0.3 and c3['close'] > (c1['open']+c1['close'])/2 and c2['high'] < c1['close'] else None
    def detect_evening_star(self, df, i):
        if i < 2: return None
        c1, c2, c3 = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
        return {'name':'Evening Star','direction':'bearish','tier':1} if self._is_bull(c1) and self._is_bear(c3) and self._r(c2) > 0 and self._b(c2)/self._r(c2) < 0.3 and c3['close'] < (c1['open']+c1['close'])/2 and c2['low'] > c1['close'] else None
    def detect_three_white_soldiers(self, df, i):
        if i < 2: return None
        c1, c2, c3 = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
        return {'name':'Three White Soldiers','direction':'bullish','tier':1} if self._is_bull(c1) and self._is_bull(c2) and self._is_bull(c3) and c2['close'] > c1['close'] and c3['close'] > c2['close'] and all(self._u(c) < self._b(c)*0.3 for c in [c1,c2,c3]) else None
    def detect_three_black_crows(self, df, i):
        if i < 2: return None
        c1, c2, c3 = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
        return {'name':'Three Black Crows','direction':'bearish','tier':1} if self._is_bear(c1) and self._is_bear(c2) and self._is_bear(c3) and c2['close'] < c1['close'] and c3['close'] < c2['close'] and all(self._l(c) < self._b(c)*0.3 for c in [c1,c2,c3]) else None
    def detect_all_patterns(self, df, window_size=10):
        patterns = []; start = max(0, len(df) - window_size)
        detectors = [self.detect_doji, self.detect_hammer, self.detect_shooting_star, self.detect_engulfing_bull, self.detect_engulfing_bear, self.detect_piercing, self.detect_dark_cloud, self.detect_harami_bull, self.detect_harami_bear, self.detect_marubozu, self.detect_morning_star, self.detect_evening_star, self.detect_three_white_soldiers, self.detect_three_black_crows]
        for idx in range(start, len(df)):
            if idx < 1: continue
            for det in detectors:
                p = det(df, idx)
                if p and not any(x['name'] == p['name'] and x.get('index') == idx for x in patterns):
                    p['index'] = idx; p['date'] = str(df.index[idx]) if hasattr(df.index[idx], 'strftime') else str(idx)
                    patterns.append(p)
        return patterns

# ==================== DATACLASS ====================
@dataclass
class StockAnalysisReport:
    symbol: str; current_price: float; market_cap: float; sector: str; df: pd.DataFrame; realtime_data: Dict
    rsi_value: float; adx_value: float; atr_value: float; macd_status: str; final_stop: float
    price_targets: Dict; reward_risk_ratio: float
    reward_risk_final: float = 0.0; avg_daily_volume_50: int = 0; last_day_volume: int = 0
    volume_pct_of_avg: float = 0.0; liquidity_pass: bool = False; weekly_aligned: bool = False
    a_mqs: float = 0.0; enhancer_count: int = 0; enhancer_pct: float = 0.0
    core_filters_pass: bool = False; swing_setup_type: str = ''; market_phase: str = ''
    signal_type: str = ''; primary_candle_pattern: str = ''
    all_candle_patterns: List[Dict] = field(default_factory=list)
    cprs: float = 0.0; ab_variant: str = 'NONE'; ab_modifier: float = 0.0
    bayesian_pass: bool = False; bayesian_deep: bool = False
    buy_zone: float = 0.0; target_zone: float = 0.0; var_stop: float = 0.0
    mcmc_signal: str = 'WAIT'; mcmc_mu: float = 0.0; mcmc_mu_raw: float = 0.0
    mcmc_sigma: float = 0.0; mcmc_t_stat: float = 0.0; breadth_theta: float = 0.0
    momentum_score: int = 0; projection_1w: float = 0.0; canvas_signal: str = 'WAIT'
    canvas_reason: str = ''; fc_mu_shrunk: float = 0.0; fc_r_mid: float = 0.0
    fc_sigma_1w: float = 0.0; fc_P_low: float = 0.0; fc_P_mid: float = 0.0; fc_P_high: float = 0.0
    fc_band_pct: float = 0.0; fc_version: str = 'DRIFT'; forecast_signal: str = 'IGNORE'
    forecast_reason: str = ''; verdict: str = 'RED'; weinstein_stage2: bool = False
    buy_conditions: Dict = field(default_factory=dict); radar_score: int = 0
    radar_flags: str = ''; last_turnover: float = 0.0
    fuzzy_P_mu: float = 0.5; fuzzy_P_mu_sigma: float = 0.5; fuzzy_B: float = 0.0
    fuzzy_S: float = 0.0; fuzzy_t: float = 0.0; fuzzy_N: int = 0; fuzzy_C_raw: float = 0.0
    fuzzy_C_penalty: float = 0.0; fuzzy_C: float = 0.0; fuzzy_P_sigma: float = 0.5
    fuzzy_P_DD: float = 0.5; fuzzy_R_raw: float = 1.0; fuzzy_R: float = 1.0
    fuzzy_FinalScore: float = 0.0; fuzzy_RegimeDamp: float = 1.0
    fuzzy_Action: str = 'Neutral / Hold'; fuzzy_ActionBn: str = '⏳ নিরপেক্ষ / অপেক্ষা'
    fuzzy_Size: str = '0%'; fuzzy_Color: str = 'wait'
    fuzzy_v3_M: float = 0.0; fuzzy_v3_P: float = 0.0; fuzzy_v3_C: float = 0.0
    fuzzy_v3_C_raw: float = 0.0; fuzzy_v3_R: float = 0.0; fuzzy_v3_L: float = 0.0
    fuzzy_v3_S_gate: float = 0.0; fuzzy_v3_S_rank: float = 0.0
    fuzzy_v3_turnover_velocity: float = 0.0; fuzzy_v3_Reasons: str = ''
    fuzzy_v3_hard_gate_pass: bool = False; fuzzy_v3_skip_reason: str = ''
    fuzzy_v3_th_strong: float = 0.60; fuzzy_v3_th_mild: float = 0.45; fuzzy_v3_th_watch: float = 0.30
    extension_pct_from_sma20: float = 0.0; sma20_value: float = 0.0
    volume_divergence_days: int = 0; volume_divergence_total: int = 0
    hike_tier: str = TIER_NONE; hike_tier_entry_allowed: bool = False
    ab_rpd_a: float = 0.0; ab_rpd_b: float = 0.0; ab_selected_variant: str = 'NONE'
    ab_position_size: int = 0; ab_max_loss: float = 0.0; ab_risk_per_share: float = 0.0
    ab_entry_price: float = 0.0; ab_stop_loss: float = 0.0; ab_take_profit: float = 0.0
    ab_max_holding_days: int = 0; ab_expected_rpd: float = 0.0
    ab_confidence_score: float = 0.0; ab_rank_score: float = 0.0
    sme_catcher_level: str = 'NONE'; sme_l_score: float = 0.0; sme_volume_ratio: float = 0.0
    sme_price_vs_ema20: float = 0.0; sme_ema20_rising: bool = False; sme_close_position: float = 0.0
    sme_relative_strength: bool = False; sme_entry_type: str = 'NONE'; sme_position_size_pct: float = 0.0
    sme_liquidity_multiplier: float = 0.0; sme_market_cap_missing: bool = False
    sme_emergency_floor_ok: bool = False; sme_adtv_20: float = 0.0; sme_free_float_est: float = 0.0
    sme_data_health: str = 'OK'; sme_ignition: bool = False
    final_score: float = 0.0; final_signal: str = 'WAIT'; asset_class: str = 'EQ'

# ==================== ANALYSIS ====================
def _classify_asset(symbol):
    s = (symbol or " ").upper()
    return "MF" if any(tag in s for tag in ("MUTUAL","1STMF","2NDMF","3RDNRB","1STICB","GROWTH","GREEN","GREENMF","POPULAR1","TRUSTB1","PRIME1ICBA","LRGLOBMF","IFILISLMF","IFIC1STMF","ICBEPMF","ICBAMCL","ICBAGRANI","ICBSONALI","PHPMF1","MBL1STMF","NCCBLMF","EBLNRBMF","EBL1STMF","DBH1STMF","EXIM1STMF","ABB1STMF","AIBL1STIMF","PF1STMF","CSE1STMF")) else "EQ"

def analyze_stock_canvas(symbol, dsex_series=None, breadth_theta=0.0, regime_factor=1.0, regime_name='NEUTRAL', vol_label='Low', account_equity=1000000.0, ab_mode='LONG'):
    realtime = get_realtime_data(symbol)
    if not realtime: return None
    df = get_historical_data(symbol, realtime)
    if df is None or df.empty: return None

    current_price = float(realtime.get('close', df['close'].iloc[-1]))
    if current_price <= 0: current_price = float(df['close'].iloc[-1])

    rsi = calculate_rsi(df['close']); adx = calculate_adx(df); atr = calculate_atr(df)
    _, _, macd_hist = calculate_macd(df['close'])
    macd_status = "Bullish Cross" if float(macd_hist.iloc[-1]) > 0 else "Bearish Cross"

    live_vol = float(realtime.get('volume', 0) or 0)
    projected_vol, projection_mode = project_full_day_volume(live_vol)
    vol20 = float(df['volume'].tail(20).mean()) if len(df) >= 20 else 0.0
    avg_vol_50 = int(df['volume'].tail(50).mean()) if len(df) >= 50 else int(vol20)

    # v1.31: weekend / closed handling
    if projection_mode in ('OK', 'EARLY'):
        volume_pct_of_avg = projected_vol / avg_vol_50 if avg_vol_50 > 0 else 1.0
        sme_volume_ratio = projected_vol / vol20 if vol20 > 0 else 0.0
    else:
        volume_pct_of_avg = 1.0
        sme_volume_ratio = 1.0

    radar_score, radar_flags, last_turnover = compute_radar_score(df, realtime, rsi, adx, macd_hist)
    vp = get_volume_profile(df); phase = detect_wyckoff_smc(df, vp)
    oflow = detect_order_flow_signal(df); decision = evaluate_signal(df, realtime, vp, phase, oflow)
    final_stop = get_dynamic_stop(df, current_price, is_long=True)

    risk = current_price - final_stop
    if risk <= 0: risk = current_price * 0.05; final_stop = current_price - risk
    target_atr = current_price + ATR_TP2_MULT * atr
    rr_prelim = (target_atr - current_price) / risk if risk > 0 else 0.0
    rr = rr_prelim; target1 = target_atr; turnover = current_price * live_vol
    liquidity_pass = turnover > 1e7 and live_vol > 100

    weekly_aligned = False
    if len(df) >= 30:
        try:
            weekly = df.resample('W-FRI', on='date').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
            if len(weekly) >= 10: weekly_aligned = weekly['close'].iloc[-1] > weekly['close'].ewm(span=20).mean().iloc[-1]
        except Exception: pass

    green_days = (df['close'].diff() > 0).tail(90).sum() / min(90, len(df))
    ag_ = df['close'].diff().where(df['close'].diff() > 0, 0).rolling(14).mean().iloc[-1]
    al_ = -df['close'].diff().where(df['close'].diff() < 0, 0).rolling(14).mean().iloc[-1]
    gl = ag_ / (al_ + 1e-9); vt = 1.0 if df['volume'].tail(20).mean() > df['volume'].tail(40).head(20).mean()*1.05 else 0.5
    raw = green_days*0.4 + min(gl, 2.0)*0.3 + vt*0.3
    ra = {'Bull':1.0, 'Sideways':0.9, 'Bear':0.8}.get(regime_name, 0.9); a_mqs = raw * ra

    live_high = float(realtime.get('high') or 0); live_low = float(realtime.get('low') or 0)
    day_high = live_high if live_high > 0 else float(df['high'].iloc[-1])
    day_low = live_low if live_low > 0 else float(df['low'].iloc[-1])
    day_range = day_high - day_low
    if day_range > 0: close_position = max(0.0, min(1.0, (current_price - day_low) / day_range))
    elif day_high > 0: close_position = 1.0 if current_price >= day_high else 0.0
    else: close_position = 0.5

    ema20_s = df['close'].ewm(span=20, adjust=False).mean()
    ema20_cur = float(ema20_s.iloc[-1]) if len(ema20_s) else current_price
    ema20_prv = float(ema20_s.iloc[-6]) if len(ema20_s) >= 6 else ema20_cur
    sme_ema20_rising = bool(ema20_cur > ema20_prv)
    price_vs_ema20 = (current_price / ema20_cur - 1) * 100 if ema20_cur > 0 else 0.0

    market_cap = realtime.get('market_cap', 0) or 0
    adtv_20 = float((df['close'] * df['volume']).tail(20).mean()) if len(df) >= 20 else 0.0
    free_float_est = float(avg_vol_50) if avg_vol_50 > 0 else 0.0

    l_score = 0.0
    if adtv_20 >= 400_000: l_score += 0.20
    elif adtv_20 >= 200_000: l_score += 0.10
    if free_float_est >= 1_000_000: l_score += 0.15
    elif free_float_est >= 500_000: l_score += 0.08
    market_cap_missing = False
    if market_cap >= 50_000_000: l_score += 0.15
    elif market_cap > 0: l_score += 0.05
    else: market_cap_missing = True
    if last_turnover >= 500_000: l_score += 0.10
    elif last_turnover >= 200_000: l_score += 0.05
    l_score = min(1.0, l_score)
    if market_cap_missing: l_score = max(l_score, SME_L_SCORE_FLOOR_NO_MCAP)

    emergency_floor_ok = bool(adtv_20 >= SME_ADTV_MIN and free_float_est >= SME_FREE_FLOAT_MIN and (market_cap >= SME_MARKET_CAP_MIN or market_cap_missing))
    volume_ok = bool(sme_volume_ratio >= SME_VOLUME_MULT); trend_ok = bool(price_vs_ema20 >= -SME_PRICE_VS_EMA20_PCT)
    day_high_safe = day_high if day_high > 0 else current_price * 1.001
    ignition = bool(projection_mode in ('OK', 'EARLY') and sme_volume_ratio >= 2.0 and close_position >= 0.70 and current_price >= day_high_safe * 0.99)

    core_catcher = bool(l_score >= SME_L_SCORE_MIN and emergency_floor_ok and ((volume_ok and trend_ok) or ignition))
    strong_momentum = bool(current_price > ema20_cur and sme_ema20_rising and close_position >= SME_CLOSE_POSITION_MIN)
    if core_catcher and strong_momentum and ignition: sme_catcher_level = 'STRONG'
    elif core_catcher: sme_catcher_level = 'CORE'
    else: sme_catcher_level = 'NONE'

    liquidity_mult = SME_LIQ_MIN
    for th, m in SME_LIQ_TIERS:
        if l_score >= th: liquidity_mult = m; break
    cap = 0.20 if sme_catcher_level == 'STRONG' else 0.12 if sme_catcher_level == 'CORE' else 0.0
    position_size_pct = round(cap * liquidity_mult * 100, 1)

    if ignition or (current_price > day_high_safe * 0.99 and sme_volume_ratio >= 1.2): sme_entry_type = 'Breakout'
    elif sme_volume_ratio >= 1.2 and len(df) >= 2 and current_price > float(df['close'].iloc[-2]): sme_entry_type = 'Continuation'
    elif price_vs_ema20 > -3.0 and close_position < 0.4: sme_entry_type = 'Pullback'
    else: sme_entry_type = 'None'

    dsex_ret_20 = 0.0
    if dsex_series is not None and len(dsex_series) >= 21: dsex_ret_20 = float((dsex_series.iloc[-1] / dsex_series.iloc[-21] - 1) * 100)
    stk_ret_20 = float((current_price / df['close'].iloc[-21] - 1) * 100) if len(df) >= 21 else 0.0
    rel_strength = bool(stk_ret_20 > dsex_ret_20)

    last_utc, src = read_data_health_cache(HEALTH_CACHE_PATH, symbol)
    realtime_for_health = {'source': realtime.get('source', src or 'unknown')}
    sme_data_health = compute_data_health(realtime_for_health, last_utc)

    extension_pct, sma20_value = compute_extension_from_sma20(df, current_price)
    vol_div_max, vol_div_total = compute_volume_divergence_days(df, VOL_DIVERGENCE_LOOKBACK)
    hike_class = classify_tier(extension_pct, vol_div_max, rsi)
    hike_tier = hike_class.tier; hike_entry_allowed = tier_entry_allowed(hike_tier)

    detector = CandleDetector(); candle_patterns = detector.detect_all_patterns(df, window_size=10)

    enhancers = 0
    if any(p.get('tier') == 1 and p.get('direction') == 'bullish' for p in candle_patterns): enhancers += 1
    if len(df) >= 30:
        rr_ = df.tail(30); ranges = rr_['high'] - rr_['low']
        if len(ranges) >= 10 and ranges.iloc[-5:].mean() < 0.8 * ranges.iloc[-10:-5].mean(): enhancers += 1
    if avg_vol_50 > 0 and projected_vol > 1.5 * avg_vol_50: enhancers += 1
    if weekly_aligned: enhancers += 1
    if breadth_theta > 0.05: enhancers += 1
    if price_vs_ema20 > -2.0 and sme_ema20_rising: enhancers += 1
    if close_position >= 0.65 and sme_volume_ratio >= 1.2: enhancers += 1

    above_50 = current_price > df['close'].rolling(50).mean().iloc[-1]
    adx_ok = adx >= 25; a_mqs_ok = a_mqs >= 0.5; vol_ok = projected_vol > 0.8 * avg_vol_50 if avg_vol_50 > 0 else True; rr_ok = rr >= 2.0
    core_pass = above_50 and adx_ok and a_mqs_ok and vol_ok and rr_ok

    primary = None
    if candle_patterns:
        candle_patterns.sort(key=lambda x: (x.get('tier', 99), -x.get('index', 0))); primary = candle_patterns[0]
    primary_name = primary['name'] if primary else None

    rsi_th = 55 if regime_name == 'Sideways' else 60; adx_th = 22 if regime_name == 'Sideways' else 30; vol_th = 1.3 if regime_name == 'Sideways' else 1.5
    momentum_score = 0
    if rsi > rsi_th: momentum_score += 1
    if volume_pct_of_avg > vol_th: momentum_score += 1
    if current_price > df['close'].rolling(20).mean().iloc[-1]: momentum_score += 1
    if adx > adx_th: momentum_score += 1

    report = StockAnalysisReport(symbol=symbol, current_price=current_price, market_cap=market_cap, sector=realtime.get('sector', ''), df=df, realtime_data=realtime, rsi_value=rsi, adx_value=adx, atr_value=atr, macd_status=macd_status, final_stop=final_stop, price_targets={'target1': target1, 'target2': target1*1.5}, reward_risk_ratio=rr_prelim, reward_risk_final=0.0, avg_daily_volume_50=avg_vol_50, last_day_volume=int(live_vol), volume_pct_of_avg=volume_pct_of_avg, liquidity_pass=liquidity_pass, weekly_aligned=weekly_aligned, a_mqs=a_mqs, enhancer_count=enhancers, enhancer_pct=min(1.0, enhancers/9.0), core_filters_pass=core_pass, swing_setup_type=phase['event'], market_phase=phase['phase'], signal_type=decision['signal'], primary_candle_pattern=primary_name if primary_name else " ", all_candle_patterns=candle_patterns, radar_score=radar_score, radar_flags=radar_flags, last_turnover=last_turnover, sme_catcher_level=sme_catcher_level, sme_l_score=round(l_score,3), sme_volume_ratio=round(sme_volume_ratio,3), sme_price_vs_ema20=round(price_vs_ema20,3), sme_ema20_rising=sme_ema20_rising, sme_close_position=round(close_position,3), sme_relative_strength=rel_strength, sme_entry_type=sme_entry_type, sme_position_size_pct=position_size_pct, sme_liquidity_multiplier=round(liquidity_mult,2), sme_market_cap_missing=bool(market_cap_missing), sme_emergency_floor_ok=emergency_floor_ok, sme_adtv_20=round(adtv_20,0), sme_free_float_est=round(free_float_est,0), sme_data_health=sme_data_health, sme_ignition=ignition, momentum_score=momentum_score, extension_pct_from_sma20=round(extension_pct, 3), sma20_value=round(sma20_value, 3), volume_divergence_days=int(vol_div_max), volume_divergence_total=int(vol_div_total), hike_tier=hike_tier, hike_tier_entry_allowed=hike_entry_allowed, asset_class=_classify_asset(symbol))

    setup_map = {'High-2':10,'Low-2':10,'Reverse Divergence':9,'2B Reversal':8,'VCP':8,'Pattern Failure':7,'Support Reversal':6,'Resistance Reversal':6,'Spring':10,'BOS (Up)':9,'BOS (Down)':1,'UTAD':2,'None':3}
    setup_score = setup_map.get(phase['event'], 3); tech = 10 if adx > 40 else 8 if adx >= 25 else 5 if adx >= 20 else 3
    vol_ = 10 if report.volume_pct_of_avg > 2.0 else 8 if report.volume_pct_of_avg > 1.5 else 7 if report.volume_pct_of_avg < 0.7 else 4
    fund = 7 if (realtime.get('pe_ratio') is not None and realtime.get('pe_ratio', 99) < 20) else 5
    gate = 1.0 if core_pass else 0.0
    report.cprs = 0.25*a_mqs + 0.20*setup_score/10 + 0.15*tech/10 + 0.10*vol_/10 + 0.10*gate + 0.10*fund/10
    confidence = (a_mqs*100*0.4) + (report.cprs*100*0.3) + ((enhancers/9)*100*0.3); confidence = min(100, max(0, confidence))

    a_pass = (current_price > df['close'].rolling(20).mean().iloc[-1] and rsi > 55 and (report.volume_pct_of_avg > 1.5 if report.avg_daily_volume_50 > 0 else False))
    b_pass = (df['close'].rolling(50).mean().iloc[-1] < current_price < df['close'].rolling(200).mean().iloc[-1] and realtime.get('pe_ratio') is not None and realtime.get('pe_ratio', 99) < 20)

    def rpd(tgt, stp, days):
        g, l = tgt - current_price, current_price - stp
        return 0.0 if g <= 0 or l <= 0 or days <= 0 else (((confidence/100)*g) - ((1-confidence/100)*l)) / days

    rpd_a = rpd(current_price + 2*atr, current_price - 1*atr, AB_VARIANT_A_MAX_DAYS) if a_pass else 0.0
    rpd_b = rpd(current_price + 3*atr, current_price - 1.5*atr, AB_VARIANT_B_MAX_DAYS) if b_pass else 0.0
    if rpd_a > 0 and rpd_b == 0: report.ab_variant = 'A'
    elif rpd_b > 0 and rpd_a == 0: report.ab_variant = 'B'
    elif rpd_a > rpd_b * 1.1: report.ab_variant = 'A'
    elif rpd_b > rpd_a * 1.1: report.ab_variant = 'B'
    elif rpd_a > 0 and rpd_b > 0: report.ab_variant = 'SPLIT'
    else: report.ab_variant = 'NONE'
    report.ab_rpd_a = round(rpd_a,4); report.ab_rpd_b = round(rpd_b,4)
    report.ab_selected_variant = report.ab_variant; report.ab_confidence_score = round(confidence,1)
    if report.ab_variant == 'A': report.ab_entry_price = current_price; report.ab_stop_loss = current_price - AB_VARIANT_A_SL_ATR*atr; report.ab_take_profit = current_price + AB_VARIANT_A_TP_ATR*atr; report.ab_max_holding_days = AB_VARIANT_A_MAX_DAYS if ab_mode == 'LONG' else AB_SHORT_MODE_DAYS
    elif report.ab_variant == 'B': report.ab_entry_price = current_price; report.ab_stop_loss = current_price - AB_VARIANT_B_SL_ATR*atr; report.ab_take_profit = current_price + AB_VARIANT_B_TP_ATR*atr; report.ab_max_holding_days = AB_VARIANT_B_MAX_DAYS if ab_mode == 'LONG' else AB_SHORT_MODE_DAYS
    else: report.ab_entry_price = current_price; report.ab_stop_loss = current_price - atr; report.ab_take_profit = current_price + 2*atr; report.ab_max_holding_days = AB_VARIANT_A_MAX_DAYS
    rps = report.ab_entry_price - report.ab_stop_loss
    if rps > 0:
        ml = account_equity * AB_MAX_RISK_PCT
        report.ab_max_loss = round(ml,2); report.ab_risk_per_share = round(rps,2); report.ab_position_size = int(ml / rps)
    if hike_tier == TIER_LATE and report.ab_position_size > 0: report.ab_position_size = int(report.ab_position_size * LATE_POSITION_MULT)
    report.ab_expected_rpd = max(rpd_a, rpd_b)

    mcmc = compute_mcmc(symbol, df, current_price, dsex_series)
    report.buy_zone = mcmc['buy_zone']; report.target_zone = mcmc['target']; report.var_stop = mcmc['stop']
    report.mcmc_signal = mcmc['signal']; report.mcmc_mu = mcmc['mu_hat']; report.mcmc_mu_raw = mcmc.get('mu_hat_raw', 0.0)
    report.mcmc_sigma = mcmc['sigma_adapt']; report.mcmc_t_stat = mcmc['t_stat']
    report.bayesian_pass = bool(current_price <= mcmc['buy_zone'] * 1.03); report.bayesian_deep = bool(current_price <= mcmc['buy_zone'])
    report.breadth_theta = breadth_theta

    ma20_val = float(df['close'].rolling(20).mean().iloc[-1]) if len(df) >= 20 else current_price
    below_ma20 = current_price < ma20_val
    bearish_candle = any(p.get('direction') == 'bearish' and p.get('tier') == 1 for p in candle_patterns)
    weak_rsi = rsi < 40; strong_bear_trend = (adx > 25) and below_ma20
    if (core_pass and momentum_score >= 3 and enhancers >= 2 and (report.bayesian_pass or momentum_score >= 4)): canvas, cr = 'BUY', 'Strong: core+momentum+enh+bayes'
    elif (core_pass and momentum_score >= 2 and enhancers >= 1 and report.bayesian_pass): canvas, cr = 'BUY', 'Moderate: core+momentum+bayes'
    elif (core_pass and momentum_score >= 2 and enhancers >= 2 and not report.bayesian_pass): canvas, cr = 'HOLD', 'Core intact, awaiting entry'
    elif (not below_ma20) and (not bearish_candle) and rsi >= 45: canvas, cr = 'HOLD', 'Above MA20, healthy RSI'
    elif ((bearish_candle and below_ma20) or (below_ma20 and weak_rsi and strong_bear_trend) or (below_ma20 and mcmc['signal'] == 'SELL' and adx > 22)): canvas, cr = 'SELL', 'Below MA20 with confirmation'
    else: canvas, cr = 'WAIT', 'No decisive signal'
    if hike_tier == TIER_EXHAUSTION: canvas, cr = 'SELL', f'EXHAUSTION_WATCH (ext={extension_pct:.1f}%, vol_div={vol_div_max}d)'
    elif hike_tier == TIER_LATE and canvas == 'BUY': canvas, cr = 'HOLD', f'LATE_HIKE damped from BUY (ext={extension_pct:.1f}%)'
    report.canvas_signal = canvas; report.canvas_reason = cr

    bc = evaluate_buy_conditions(report, dsex_series, regime_name, vol_label)
    report.buy_conditions = bc; report.verdict = bc['Verdict']; report.weinstein_stage2 = bool(bc['Weinstein_Stage2'])

    forecast = compute_forecast_1w(price=current_price, mcmc_mu=mcmc['mu_hat'], mcmc_t_stat=mcmc['t_stat'], mcmc_sigma=mcmc['sigma_adapt'], volume_conf=report.volume_pct_of_avg, verdict=bc['Verdict'], weinstein_stage2='YES' if bc['Weinstein_Stage2'] else 'NO', regime=regime_name, avg_turnover=mcmc['avg_turnover'], a_mqs=a_mqs, use_simplified=(abs(mcmc['t_stat']) < 1.8))
    report.fc_mu_shrunk = forecast['mu_shrunk']; report.fc_r_mid = forecast['r_mid_effective']
    report.fc_sigma_1w = forecast['sigma_1w']; report.fc_P_low = forecast['P_low']; report.fc_P_mid = forecast['P_mid']; report.fc_P_high = forecast['P_high']
    report.fc_band_pct = forecast['band_width_pct']; report.fc_version = forecast['version']; report.forecast_signal = forecast['signal']; report.forecast_reason = forecast['reason']
    report.projection_1w = current_price * np.exp(forecast['mu_shrunk'] * TRADING_DAYS_1W)
    reward = max(forecast['P_high'] - current_price, 0.0); rr_final = min(reward / max(risk, 1e-6), RR_FINAL_CAP); report.reward_risk_final = float(rr_final)

    fz = compute_fuzzy_v3(df=df, mcmc=mcmc, patterns=candle_patterns, breadth_theta=breadth_theta, regime_factor=regime_factor, price_vs_ema20=price_vs_ema20, volume_ratio=sme_volume_ratio, close_position=close_position, rsi=rsi, adx=adx, turnover=last_turnover, avg_vol_50=avg_vol_50, market_cap=market_cap, free_float_cap=free_float_est, adtv20=adtv_20, extension_pct=extension_pct, vol_div_days=vol_div_max, hike_tier=hike_tier)
    report.fuzzy_v3_M = fz['M']; report.fuzzy_v3_P = fz['P']; report.fuzzy_v3_C = fz['C']; report.fuzzy_v3_C_raw = fz.get('C_raw', 0.0); report.fuzzy_v3_R = fz['R']; report.fuzzy_v3_L = fz['L']
    report.fuzzy_v3_S_gate = fz['S_gate']; report.fuzzy_v3_S_rank = fz['S_rank']; report.fuzzy_v3_turnover_velocity = fz['turnover_velocity']
    report.fuzzy_v3_Reasons = safe_json_dumps(fz['Reasons'], ensure_ascii=False); report.fuzzy_v3_hard_gate_pass = fz['hard_gate_pass']; report.fuzzy_v3_skip_reason = fz['skip_reason']
    report.fuzzy_v3_th_strong = fz['th_strong']; report.fuzzy_v3_th_mild = fz['th_mild']; report.fuzzy_v3_th_watch = fz['th_watch']

    report.fuzzy_S = fz['S_rank']; report.fuzzy_C = fz['C']; report.fuzzy_R = fz['R']; report.fuzzy_FinalScore = fz['S_rank']
    report.fuzzy_Action = fz['Action']; report.fuzzy_ActionBn = fz['ActionBn']; report.fuzzy_Size = fz['Size']; report.fuzzy_Color = fz['Color']
    report.fuzzy_t = mcmc['t_stat']; report.fuzzy_N = mcmc['n_eff']; report.fuzzy_P_mu = mcmc.get('P_mu', 0.5); report.fuzzy_P_mu_sigma = 0.5
    report.fuzzy_B = breadth_theta; report.fuzzy_C_raw = fz.get('C_raw', 0.0); report.fuzzy_C_penalty = 1.0; report.fuzzy_P_sigma = 0.5; report.fuzzy_P_DD = 0.5
    report.fuzzy_R_raw = fz['R']; report.fuzzy_RegimeDamp = 1.0

    if hike_tier == TIER_LATE and report.sme_position_size_pct > 0: report.sme_position_size_pct = round(report.sme_position_size_pct * LATE_POSITION_MULT, 1)

    _fuzzy_rank = float(fz.get('S_rank', 0.0) or 0.0); _gate_pass = bool(fz.get('hard_gate_pass', False))
    _radar_norm = max(0.0, min(1.0, float(radar_score) / 10.0)); _rr_norm = max(0.0, min(1.0, float(rr_final) / 3.0)); _sme_l_norm = max(0.0, min(1.0, float(l_score)))
    report.final_score = round(FINAL_W_FUZZY * max(0.0, min(1.0, _fuzzy_rank)) + FINAL_W_RADAR * _radar_norm + FINAL_W_RR * _rr_norm + FINAL_W_SME_L * _sme_l_norm, 4)
    final_signal = consensus_signal(gate_pass=_gate_pass, data_health=sme_data_health, final_score=report.final_score, canvas_signal=canvas, hike_tier=hike_tier, regime=regime_name)
    report.final_signal = final_signal; report.canvas_signal = final_signal
    return report

# ==================== BUY CONDITIONS ====================
def get_dynamic_thresholds(regime, vol_label):
    if regime == 'Bull' and vol_label == 'Low': return {'mqs_min':0.45,'rsi_min':50,'adx_min':20,'vol_min':1.0,'pos_size':1.25}
    if regime == 'Bull' and vol_label == 'High': return {'mqs_min':0.50,'rsi_min':52,'adx_min':25,'vol_min':1.2,'pos_size':1.0}
    if regime == 'Sideways' and vol_label == 'Low': return {'mqs_min':0.55,'rsi_min':55,'adx_min':25,'vol_min':1.5,'pos_size':0.75}
    if regime == 'Sideways' and vol_label == 'High': return {'mqs_min':0.60,'rsi_min':58,'adx_min':30,'vol_min':2.0,'pos_size':0.50}
    return {'mqs_min':0.65,'rsi_min':60,'adx_min':30,'vol_min':2.0,'pos_size':0.25}

def evaluate_buy_conditions(report, dsex_series, regime, vol_label):
    th = get_dynamic_thresholds(regime, vol_label)
    dsex_close = dsex_series.iloc[-1] if dsex_series is not None and len(dsex_series) > 0 else report.current_price
    r1 = bool(dsex_close > dsex_series.rolling(200).mean().iloc[-1]) if dsex_series is not None and len(dsex_series) >= 200 else True
    hh = bool(report.df['high'].iloc[-1] > report.df['high'].iloc[-5]) if len(report.df) >= 5 else False
    hl = bool(report.df['low'].iloc[-1] > report.df['low'].iloc[-5]) if len(report.df) >= 5 else False
    a50 = bool(report.current_price > report.df['close'].rolling(50).mean().iloc[-1])
    r2 = bool(hh and hl and a50); r3 = bool(report.adx_value >= th['adx_min'])
    pcs = bool(report.current_price <= report.df['close'].rolling(50).mean().iloc[-1] * 1.02)
    bo = bool(report.a_mqs >= 0.70 and report.current_price > report.df['close'].rolling(20).mean().iloc[-1])
    r4 = bool(pcs or bo); r5 = bool(report.rsi_value >= th['rsi_min'] and report.macd_status == "Bullish Cross")
    has_bull = any(p.get('direction') == 'bullish' for p in report.all_candle_patterns)
    r6 = bool(has_bull and report.enhancer_count >= 2); r7 = bool(report.volume_pct_of_avg >= th['vol_min'])
    rrm = 2.0 if regime == 'Bull' else 2.5 if regime == 'Sideways' else 3.0
    r8 = bool(report.reward_risk_ratio >= rrm)
    rules = [r1,r2,r3,r4,r5,r6,r7,r8]; tp = int(sum(rules))
    pg = (report.df['close'].diff() > 0).tail(90).mean() * 100 if len(report.df) >= 90 else 50
    cg = 0
    for i in range(len(report.df)-1, -1, -1):
        if i > 0 and report.df['close'].iloc[i] > report.df['close'].iloc[i-1]: cg += 1
        else: break
    fip = (pg/10) + min(cg,10); fm = 8 if regime == 'Bull' else 12 if regime == 'Sideways' else 15; fp = bool(fip >= fm)
    bm = 0
    obv = (np.sign(report.df['close'].diff()).fillna(0) * report.df['volume']).cumsum()
    if len(obv) > 20 and obv.tail(10).mean() > obv.tail(20).mean(): bm += 2
    if report.avg_daily_volume_50 > 0 and report.last_day_volume > 1.2*report.avg_daily_volume_50: bm += 1
    if report.adx_value >= th['adx_min']: bm += 1
    if report.rsi_value >= th['rsi_min']: bm += 1
    if report.current_price > report.df['close'].rolling(20).mean().iloc[-1]: bm += 1
    if report.weekly_aligned: bm += 1
    bt = 6 if regime == 'Bull' else 5 if regime == 'Sideways' else 7; bmp = bool(bm >= bt)
    ma200 = report.df['close'].rolling(200, min_periods=100).mean().iloc[-1]
    ma50s = report.df['close'].rolling(50, min_periods=30).mean()
    ma50n = ma50s.iloc[-1] if len(ma50s) else report.current_price; ma50p = ma50s.iloc[-6] if len(ma50s) >= 6 else ma50n
    s2 = bool(not np.isnan(ma200) and report.current_price > ma200 and not np.isnan(ma50n) and not np.isnan(ma50p) and ma50n > ma50p)
    if tp >= 7 and fp and bmp and s2: v = 'GREEN'
    elif tp >= 5 and (fp or bmp): v = 'YELLOW'
    else: v = 'RED'
    return {'Rule1':r1,'Rule2':r2,'Rule3':r3,'Rule4':r4,'Rule5':r5,'Rule6':r6,'Rule7':r7,'Rule8':r8,'TotalPass':tp,'FIP_Score':round(float(fip),1),'FIP_Pass':fp,'BigMoney_Score':int(bm),'BigMoney_Pass':bmp,'Weinstein_Stage2':s2,'Verdict':v}

# ==================== BANGLA LABELS ====================
def _bn_sig(sig):
    return {'BUY':('🟢 এখন কেনার জন্য ভালো সময়','buy'),'HOLD':('🟡 হাতে ধরে রাখুন','hold'),'WAIT':('⏳ এখন অপেক্ষা করুন','wait'),'SELL':('🔴 বিক্রি করার কথা ভাবুন','sell')}.get(sig, ('❔ স্পষ্ট সংকেত নেই','wait'))

def _bn_radar(f):
    m = {'BB_SQUEEZE':'দামের ওঠানামা ছোট — বড় নড়াচড়ার প্রস্তুতি','VOL_DRYUP':'লেনদেন শান্ত — বড় ক্রেতাদের আগমনী সময়','NEAR_HIGH':'সাম্প্রতিক উঁচু দামের কাছাকাছি','RSI_ZONE':'দাম সঠিক গতিতে এগোচ্ছে','ADX_RISING':'ঊর্ধ্বমুখী প্রবণতা শক্তিশালী হচ্ছে','MACD_TURN':'দাম উপরের দিকে ঘুরে দাঁড়াচ্ছে','OBV_RISING':'বড় ক্রেতারা ধীরে ধীরে কিনছে','ABOVE_MA20':'গড় দামের উপরে অবস্থান','VOL_IGNITION':'🔥 হঠাৎ লেনদেন অনেক বেড়েছে','LIQ_OK':'পর্যাপ্ত কেনাবেচা হচ্ছে'}
    return m.get(f, f)

def _bn_rsi(r):
    r = float(r)
    if r >= 75: return 'খুব গরম — সাবধান'
    if r >= 60: return 'শক্তিশালী — ক্রেতার চাপ ভালো'
    if r >= 45: return 'স্বাভাবিক — দুই দিকেই যেতে পারে'
    if r >= 30: return 'দুর্বল — বিক্রির চাপ আছে'
    return 'খুব ঠান্ডা — হয়তো ঘুরে দাঁড়ানোর সময়'

def _bn_verdict(v):
    return {'GREEN':'সমস্ত শর্ত পূরণ','YELLOW':'বেশিরভাগ শর্ত পূরণ','RED':'এখনো স্পষ্ট সমর্থন নেই'}.get(v, v)

def _bn_trend(r):
    try: c20 = float(r.df['close'].rolling(20).mean().iloc[-1]); c50 = float(r.df['close'].rolling(50).mean().iloc[-1])
    except Exception: return 'নির্ধারণ করা যায়নি'
    p = r.current_price
    if p > c20 > c50: return 'দাম উপরের দিকে — ক্রেতা শক্তিশালী'
    if p < c20 < c50: return 'দাম নিচের দিকে — বিক্রেতা শক্তিশালী'
    return 'দাম পাশাপাশি চলছে'

def _bn_reasons(r):
    rs = []
    if r.canvas_signal == 'BUY': rs.append('দাম গড়ের উপরে + ক্রেতা শক্তিশালী')
    if r.radar_score >= 3: rs.append(f'{r.radar_score}টি ইতিবাচক লক্ষণ')
    if 'VOL_IGNITION' in (r.radar_flags or ''): rs.append('🔥 হঠাৎ লেনদেন বেড়েছে')
    if r.rsi_value >= 60: rs.append('দাম বাড়ার গতি ভালো')
    if r.momentum_score >= 3: rs.append('স্বল্পমেয়াদে গতি ইতিবাচক')
    if r.canvas_signal == 'HOLD' and r.canvas_reason: rs.append(f'কারণ: {r.canvas_reason}')
    if r.canvas_signal == 'SELL' and r.canvas_reason: rs.append(f'কারণ: {r.canvas_reason}')
    if r.canvas_signal == 'SELL': rs.append('দাম নিচের দিকে ঝুঁকছে — সাবধান')
    if r.canvas_signal == 'WAIT': rs.append('এখনো স্পষ্ট সংকেত নেই')
    if r.sme_data_health not in ('OK','LIVE','FRESH'): rs.append(f'⚠️ তথ্যের মান: {r.sme_data_health}')
    tier_label, _ = TIER_BN.get(getattr(r, 'hike_tier', TIER_NONE), ('', ''))
    if tier_label: rs.append(f'🥾 হাইক স্তর: {tier_label} (ext={r.extension_pct_from_sma20:.1f}%)')
    if r.volume_divergence_days >= VOL_DIVERGENCE_OVERRIDE: rs.append(f'📉 {r.volume_divergence_days} দিন ভলিউম-ডাইভারজেন্স — DISTRIBUTION')
    if not rs: rs.append('সাধারণ অবস্থা')
    return rs

def report_to_bangla_dict(r, dts):
    rt, rc = _bn_sig(r.canvas_signal); price = float(r.current_price); proj = float(r.projection_1w or price)
    pd_ = ((proj - price) / price * 100.0) if price > 0 else 0.0; arrow = '▲' if pd_ > 0.3 else '▼' if pd_ < -0.3 else '▬'
    flags = [f for f in (r.radar_flags or '').split(',') if f]; fl = [_bn_radar(f) for f in flags] or ['এখন বিশেষ ইঙ্গিত নেই']
    reasons = _bn_reasons(r)
    if not isinstance(reasons, list): reasons = []
    if not reasons: reasons = ['সাধারণ অবস্থা']
    try: v3_reasons = json.loads(r.fuzzy_v3_Reasons) if r.fuzzy_v3_Reasons else []
    except Exception: v3_reasons = []
    tier_label, tier_cls = TIER_BN.get(getattr(r, 'hike_tier', TIER_NONE), ('', ''))
    return {'symbol':str(r.symbol),'price':safe_float(price,2),'proj':safe_float(proj,2),'projDiff':safe_float(pd_,1),'projArrow':str(arrow),'recText':str(rt),'recClass':str(rc),'trend':str(_bn_trend(r)),'rsiText':str(_bn_rsi(r.rsi_value)),'verdictText':str(_bn_verdict(r.verdict)),'reasons':list(reasons),'flags':list(fl),'buyZone':safe_float(r.buy_zone or price,2),'target':safe_float(r.target_zone or price*1.05,2),'stop':safe_float(r.var_stop or price*0.95,2),'radarScore':safe_int(r.radar_score),'canvasSignal':str(r.canvas_signal),'canvasReason':str(r.canvas_reason),'hasIgnition':bool('VOL_IGNITION' in (r.radar_flags or '')),'verdict':str(r.verdict),'fuzzyAction':str(r.fuzzy_ActionBn),'fuzzyActionEn':str(r.fuzzy_Action),'fuzzyFinal':safe_float(r.fuzzy_FinalScore,3),'fuzzyColor':str(r.fuzzy_Color),'fuzzySize':str(r.fuzzy_Size),'dataHealth':str(r.sme_data_health),'smeIgnition':bool(r.sme_ignition),'fuzzyV3Reasons':v3_reasons,'finalScore': safe_float(r.final_score, 4),'finalSignal': str(r.final_signal),'assetClass': str(r.asset_class),'fuzzyCRaw': safe_float(r.fuzzy_v3_C_raw, 4),'hikeTier': str(getattr(r, 'hike_tier', TIER_NONE)),'hikeTierBn': str(tier_label), 'hikeTierCls': str(tier_cls),'extensionPct': safe_float(getattr(r, 'extension_pct_from_sma20', 0.0), 2),'sma20': safe_float(getattr(r, 'sma20_value', 0.0), 2),'volDivDays': safe_int(getattr(r, 'volume_divergence_days', 0)),'volDivTotal': safe_int(getattr(r, 'volume_divergence_total', 0)),'tierEntryAllowed': bool(getattr(r, 'hike_tier_entry_allowed', False)),'rrPrelim': safe_float(getattr(r, 'reward_risk_ratio', 0.0), 2),'rrFinal': safe_float(getattr(r, 'reward_risk_final', 0.0), 2)}

# ==================== HTML GENERATOR (FULL — v1.31) ====================
def generate_bangla_all_in_one_html(reports, timestamp, account_equity=1000000.0, ab_mode='LONG'):
    try:
        dto = dt.datetime.strptime(timestamp, '%Y%m%d_%H%M'); dts = dto.strftime('%d-%m-%Y, %H:%M')
    except Exception: dts = timestamp

    def ab_vn(v): return {'A':('মোমেন্টাম কৌশল','a'),'B':('ভ্যালু কৌশল','b'),'SPLIT':('৫০/৫০','split')}.get(v, ('কোনো কৌশল নেই','none'))
    def ab_an(v, a, b):
        if v == 'A': return f'মোমেন্টাম কৌশল বেছে নিন (RPD: {a:.4f})'
        if v == 'B': return f'ভ্যালু কৌশল বেছে নিন (RPD: {b:.4f})'
        if v == 'SPLIT': return 'দুই কৌশলেই অর্ধেক বিনিয়োগ'
        return 'এখন কোনো ট্রেড নেবেন না'
    def fz_bn(a): return {'Strong Buy':('🟢🟢 দৃঢ় ক্রয়','buy-strong'),'Mild Buy':('🟢 মৃদু ক্রয়','buy-mild'),'Neutral / Watch':('⏳ নিরপেক্ষ / অপেক্ষা','neutral'),'Avoid':('🔴 এড়িয়ে যান','sell-strong')}.get(a, ('❔ অজানা','neutral'))
    def fz_sz(a): return {'Strong Buy':'সর্বোচ্চ মূলধন (৮-১০%)','Mild Buy':'অর্ধেক (৪-৫%)','Neutral / Watch':'নগদ / কোনো ট্রেড নয় (০%)','Avoid':'নগদ / কোনো ট্রেড নয় (০%)'}.get(a, 'স্পষ্ট নয়')
    def fz_rs(r):
        rs = []
        if r.fuzzy_v3_M >= 0.6: rs.append('📈 শক্তিশালী মোমেন্টাম')
        elif r.fuzzy_v3_M <= 0.3: rs.append('📉 দুর্বল মোমেন্টাম')
        if r.fuzzy_v3_P >= 0.6: rs.append('🕯️ বুলিশ প্যাটার্ন / প্রাইস অ্যাকশন')
        if r.fuzzy_v3_C >= 0.6: rs.append('✅ MCMC আত্মবিশ্বাস বেশি')
        elif r.fuzzy_v3_C < 0.3: rs.append('⚠️ MCMC আত্মবিশ্বাস কম')
        if r.fuzzy_v3_R >= 0.7: rs.append('🌊 অনুকূল বাজার অবস্থা')
        elif r.fuzzy_v3_R < 0.4: rs.append('🔥 প্রতিকূল বাজার')
        if r.fuzzy_v3_L < 0.5: rs.append('⚠️ তারল্য তুলনামূলক কম')
        if not r.fuzzy_v3_hard_gate_pass: rs.append(f'⛔ হার্ড গেট: {r.fuzzy_v3_skip_reason}')
        else: rs.append(f'✅ হার্ড গেট পাস (TV={r.fuzzy_v3_turnover_velocity:.5f})')
        if getattr(r, 'hike_tier', TIER_NONE) == TIER_EXHAUSTION: rs.append(f'🔴 EXHAUSTION_WATCH — ext={r.extension_pct_from_sma20:.1f}%, vol_div={r.volume_divergence_days}d')
        elif getattr(r, 'hike_tier', TIER_NONE) == TIER_LATE: rs.append(f'🟠 LATE_HIKE — ext={r.extension_pct_from_sma20:.1f}% (সাইজ ক্যাপ)')
        return rs
    def sme_c(l): return {'STRONG':('🔥 শক্তিশালী সন্ধান','strong'),'CORE':('✅ মূল সন্ধান','core'),'NONE':('⏭️ এড়িয়ে যান','none')}.get(l, ('❔ অজানা','none'))
    def sme_e(e): return {'Breakout':'🚀 ব্রেকআউট (ভাঙার মুহূর্ত)','Pullback':'📉 পুলব্যাক (পিছু হটা) - সবচেয়ে পছন্দের','Continuation':'➡️ কন্টিনিউয়েশন (চলমান)','None':'⏭️ প্রবেশ নেই'}.get(e, e)
    def sme_v(v):
        try: v = float(v)
        except Exception: v = 0.0
        if v >= 2.0: return '🔥🔥 খুব বেশি'
        if v >= 1.5: return '🔥 বেশি'
        if v >= 1.2: return '✅ স্বাভাবিকের চেয়ে বেশি'
        if v >= 0.8: return '➖ স্বাভাবিক'
        return '📉 কম'
    def sme_l(l):
        try: l = float(l)
        except Exception: l = 0.0
        if l >= 0.7: return '🟢 চমৎকার'
        if l >= 0.5: return '🟡 ভালো'
        if l >= 0.3: return '🟠 গ্রহণযোগ্য'
        return '🔴 দুর্বল'
    def sme_p(pct, c):
        try: pct = float(pct)
        except Exception: pct = 0.0
        if c == 'NONE': return '⏭️ এড়িয়ে যান (০%)'
        if pct >= 12: return f'💰 বড় বিনিয়োগ ({pct:.0f}%)'
        if pct >= 5: return f'💰 মাঝারি বিনিয়োগ ({pct:.0f}%)'
        if pct > 0: return f'💰 ছোট বিনিয়োগ ({pct:.0f}%)'
        return '❌ বিনিয়োগ নেই'

    max_rpd_batch = max([max(r.ab_rpd_a or 0, r.ab_rpd_b or 0) for r in reports] or [1e-9])
    if max_rpd_batch <= 0: max_rpd_batch = 1e-9

    sp_data = []
    for r in reports:
        vb, vc = ab_vn(r.ab_selected_variant); ab_act = ab_an(r.ab_selected_variant, r.ab_rpd_a, r.ab_rpd_b)
        fzab, fzac = fz_bn(r.fuzzy_Action); fzsb = fz_sz(r.fuzzy_Action); fzrs = fz_rs(r)
        cb, cc = sme_c(r.sme_catcher_level); eb = sme_e(r.sme_entry_type)
        lbn = sme_l(r.sme_l_score); pbn = sme_p(r.sme_position_size_pct, r.sme_catcher_level); vbn = sme_v(r.sme_volume_ratio)
        rt, rc = _bn_sig(r.canvas_signal); price = float(r.current_price); proj = float(r.projection_1w or price)
        pd_ = ((proj - price) / price * 100.0) if price > 0 else 0.0; arrow = '▲' if pd_ > 0.3 else '▼' if pd_ < -0.3 else '▬'
        try: ls = float(r.sme_l_score)
        except Exception: ls = 0.0
        if r.sme_catcher_level == 'STRONG': sme_ps = 0.7 + ls * 0.3
        elif r.sme_catcher_level == 'CORE': sme_ps = 0.4 + ls * 0.3
        else: sme_ps = 0.0
        ab_ps = (r.ab_rpd_a * 0.4 + r.ab_rpd_b * 0.4 + (r.ab_confidence_score or 0) / 100.0 * 0.2) if (r.ab_rpd_a + r.ab_rpd_b) > 0 else 0
        fz_ps = (abs(r.fuzzy_v3_S_rank) * ((r.fuzzy_v3_C + r.fuzzy_v3_R) / 2.0) if r.fuzzy_v3_C > 0 else 0)
        flags = [f for f in (r.radar_flags or '').split(',') if f]
        fl = [_bn_radar(f) for f in flags] or ['এখন বিশেষ কোনো ইঙ্গিত পাওয়া যায়নি।']
        reasons = _bn_reasons(r)
        rpd_max = max(r.ab_rpd_a or 0, r.ab_rpd_b or 0, 1e-9); rpd_norm = min(1.0, rpd_max / max_rpd_batch)
        conf_n = max(0.0, min(1.0, (r.ab_confidence_score or 0) / 100.0))
        tp = r.ab_take_profit or price; sl = r.ab_stop_loss or (price * 0.95)
        rr_v = max(0.0, tp - price) / max(0.01, price - sl); rr_n = max(0.0, min(1.0, rr_v / 3.0))
        fuzzy_n = max(0.0, min(1.0, r.fuzzy_v3_S_rank or 0.0))
        rank_score = 0.40 * rpd_norm + 0.25 * conf_n + 0.20 * rr_n + 0.15 * fuzzy_n
        tier_label, tier_cls = TIER_BN.get(getattr(r, 'hike_tier', TIER_NONE), ('', ''))
        sp_data.append({'symbol': str(r.symbol),'price': safe_float(price, 2),'proj': safe_float(proj, 2),'projDiff': safe_float(pd_, 1),'projArrow': str(arrow),'canvasSignal': str(r.canvas_signal),'recText': str(rt),'recClass': str(rc),'canvasReason': str(r.canvas_reason),'verdict': str(r.verdict),'verdictText': str(_bn_verdict(r.verdict)),'radarScore': safe_int(r.radar_score),'radarFlags': str(r.radar_flags),'radarFlagLines': list(fl),'hasIgnition': bool('VOL_IGNITION' in (r.radar_flags or '')),'buyZone': safe_float(r.buy_zone or price, 2),'target': safe_float(r.target_zone or price * 1.05, 2),'stop': safe_float(r.var_stop or price * 0.95, 2),'trend': str(_bn_trend(r)),'rsiText': str(_bn_rsi(r.rsi_value)),'rsi': safe_float(r.rsi_value, 1),'adx': safe_float(r.adx_value, 1),'aMQS': safe_float(r.a_mqs, 3),'reasons': list(reasons),'abVariant': str(r.ab_selected_variant),'abVariantBn': str(vb),'abVariantCls': str(vc),'abActionBn': str(ab_act),'abRpdA': safe_float(r.ab_rpd_a, 4),'abRpdB': safe_float(r.ab_rpd_b, 4),'abExpectedRpd': safe_float(r.ab_expected_rpd, 4),'abPositionSize': safe_int(r.ab_position_size),'abEntryPrice': safe_float(r.ab_entry_price, 2),'abStopLoss': safe_float(r.ab_stop_loss, 2),'abTakeProfit': safe_float(r.ab_take_profit, 2),'abMaxHoldingDays': safe_int(r.ab_max_holding_days),'abConfidence': safe_float(r.ab_confidence_score, 1),'abProductScore': safe_float(ab_ps, 4),'abMaxLoss': safe_float(r.ab_max_loss, 2),'abRiskPerShare': safe_float(r.ab_risk_per_share, 2),'abRankScore': safe_float(rank_score, 4),'abRewardRisk': safe_float(rr_v, 2),'fuzzyAction': str(r.fuzzy_ActionBn),'fuzzyActionEn': str(r.fuzzy_Action),'fuzzyActionCls': str(fzac),'fuzzyS': safe_float(r.fuzzy_v3_S_rank, 4),'fuzzySGate': safe_float(r.fuzzy_v3_S_gate, 4),'fuzzySRank': safe_float(r.fuzzy_v3_S_rank, 4),'fuzzyM': safe_float(r.fuzzy_v3_M, 4),'fuzzyP': safe_float(r.fuzzy_v3_P, 4),'fuzzyC': safe_float(r.fuzzy_v3_C, 4),'fuzzyCRaw': safe_float(r.fuzzy_v3_C_raw, 4),'fuzzyR': safe_float(r.fuzzy_v3_R, 4),'fuzzyL': safe_float(r.fuzzy_v3_L, 4),'fuzzyTV': safe_float(r.fuzzy_v3_turnover_velocity, 6),'fuzzyGatePass': safe_bool(r.fuzzy_v3_hard_gate_pass),'fuzzySkip': str(r.fuzzy_v3_skip_reason),'fuzzyThStrong': safe_float(r.fuzzy_v3_th_strong, 4),'fuzzyThMild': safe_float(r.fuzzy_v3_th_mild, 4),'fuzzyThWatch': safe_float(r.fuzzy_v3_th_watch, 4),'fuzzySizeBn': str(fzsb),'fuzzyColor': str(r.fuzzy_Color),'fuzzyReasons': list(fzrs),'fuzzyProductScore': safe_float(fz_ps, 4),'smeCatcherLevel': str(r.sme_catcher_level),'smeCatcherBn': str(cb),'smeCatcherCls': str(cc),'smeEntryType': str(r.sme_entry_type),'smeEntryBn': str(eb),'smeLScore': safe_float(r.sme_l_score, 3),'smeLScoreBn': str(lbn),'smePositionSizePct': safe_float(r.sme_position_size_pct, 1),'smePositionBn': str(pbn),'smeVolumeRatio': safe_float(r.sme_volume_ratio, 3),'smeVolumeBn': str(vbn),'smePriceVsEma20': safe_float(r.sme_price_vs_ema20, 3),'smeMarketCapMissing': safe_bool(r.sme_market_cap_missing),'smeProductScore': safe_float(sme_ps, 4),'finalScore': safe_float(r.final_score, 4),'finalSignal': str(r.final_signal),'assetClass': str(r.asset_class),'hikeTier': str(getattr(r, 'hike_tier', TIER_NONE)),'hikeTierBn': str(tier_label),'hikeTierCls': str(tier_cls),'extensionPct': safe_float(getattr(r, 'extension_pct_from_sma20', 0.0), 2),'sma20': safe_float(getattr(r, 'sma20_value', 0.0), 2),'volDivDays': safe_int(getattr(r, 'volume_divergence_days', 0)),'volDivTotal': safe_int(getattr(r, 'volume_divergence_total', 0)),'tierEntryAllowed': bool(getattr(r, 'hike_tier_entry_allowed', False))})

    sp_json = safe_json_dumps(sp_data, ensure_ascii=False); sp_total = len(sp_data)
    sp_buy = sum(1 for d in sp_data if d['canvasSignal']=='BUY'); sp_hold = sum(1 for d in sp_data if d['canvasSignal']=='HOLD')
    sp_wait = sum(1 for d in sp_data if d['canvasSignal']=='WAIT'); sp_sell = sum(1 for d in sp_data if d['canvasSignal']=='SELL')
    sp_strong = sum(1 for d in sp_data if d['smeCatcherLevel']=='STRONG'); sp_core = sum(1 for d in sp_data if d['smeCatcherLevel']=='CORE')

    summary_data = [report_to_bangla_dict(r, dts) for r in reports]; summary_json = safe_json_dumps(summary_data, ensure_ascii=False)
    s_total = len(summary_data); s_buy = sum(1 for d in summary_data if d['canvasSignal']=='BUY')
    s_hold = sum(1 for d in summary_data if d['canvasSignal']=='HOLD'); s_wait = sum(1 for d in summary_data if d['canvasSignal']=='WAIT')
    s_sell = sum(1 for d in summary_data if d['canvasSignal']=='SELL')

    ab_data = []
    for r in reports:
        vb, vc = ab_vn(r.ab_selected_variant); act = ab_an(r.ab_selected_variant, r.ab_rpd_a, r.ab_rpd_b)
        rpd_a = r.ab_rpd_a or 0.0; rpd_b = r.ab_rpd_b or 0.0; rpd_max = max(rpd_a, rpd_b, 1e-9)
        rpd_norm = min(1.0, rpd_max / max_rpd_batch); conf_n = max(0.0, min(1.0, (r.ab_confidence_score or 0) / 100.0))
        price = max(0.01, r.current_price or 0.01); tp = r.ab_take_profit or price; sl = r.ab_stop_loss or (price * 0.95)
        reward = max(0.0, tp - price); risk_r = max(0.01, price - sl); rr = reward / risk_r
        rr_n = max(0.0, min(1.0, rr / 3.0)); fuzzy_n = max(0.0, min(1.0, r.fuzzy_v3_S_rank or 0.0))
        rank_score = 0.40 * rpd_norm + 0.25 * conf_n + 0.20 * rr_n + 0.15 * fuzzy_n
        ps = (rpd_a * 0.4 + rpd_b * 0.4 + conf_n * 0.2) if (rpd_a + rpd_b) > 0 else 0.0
        ab_data.append({'symbol': str(r.symbol),'price': safe_float(price, 2),'variantBn': str(vb),'variant': str(r.ab_selected_variant),'variantCls': str(vc),'actionBn': str(act),'rpdA': safe_float(rpd_a, 4),'rpdB': safe_float(rpd_b, 4),'expectedRpd': safe_float(r.ab_expected_rpd, 4),'confidence': safe_float(r.ab_confidence_score, 1),'entryPrice': safe_float(r.ab_entry_price, 2),'stopLoss': safe_float(r.ab_stop_loss, 2),'takeProfit': safe_float(r.ab_take_profit, 2),'positionSize': safe_int(r.ab_position_size),'maxLoss': safe_float(r.ab_max_loss, 2),'riskPerShare': safe_float(r.ab_risk_per_share, 2),'maxHoldingDays': safe_int(r.ab_max_holding_days),'productScore': safe_float(ps, 4),'rewardRisk': safe_float(rr, 2),'rankScore': safe_float(rank_score, 4),'rpdNorm': safe_float(rpd_norm, 4),'canvasSignal': str(r.canvas_signal),'hikeTier': str(getattr(r, 'hike_tier', TIER_NONE))})
    ab_json = safe_json_dumps(ab_data, ensure_ascii=False); ab_total = len(ab_data)
    ab_a = sum(1 for d in ab_data if d['variant']=='A'); ab_b = sum(1 for d in ab_data if d['variant']=='B')
    ab_sp = sum(1 for d in ab_data if d['variant']=='SPLIT'); ab_no = sum(1 for d in ab_data if d['variant']=='NONE')
    ab_tradeable = ab_a + ab_b + ab_sp

    fuzzy_data = []
    for r in reports:
        ab_, ac = fz_bn(r.fuzzy_Action); sb = fz_sz(r.fuzzy_Action); rs = fz_rs(r)
        ps = (abs(r.fuzzy_v3_S_rank) * ((r.fuzzy_v3_C + r.fuzzy_v3_R) / 2.0) if r.fuzzy_v3_C > 0 else 0.0)
        fuzzy_data.append({'symbol': str(r.symbol),'price': safe_float(r.current_price, 2),'actionBn': str(ab_),'actionCls': str(ac),'action': str(r.fuzzy_Action),'sizeBn': str(sb),'finalScore': safe_float(r.fuzzy_v3_S_rank, 4),'S': safe_float(r.fuzzy_v3_S_rank, 4),'M': safe_float(r.fuzzy_v3_M, 4),'P': safe_float(r.fuzzy_v3_P, 4),'C': safe_float(r.fuzzy_v3_C, 4),'C_raw': safe_float(r.fuzzy_v3_C_raw, 4),'R': safe_float(r.fuzzy_v3_R, 4),'L': safe_float(r.fuzzy_v3_L, 4),'S_gate': safe_float(r.fuzzy_v3_S_gate, 4),'S_rank': safe_float(r.fuzzy_v3_S_rank, 4),'turnover_velocity': safe_float(r.fuzzy_v3_turnover_velocity, 6),'hard_gate_pass': safe_bool(r.fuzzy_v3_hard_gate_pass),'skip_reason': str(r.fuzzy_v3_skip_reason),'th_strong': safe_float(r.fuzzy_v3_th_strong, 4),'th_mild': safe_float(r.fuzzy_v3_th_mild, 4),'th_watch': safe_float(r.fuzzy_v3_th_watch, 4),'t': safe_float(r.fuzzy_t, 4),'N': safe_int(r.fuzzy_N),'reasons': rs,'productScore': safe_float(ps, 4),'canvasSignal': str(r.canvas_signal),'hikeTier': str(getattr(r, 'hike_tier', TIER_NONE))})
    fuzzy_json = safe_json_dumps(fuzzy_data, ensure_ascii=False); fz_total = len(fuzzy_data)
    fz_sb = sum(1 for d in fuzzy_data if d['action']=='Strong Buy'); fz_mb = sum(1 for d in fuzzy_data if d['action']=='Mild Buy')
    fz_n = sum(1 for d in fuzzy_data if d['action']=='Neutral / Watch'); fz_av = sum(1 for d in fuzzy_data if d['action']=='Avoid')

    sme_data = []
    for r in reports:
        cb, cc = sme_c(r.sme_catcher_level); eb = sme_e(r.sme_entry_type)
        vbn = sme_v(r.sme_volume_ratio); lbn = sme_l(r.sme_l_score); pbn = sme_p(r.sme_position_size_pct, r.sme_catcher_level)
        try: ls = float(r.sme_l_score)
        except Exception: ls = 0.0
        if r.sme_catcher_level == 'STRONG': ps = 0.7 + ls * 0.3
        elif r.sme_catcher_level == 'CORE': ps = 0.4 + ls * 0.3
        else: ps = 0.0
        sme_data.append({'symbol': str(r.symbol),'price': safe_float(r.current_price, 2),'catcherBn': str(cb),'catcherCls': str(cc),'catcherLevel': str(r.sme_catcher_level),'entryBn': str(eb),'entryType': str(r.sme_entry_type),'lScore': safe_float(r.sme_l_score, 3),'lScoreBn': str(lbn),'volumeRatio': safe_float(r.sme_volume_ratio, 3),'volumeBn': str(vbn),'priceVsEma20': safe_float(r.sme_price_vs_ema20, 3),'ema20Rising': safe_bool(r.sme_ema20_rising),'closePosition': safe_float(r.sme_close_position, 3),'positionSizePct': safe_float(r.sme_position_size_pct, 1),'positionBn': str(pbn),'liquidityMult': safe_float(r.sme_liquidity_multiplier, 2),'productScore': safe_float(ps, 4),'canvasSignal': str(r.canvas_signal),'marketCapMissing': safe_bool(r.sme_market_cap_missing),'relativeStrength': safe_bool(r.sme_relative_strength),'emergencyFloorOk': safe_bool(r.sme_emergency_floor_ok),'adtv20': safe_float(r.sme_adtv_20, 0),'dataHealth': str(r.sme_data_health),'ignition': safe_bool(r.sme_ignition),'hikeTier': str(getattr(r, 'hike_tier', TIER_NONE))})
    sme_json = safe_json_dumps(sme_data, ensure_ascii=False); sme_total = len(sme_data)
    sme_strong = sum(1 for d in sme_data if d['catcherLevel']=='STRONG'); sme_core = sum(1 for d in sme_data if d['catcherLevel']=='CORE')
    sme_none = sum(1 for d in sme_data if d['catcherLevel']=='NONE'); sme_mcap_missing = sum(1 for d in sme_data if d['marketCapMissing'])
    sme_breakout = sum(1 for d in sme_data if d['entryType']=='Breakout'); sme_pullback = sum(1 for d in sme_data if d['entryType']=='Pullback')
    sme_cont = sum(1 for d in sme_data if d['entryType']=='Continuation')

    hike_data = []
    for r in reports:
        tier_label, tier_cls = TIER_BN.get(getattr(r, 'hike_tier', TIER_NONE), ('', ''))
        hike_data.append({'symbol': str(r.symbol),'price': safe_float(r.current_price, 2),'sma20': safe_float(getattr(r, 'sma20_value', 0.0), 2),'extensionPct': safe_float(getattr(r, 'extension_pct_from_sma20', 0.0), 2),'volDivDays': safe_int(getattr(r, 'volume_divergence_days', 0)),'volDivTotal': safe_int(getattr(r, 'volume_divergence_total', 0)),'hikeTier': str(getattr(r, 'hike_tier', TIER_NONE)),'hikeTierBn': str(tier_label),'hikeTierCls': str(tier_cls),'tierEntryAllowed': bool(getattr(r, 'hike_tier_entry_allowed', False)),'rsi': safe_float(r.rsi_value, 1),'finalSignal': str(r.final_signal),'finalScore': safe_float(r.final_score, 4),'canvasSignal': str(r.canvas_signal),'canvasReason': str(r.canvas_reason)})
    hike_json = safe_json_dumps(hike_data, ensure_ascii=False); hike_total = len(hike_data)
    hike_counts = {t: sum(1 for d in hike_data if d['hikeTier'] == t) for t in TIER_ORDER}

    parts = []
    parts.append('<!DOCTYPE html><html lang="bn"><head><meta charset="UTF-8">')
    parts.append('<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">')
    parts.append(f'<title>সব বাংলা রিপোর্ট (একীভূত – ৬টি ট্যাব) — {dts}</title>')
    parts.append('''<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'Noto Sans Bengali','Segoe UI',Arial,sans-serif; background:#eef2f7; color:#1f2937; padding:12px; line-height:1.65; }
.wrap { max-width:1400px; margin:0 auto; }
.head { background:linear-gradient(135deg,#667eea 0%,#764ba2 100%); border-radius:20px; padding:22px 26px; margin-bottom:14px; color:#fff; box-shadow:0 10px 40px rgba(102,126,234,0.28); }
.head h1 { font-size:24px; margin-bottom:4px; }
.head .sub { font-size:13.5px; opacity:0.95; }
.head .mode { display:inline-block; background:rgba(255,255,255,0.22); padding:4px 14px; border-radius:20px; font-size:12.5px; margin-top:10px; }
.tabs { display:flex; gap:8px; margin-bottom:14px; flex-wrap:wrap; background:#fff; padding:10px; border-radius:16px; box-shadow:0 4px 20px rgba(0,0,0,0.05); }
.tab-btn { padding:10px 18px; border:none; background:#f1f5f9; border-radius:30px; font-size:13.5px; font-weight:600; cursor:pointer; color:#475569; font-family:inherit; transition:all .15s; }
.tab-btn:hover { background:#e2e8f0; }
.tab-btn.active { background:#2563eb; color:#fff; }
.tab-btn.tab-swing { background:#fef3c7; color:#92400e; border:1px dashed #f59e0b; }
.tab-btn.tab-swing.active { background:linear-gradient(90deg,#f59e0b,#ea580c); color:#fff; border:none; }
.tab-btn.tab-hike { background:#fee2e2; color:#991b1b; border:1px dashed #dc2626; }
.tab-btn.tab-hike.active { background:linear-gradient(90deg,#dc2626,#ef4444); color:#fff; border:none; }
.tab-content { display:none; } .tab-content.active { display:block; }
.sum-head { background:#fff; border-radius:18px; padding:16px 20px; box-shadow:0 6px 22px rgba(15,23,42,0.06); margin-bottom:14px; }
.sum-head h2 { font-size:20px; color:#0f172a; margin-bottom:4px; }
.sum-head .sub { font-size:13px; color:#64748b; margin-bottom:12px; }
.counts { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:10px; }
.cnt { background:#f8fafc; border:1px solid #e5e7eb; border-radius:12px; padding:10px; text-align:center; }
.cnt .num { font-size:22px; font-weight:800; color:#0f172a; }
.cnt .lbl { font-size:12px; color:#475569; margin-top:2px; }
.cnt.buy .num { color:#15803d; } .cnt.hold .num { color:#a16207; } .cnt.wait .num { color:#475569; }
.cnt.sell .num { color:#b91c1c; } .cnt.ign .num { color:#c2410c; } .cnt.radar .num { color:#0369a1; }
.cnt.stat-blue .num { color:#2563eb; } .cnt.stat-green .num { color:#059669; }
.cnt.stat-purple .num { color:#7c3aed; } .cnt.stat-orange .num { color:#ea580c; } .cnt.stat-warn .num { color:#f59e0b; }
.cnt.tier-none .num { color:#64748b; } .cnt.tier-early .num { color:#059669; }
.cnt.tier-mid .num { color:#a16207; } .cnt.tier-late .num { color:#ea580c; } .cnt.tier-exhaust .num { color:#dc2626; }
.filters { background:#fff; border-radius:16px; padding:14px 16px; margin-bottom:14px; box-shadow:0 6px 20px rgba(15,23,42,0.05); display:flex; flex-wrap:wrap; gap:10px; align-items:flex-end; }
.f { display:flex; flex-direction:column; gap:4px; min-width:140px; flex:1 1 auto; }
.f.wide { min-width:220px; flex:2 1 auto; } .f.narrow { min-width:100px; flex:0 1 auto; }
.f label { font-size:12px; font-weight:600; color:#334155; }
.f input, .f select { padding:9px 12px; border:1px solid #cbd5e1; border-radius:10px; font-size:14px; background:#fff; outline:none; font-family:inherit; width:100%; }
.f input:focus, .f select:focus { border-color:#2563eb; box-shadow:0 0 0 3px rgba(37,99,235,0.12); }
.btn { padding:10px 18px; border:none; border-radius:10px; font-size:14px; font-weight:600; cursor:pointer; font-family:inherit; background:#2563eb; color:#fff; }
.btn.ghost { background:#e2e8f0; color:#334155; }
.show-count { width:100%; font-size:13.5px; color:#334155; padding-top:8px; border-top:1px dashed #e5e7eb; }
.show-count b { color:#2563eb; font-size:15px; }
.grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(360px,1fr)); gap:14px; }
.grid.wide { grid-template-columns:repeat(auto-fill,minmax(430px,1fr)); }
.card { background:#fff; border-radius:18px; overflow:hidden; box-shadow:0 6px 22px rgba(15,23,42,0.06); border:1px solid #eef2f7; display:flex; flex-direction:column; }
.card.none { opacity:0.7; }
.sum-card-top { padding:14px 16px 12px; border-bottom:1px solid #eef2f7; }
.sum-sym { font-size:21px; font-weight:800; color:#0f172a; }
.sum-sym .radar-pill { margin-left:6px; font-size:11.5px; padding:3px 9px; background:#e0f2fe; color:#0369a1; border-radius:12px; font-weight:700; vertical-align:middle; }
.sum-sym .radar-pill.hot { background:#0ea5e9; color:#fff; }
.tier-pill { display:inline-block; margin-left:6px; font-size:11.5px; padding:3px 9px; border-radius:12px; font-weight:700; vertical-align:middle; }
.tier-pill.none { background:#e2e8f0; color:#475569; } .tier-pill.early { background:#d1fae5; color:#065f46; }
.tier-pill.mid { background:#fef3c7; color:#92400e; } .tier-pill.late { background:#ffedd5; color:#9a3412; } .tier-pill.exhaust { background:#fee2e2; color:#991b1b; }
.price-row { display:flex; justify-content:space-between; align-items:center; gap:10px; margin-top:8px; flex-wrap:wrap; }
.price-row .pr { font-size:26px; font-weight:800; color:#0c4a6e; } .price-row .pr small { font-size:14px; color:#0369a1; margin-left:3px; }
.price-row .proj { background:#f0f9ff; padding:8px 12px; border-radius:10px; font-size:13px; color:#0f172a; border:1px solid #bae6fd; }
.price-row .proj b { font-size:16px; color:#0f766e; }
.rec { padding:12px 16px; font-size:17px; font-weight:700; text-align:center; }
.rec.buy { background:#dcfce7; color:#166534; } .rec.hold { background:#fef9c3; color:#854d0e; }
.rec.wait { background:#e2e8f0; color:#334155; } .rec.sell { background:#fee2e2; color:#991b1b; }
.fuzzy-strip { padding:9px 16px; font-size:13px; font-weight:600; background:#f5f3ff; color:#5b21b6; border-top:1px solid #ede9fe; }
.fuzzy-strip.buy { background:#ecfdf5; color:#065f46; border-top-color:#a7f3d0; }
.fuzzy-strip.hold { background:#fefce8; color:#854d0e; border-top-color:#fef08a; }
.fuzzy-strip.wait { background:#f1f5f9; color:#334155; border-top-color:#e2e8f0; }
.fuzzy-strip.sell { background:#fef2f2; color:#991b1b; border-top-color:#fecaca; }
.sum-body { padding:12px 16px 14px; }
.block { margin-bottom:12px; } .block:last-child { margin-bottom:0; }
.block h3 { font-size:13.5px; color:#0f172a; margin-bottom:6px; }
.block ul { margin-left:18px; } .block li { font-size:13.5px; margin-bottom:3px; } .block p { font-size:13.5px; }
.levels { display:grid; grid-template-columns:repeat(3,1fr); gap:8px; }
.level { background:#f8fafc; border:1px solid #e5e7eb; border-radius:12px; padding:10px 8px; text-align:center; }
.level .lbl { font-size:11.5px; color:#64748b; margin-bottom:4px; }
.level .val { font-size:18px; font-weight:800; } .level .val small { font-size:12px; color:#475569; }
.level.buy .val { color:#15803d; } .level.tgt .val { color:#0369a1; } .level.stop .val { color:#b91c1c; }
.flags-inline { display:flex; flex-wrap:wrap; gap:6px; }
.flag-chip { background:#fff7ed; color:#9a3412; border:1px solid #fed7aa; padding:3px 10px; border-radius:12px; font-size:12px; }
.ignition-strip { background:linear-gradient(90deg,#fff7ed,#ffedd5); border-left:5px solid #f97316; padding:8px 12px; font-size:13.5px; color:#9a3412; font-weight:600; }
.exhaust-strip { background:linear-gradient(90deg,#fef2f2,#fee2e2); border-left:5px solid #dc2626; padding:8px 12px; font-size:13.5px; color:#991b1b; font-weight:600; }
.ab-card-header { padding:18px 20px 14px; background:linear-gradient(135deg,#f8fafc 0%,#f1f5f9 100%); border-bottom:1px solid #e2e8f0; }
.ab-card-header .sym { font-size:22px; font-weight:800; color:#1e293b; }
.ab-card-header .price { font-size:18px; color:#667eea; font-weight:700; margin-top:4px; }
.ab-variant { display:inline-block; padding:4px 12px; border-radius:20px; font-size:12px; font-weight:600; margin-top:8px; }
.ab-variant.a { background:#dbeafe; color:#1d4ed8; } .ab-variant.b { background:#d1fae5; color:#059669; }
.ab-variant.split { background:#fef3c7; color:#b45309; } .ab-variant.none { background:#fee2e2; color:#dc2626; }
.ab-action { background:#f0fdf4; border:1px solid #bbf7d0; border-radius:12px; padding:12px 16px; margin-bottom:14px; font-size:14px; font-weight:600; color:#166534; }
.ab-action.none { background:#fef2f2; border-color:#fecaca; color:#991b1b; }
.metrics { display:grid; grid-template-columns:repeat(2,1fr); gap:10px; margin-bottom:14px; }
.metric { background:#f8fafc; border-radius:10px; padding:10px 12px; }
.metric .lbl { font-size:11px; color:#64748b; } .metric .val { font-size:16px; font-weight:700; color:#1e293b; margin-top:2px; }
.metric .val.green { color:#059669; } .metric .val.red { color:#dc2626; } .metric .val.blue { color:#2563eb; }
.trade-plan { background:#fffbeb; border:1px solid #fde68a; border-radius:12px; padding:14px; margin-bottom:14px; }
.trade-plan h4 { font-size:13px; color:#92400e; margin-bottom:10px; }
.trade-plan .row { display:flex; justify-content:space-between; padding:4px 0; font-size:13px; }
.trade-plan .row .k { color:#78716c; } .trade-plan .row .v { font-weight:700; color:#1e293b; }
.trade-plan .row .v.green { color:#059669; } .trade-plan .row .v.red { color:#dc2626; }
.rpd-compare { background:#f5f3ff; border-radius:12px; padding:12px 14px; margin-bottom:14px; }
.rpd-compare h4 { font-size:12px; color:#6b21a8; margin-bottom:8px; }
.rpd-bar { display:flex; align-items:center; gap:10px; margin-bottom:6px; }
.rpd-bar .label { font-size:12px; width:50px; color:#64748b; }
.rpd-bar .bar { flex:1; height:20px; background:#e9d5ff; border-radius:10px; overflow:hidden; }
.rpd-bar .bar .fill { height:100%; border-radius:10px; background:linear-gradient(90deg,#8b5cf6,#a78bfa); }
.rpd-bar .bar .fill.b { background:linear-gradient(90deg,#059669,#34d399); }
.rpd-bar .value { font-size:12px; font-weight:700; color:#5b21b6; width:60px; text-align:right; }
.product-score { background:linear-gradient(135deg,#667eea 0%,#764ba2 100%); border-radius:12px; padding:12px 16px; color:#fff; display:flex; justify-content:space-between; align-items:center; }
.product-score .lbl { font-size:12px; opacity:0.9; } .product-score .val { font-size:22px; font-weight:800; }
.fz-card-header { padding:16px 20px; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #e2e8f0; }
.fz-card-header .sym { font-size:22px; font-weight:800; color:#1e293b; } .fz-card-header .price { font-size:16px; color:#64748b; }
.action-badge { padding:10px 20px; text-align:center; font-size:16px; font-weight:700; }
.action-badge.buy-strong { background:linear-gradient(90deg,#059669,#10b981); color:#fff; }
.action-badge.buy-mild { background:#d1fae5; color:#065f46; }
.action-badge.neutral { background:#f1f5f9; color:#475569; }
.action-badge.sell-mild { background:#fef3c7; color:#92400e; }
.action-badge.sell-strong { background:linear-gradient(90deg,#dc2626,#ef4444); color:#fff; }
.score-section { background:#f5f3ff; border-radius:12px; padding:14px; margin-bottom:14px; }
.score-section h4 { font-size:12px; color:#6b21a8; margin-bottom:10px; }
.score-row { display:flex; align-items:center; gap:10px; margin-bottom:8px; }
.score-row .label { font-size:12px; width:80px; color:#64748b; }
.score-row .bar { flex:1; height:18px; background:#e9d5ff; border-radius:9px; overflow:hidden; }
.score-row .bar .fill { height:100%; border-radius:9px; background:linear-gradient(90deg,#8b5cf6,#a78bfa); }
.score-row .bar .fill.green { background:linear-gradient(90deg,#059669,#34d399); }
.score-row .bar .fill.blue { background:linear-gradient(90deg,#2563eb,#60a5fa); }
.score-row .value { font-size:12px; font-weight:700; color:#5b21b6; width:50px; text-align:right; }
.final-score { background:linear-gradient(135deg,#8b5cf6 0%,#7c3aed 100%); border-radius:12px; padding:14px 18px; color:#fff; display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }
.final-score .lbl { font-size:13px; opacity:0.9; } .final-score .val { font-size:24px; font-weight:800; }
.fz-reasons { background:#f0fdf4; border-radius:12px; padding:12px 16px; margin-bottom:14px; }
.fz-reasons h4 { font-size:12px; color:#166534; margin-bottom:8px; }
.fz-reasons ul { margin-left:18px; } .fz-reasons li { font-size:13px; color:#166534; margin-bottom:4px; }
.size-box { background:#fef3c7; border:1px solid #fde68a; border-radius:12px; padding:12px 16px; display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }
.size-box .lbl { font-size:12px; color:#92400e; } .size-box .val { font-size:15px; font-weight:700; color:#92400e; }
.params { display:grid; grid-template-columns:repeat(3,1fr); gap:8px; margin-top:14px; padding-top:14px; border-top:1px dashed #e5e7eb; }
.param { background:#f8fafc; border-radius:8px; padding:8px; text-align:center; }
.param .lbl { font-size:10px; color:#64748b; } .param .val { font-size:13px; font-weight:700; color:#1e293b; }
.sme-card-header { padding:16px 20px; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #e2e8f0; }
.sme-card-header .sym { font-size:22px; font-weight:800; color:#1e293b; } .sme-card-header .price { font-size:16px; color:#64748b; }
.mcap-warn { font-size:10px; background:#fef3c7; color:#92400e; padding:2px 6px; border-radius:8px; margin-left:6px; }
.catcher-badge { padding:10px 20px; text-align:center; font-size:16px; font-weight:700; }
.catcher-badge.strong { background:linear-gradient(90deg,#dc2626,#ef4444); color:#fff; }
.catcher-badge.core { background:#d1fae5; color:#065f46; } .catcher-badge.none { background:#f1f5f9; color:#64748b; }
.entry-box { background:#eff6ff; border:1px solid #bfdbfe; border-radius:12px; padding:12px 16px; margin-bottom:14px; display:flex; justify-content:space-between; align-items:center; }
.entry-box .lbl { font-size:12px; color:#1d4ed8; } .entry-box .val { font-size:15px; font-weight:700; color:#1d4ed8; }
.sme-metrics { display:grid; grid-template-columns:repeat(2,1fr); gap:10px; margin-bottom:14px; }
.sme-metric { background:#f8fafc; border-radius:10px; padding:10px 12px; }
.sme-metric .lbl { font-size:11px; color:#64748b; } .sme-metric .val { font-size:15px; font-weight:700; color:#1e293b; margin-top:2px; }
.sme-metric .val.green { color:#059669; } .sme-metric .val.red { color:#dc2626; } .sme-metric .val.orange { color:#ea580c; }
.sme-score-section { background:#f0fdf4; border-radius:12px; padding:14px; margin-bottom:14px; }
.sme-score-section h4 { font-size:12px; color:#166534; margin-bottom:10px; }
.sme-score-row { display:flex; align-items:center; gap:10px; margin-bottom:8px; }
.sme-score-row .label { font-size:12px; width:70px; color:#64748b; }
.sme-score-row .bar { flex:1; height:18px; background:#d1fae5; border-radius:9px; overflow:hidden; }
.sme-score-row .bar .fill { height:100%; border-radius:9px; background:linear-gradient(90deg,#059669,#34d399); }
.sme-score-row .value { font-size:12px; font-weight:700; color:#065f46; width:50px; text-align:right; }
.position-box { background:#fef3c7; border:1px solid #fde68a; border-radius:12px; padding:12px 16px; display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }
.position-box .lbl { font-size:12px; color:#92400e; } .position-box .val { font-size:15px; font-weight:700; color:#92400e; }
.position-box.skip { background:#f1f5f9; border-color:#e2e8f0; }
.position-box.skip .lbl, .position-box.skip .val { color:#64748b; }
.sme-final-score { background:linear-gradient(135deg,#059669 0%,#10b981 100%); border-radius:12px; padding:14px 18px; color:#fff; display:flex; justify-content:space-between; align-items:center; }
.sme-final-score.none { background:linear-gradient(135deg,#6b7280 0%,#9ca3af 100%); }
.sme-final-score .lbl { font-size:13px; opacity:0.9; } .sme-final-score .val { font-size:24px; font-weight:800; }
.hike-card { background:#fff; border-radius:18px; overflow:hidden; box-shadow:0 6px 22px rgba(15,23,42,0.06); border:1px solid #fee2e2; display:flex; flex-direction:column; }
.hike-card.tier-none { border-color:#e2e8f0; } .hike-card.tier-early { border-color:#a7f3d0; }
.hike-card.tier-mid { border-color:#fde68a; } .hike-card.tier-late { border-color:#fdba74; } .hike-card.tier-exhaust { border-color:#fca5a5; }
.hike-card-top { padding:14px 16px 12px; border-bottom:1px solid #eef2f7; }
.hike-sym { font-size:20px; font-weight:800; color:#0f172a; } .hike-price { font-size:16px; color:#0c4a6e; font-weight:700; margin-top:2px; }
.hike-tier-badge { padding:10px 16px; text-align:center; font-size:15px; font-weight:700; }
.hike-tier-badge.none { background:#f1f5f9; color:#475569; } .hike-tier-badge.early { background:#d1fae5; color:#065f46; }
.hike-tier-badge.mid { background:#fef9c3; color:#854d0e; } .hike-tier-badge.late { background:#ffedd5; color:#9a3412; }
.hike-tier-badge.exhaust { background:linear-gradient(90deg,#dc2626,#ef4444); color:#fff; }
.hike-body { padding:12px 16px; }
.hike-metrics { display:grid; grid-template-columns:repeat(2,1fr); gap:8px; }
.hike-metric { background:#f8fafc; border-radius:10px; padding:8px 10px; }
.hike-metric .lbl { font-size:11px; color:#64748b; } .hike-metric .val { font-size:15px; font-weight:700; color:#0f172a; }
.hike-metric .val.green { color:#059669; } .hike-metric .val.red { color:#dc2626; }
.hike-metric .val.orange { color:#ea580c; } .hike-metric .val.blue { color:#2563eb; }
.hike-ext-bar { height:14px; background:linear-gradient(90deg,#d1fae5 0%,#fef3c7 25%,#ffedd5 55%,#fee2e2 100%); border-radius:7px; position:relative; margin:10px 0; }
.hike-ext-marker { width:4px; height:20px; background:#0f172a; position:absolute; top:-3px; border-radius:2px; }
.sp-card { background:#fff; border-radius:18px; overflow:hidden; box-shadow:0 6px 22px rgba(15,23,42,0.06); border:2px solid #fef3c7; display:flex; flex-direction:column; }
.sp-card-top { padding:14px 16px 12px; background:linear-gradient(135deg,#fffbeb 0%,#fef3c7 100%); border-bottom:1px solid #fde68a; }
.sp-sym { font-size:21px; font-weight:800; color:#92400e; }
.sp-sym .radar-pill { margin-left:6px; font-size:11.5px; padding:3px 9px; background:#fef3c7; color:#92400e; border-radius:12px; font-weight:700; border:1px solid #f59e0b; vertical-align:middle; }
.sp-sym .radar-pill.hot { background:#f59e0b; color:#fff; border-color:#d97706; }
.sp-price-row { display:flex; justify-content:space-between; align-items:center; gap:10px; margin-top:8px; flex-wrap:wrap; }
.sp-price-row .pr { font-size:24px; font-weight:800; color:#78350f; } .sp-price-row .pr small { font-size:14px; color:#a16207; margin-left:3px; }
.sp-price-row .proj { background:#fff; padding:8px 12px; border-radius:10px; font-size:13px; color:#0f172a; border:1px solid #fde68a; }
.sp-price-row .proj b { font-size:16px; color:#0f766e; }
.sp-rec { padding:10px 16px; font-size:16px; font-weight:700; text-align:center; }
.sp-rec.buy { background:#dcfce7; color:#166534; } .sp-rec.hold { background:#fef9c3; color:#854d0e; }
.sp-rec.wait { background:#e2e8f0; color:#334155; } .sp-rec.sell { background:#fee2e2; color:#991b1b; }
.sp-body { padding:12px 16px 14px; }
.sp-section { background:#f8fafc; border-radius:12px; padding:10px 12px; margin-bottom:10px; border-left:4px solid #94a3b8; }
.sp-section.ab { border-left-color:#667eea; background:#f5f7ff; }
.sp-section.fz { border-left-color:#8b5cf6; background:#faf5ff; }
.sp-section.sme { border-left-color:#059669; background:#f0fdf4; }
.sp-section.hike { border-left-color:#dc2626; background:#fef2f2; }
.sp-section h4 { font-size:12.5px; color:#0f172a; margin-bottom:6px; font-weight:700; display:flex; justify-content:space-between; align-items:center; gap:6px; }
.sp-section .row { display:flex; justify-content:space-between; padding:2px 0; font-size:12.5px; gap:10px; }
.sp-section .row .k { color:#64748b; flex-shrink:0; } .sp-section .row .v { font-weight:700; color:#1e293b; text-align:right; }
.sp-section .row .v.green { color:#059669; } .sp-section .row .v.red { color:#dc2626; }
.sp-section .row .v.blue { color:#2563eb; } .sp-section .row .v.orange { color:#ea580c; } .sp-section .row .v.purple { color:#7c3aed; }
.sp-badge { display:inline-block; padding:2px 10px; border-radius:12px; font-size:11px; font-weight:700; }
.sp-badge.buy-strong { background:#059669; color:#fff; } .sp-badge.buy-mild { background:#d1fae5; color:#065f46; }
.sp-badge.neutral { background:#f1f5f9; color:#475569; } .sp-badge.sell-mild { background:#fef3c7; color:#92400e; }
.sp-badge.sell-strong { background:#dc2626; color:#fff; } .sp-badge.strong { background:#dc2626; color:#fff; }
.sp-badge.core { background:#d1fae5; color:#065f46; } .sp-badge.none { background:#f1f5f9; color:#64748b; }
.sp-badge.a { background:#dbeafe; color:#1d4ed8; } .sp-badge.b { background:#d1fae5; color:#059669; } .sp-badge.split { background:#fef3c7; color:#b45309; }
.sp-verdict { display:inline-block; padding:2px 10px; border-radius:12px; font-size:11px; font-weight:700; }
.sp-verdict.green { background:#dcfce7; color:#166534; } .sp-verdict.yellow { background:#fef9c3; color:#854d0e; } .sp-verdict.red { background:#fee2e2; color:#991b1b; }
.sp-flags { display:flex; flex-wrap:wrap; gap:4px; margin-top:4px; }
.sp-flag-chip { background:#fff7ed; color:#9a3412; border:1px solid #fed7aa; padding:2px 8px; border-radius:10px; font-size:11px; }
.empty { text-align:center; padding:40px 20px; color:#64748b; background:#fff; border-radius:16px; }
.empty .big { font-size:42px; margin-bottom:8px; }
.foot { text-align:center; color:#64748b; font-size:12.5px; margin:18px 0 6px; }
.print-btn { position:fixed; right:16px; bottom:16px; z-index:100; background:#2563eb; color:#fff; border:none; padding:14px 18px; border-radius:50%; font-size:22px; cursor:pointer; box-shadow:0 10px 25px rgba(37,99,235,0.35); font-family:inherit; }
.print-btn:hover { background:#1d4ed8; }
@media print { body { background:#fff; padding:0; } .filters, .print-btn, .tabs { display:none !important; } .grid, .grid.wide { grid-template-columns:1fr 1fr; gap:8px; } .card, .sp-card, .hike-card { box-shadow:none; border:1px solid #ddd; break-inside:avoid; } }
</style>''')
    parts.append('</head><body><div class="wrap">')

    parts.append(f'''
  <div class="head">
    <h1>📄 সব বাংলা রিপোর্ট (একীভূত – ৬টি ট্যাব) — v1.31 (JSON API + Weekend Fix)</h1>
    <div class="sub">রিপোর্ট সময়: <b>{dts}</b> &bull; অ্যাকাউন্ট: <b>{account_equity:,.0f}</b> টাকা</div>
    <div class="mode">📈 A/B মোড: {'স্বল্পমেয়াদী (১০ দিন)' if ab_mode == 'SHORT' else 'দীর্ঘমেয়াদী (২০ দিন)'}</div>
  </div>''')

    parts.append('''
  <div class="tabs">
    <button class="tab-btn tab-swing active" onclick="switchTab('swingparam')">🎯 Swing Parameter</button>
    <button class="tab-btn tab-hike" onclick="switchTab('hike')">🥾 Hike Tier</button>
    <button class="tab-btn" onclick="switchTab('summary')">📄 সহজ সারসংক্ষেপ</button>
    <button class="tab-btn" onclick="switchTab('ab')">📊 A/B Testing MCMC</button>
    <button class="tab-btn" onclick="switchTab('fuzzy')">🧠 ফাজি লজিক</button>
    <button class="tab-btn" onclick="switchTab('sme')">🚀 Swing Momentum</button>
  </div>''')

    parts.append(f'''
  <div id="tab-swingparam" class="tab-content active">
    <div class="sum-head" style="background:linear-gradient(135deg,#fffbeb,#fef3c7);border-left:5px solid #f59e0b;">
      <h2>🎯 Swing Parameter — একীভূত ভিউ (v1.31)</h2>
      <div class="sub">মোট স্টক: <b>{sp_total}</b></div>
      <div class="counts">
        <div class="cnt buy"><div class="num">{sp_buy}</div><div class="lbl">🟢 BUY</div></div>
        <div class="cnt hold"><div class="num">{sp_hold}</div><div class="lbl">🟡 HOLD</div></div>
        <div class="cnt wait"><div class="num">{sp_wait}</div><div class="lbl">⏳ WAIT</div></div>
        <div class="cnt sell"><div class="num">{sp_sell}</div><div class="lbl">🔴 SELL</div></div>
        <div class="cnt ign"><div class="num">{sum(1 for d in sp_data if d['hasIgnition'])}</div><div class="lbl">🔥 Ignition</div></div>
        <div class="cnt radar"><div class="num">{sum(1 for d in sp_data if d['radarScore'] >= 3)}</div><div class="lbl">📡 Radar</div></div>
        <div class="cnt stat-blue"><div class="num">{ab_a}</div><div class="lbl">🅰️ A/B – A</div></div>
        <div class="cnt stat-green"><div class="num">{ab_b}</div><div class="lbl">🅱️ A/B – B</div></div>
        <div class="cnt stat-orange"><div class="num">{ab_sp}</div><div class="lbl">⚖️ A/B – Split</div></div>
        <div class="cnt sell"><div class="num">{ab_no}</div><div class="lbl">❌ A/B – None</div></div>
        <div class="cnt stat-green"><div class="num">{fz_sb + fz_mb}</div><div class="lbl">🧠 Fuzzy Buy</div></div>
        <div class="cnt sell"><div class="num">{fz_av}</div><div class="lbl">🧠 Fuzzy Avoid</div></div>
        <div class="cnt sell"><div class="num">{sp_strong}</div><div class="lbl">🔥 SME Strong</div></div>
        <div class="cnt stat-green"><div class="num">{sp_core}</div><div class="lbl">✅ SME Core</div></div>
      </div>
    </div>
    <div class="filters">
      <div class="f wide"><label>🔍 স্টকের নাম</label><input type="text" id="sp_fSearch" placeholder="যেমন: BATBC" oninput="applyFiltersSP()"></div>
      <div class="f"><label>📈 Canvas Signal</label><select id="sp_fCanvas" onchange="applyFiltersSP()">
        <option value="all">সব</option><option value="BUY">🟢 BUY</option><option value="HOLD">🟡 HOLD</option><option value="WAIT">⏳ WAIT</option><option value="SELL">🔴 SELL</option></select></div>
      <div class="f"><label>🏷️ Verdict</label><select id="sp_fVerdict" onchange="applyFiltersSP()">
        <option value="all">সব</option><option value="GREEN">GREEN</option><option value="YELLOW">YELLOW</option><option value="RED">RED</option></select></div>
      <div class="f"><label>📊 A/B Variant</label><select id="sp_fABVariant" onchange="applyFiltersSP()">
        <option value="all">সব</option><option value="A">🅰️ A</option><option value="B">🅱️ B</option><option value="SPLIT">⚖️ SPLIT</option><option value="NONE">❌ NONE</option></select></div>
      <div class="f"><label>🧠 Fuzzy Action</label><select id="sp_fFuzzyAction" onchange="applyFiltersSP()">
        <option value="all">সব</option><option value="Strong Buy">🟢🟢 Strong Buy</option><option value="Mild Buy">🟢 Mild Buy</option><option value="Neutral / Watch">⏳ Neutral</option><option value="Avoid">🔴 Avoid</option></select></div>
      <div class="f"><label>🚀 SME Catcher</label><select id="sp_fSMECatcher" onchange="applyFiltersSP()">
        <option value="all">সব</option><option value="STRONG">🔥 শক্তিশালী</option><option value="CORE">✅ মূল</option><option value="NONE">⏭️ এড়িয়ে যান</option></select></div>
      <div class="f"><label>🎯 SME Entry</label><select id="sp_fSMEEntry" onchange="applyFiltersSP()">
        <option value="all">সব</option><option value="Breakout">🚀 ব্রেকআউট</option><option value="Pullback">📉 পুলব্যাক</option><option value="Continuation">➡️ কন্টিনিউয়েশন</option><option value="None">⏭️ নেই</option></select></div>
      <div class="f"><label>📡 Radar</label><select id="sp_fRadar" onchange="applyFiltersSP()">
        <option value="all">সব</option><option value="yes">Radar ≥ 3</option><option value="ignition">🔥 VOL_IGNITION</option><option value="no">Radar &lt; 3</option></select></div>
      <div class="f"><label>🥾 Hike Tier</label><select id="sp_fHikeTier" onchange="applyFiltersSP()">
        <option value="all">সব</option><option value="NONE">⚪ NONE</option><option value="EARLY_SETUP">🟢 EARLY_SETUP</option><option value="MID_HIKE">🟡 MID_HIKE</option><option value="LATE_HIKE">🟠 LATE_HIKE</option><option value="EXHAUSTION_WATCH">🔴 EXHAUSTION_WATCH</option></select></div>
      <div class="f narrow"><label>🎯 Final Score ≥</label><input type="number" id="sp_fFinalScore" placeholder="0.00" step="0.05" min="0" max="1" oninput="applyFiltersSP()"></div>
      <div class="f narrow"><label>🧠 Fuzzy Final ≥</label><input type="number" id="sp_fFuzzyMin" placeholder="-1..1" step="0.05" min="-1" max="1" oninput="applyFiltersSP()"></div>
      <div class="f narrow"><label>📊 RPD ≥</label><input type="number" id="sp_fRpdMin" placeholder="0.00" step="0.01" min="0" oninput="applyFiltersSP()"></div>
      <div class="f narrow"><label>📈 L_score ≥</label><input type="number" id="sp_fLScoreMin" placeholder="0.00" step="0.05" min="0" max="1" oninput="applyFiltersSP()"></div>
      <div class="f narrow"><label>💰 Position % ≥</label><input type="number" id="sp_fPosMin" placeholder="0" step="5" min="0" max="100" oninput="applyFiltersSP()"></div>
      <button class="btn ghost" onclick="resetFiltersSP()">↺ রিসেট</button>
      <div class="show-count">দেখানো হচ্ছে <b id="sp_shown">0</b> / <span id="sp_total">{sp_total}</span> স্টক</div>
    </div>
    <div id="swingParamGrid" class="grid wide"></div>
  </div>''')

    parts.append(f'''
  <div id="tab-hike" class="tab-content">
    <div class="sum-head" style="background:linear-gradient(135deg,#fef2f2,#fee2e2);border-left:5px solid #dc2626;">
      <h2>🥾 Hike-Tier Engine — v1.31</h2>
      <div class="sub">Extension = (close / SMA_20 − 1) × 100 &nbsp;|&nbsp; মোট: <b>{hike_total}</b></div>
      <div class="counts">
        <div class="cnt tier-none"><div class="num">{hike_counts.get(TIER_NONE,0)}</div><div class="lbl">⚪ NONE</div></div>
        <div class="cnt tier-early"><div class="num">{hike_counts.get(TIER_EARLY,0)}</div><div class="lbl">🟢 EARLY</div></div>
        <div class="cnt tier-mid"><div class="num">{hike_counts.get(TIER_MID,0)}</div><div class="lbl">🟡 MID</div></div>
        <div class="cnt tier-late"><div class="num">{hike_counts.get(TIER_LATE,0)}</div><div class="lbl">🟠 LATE</div></div>
        <div class="cnt tier-exhaust"><div class="num">{hike_counts.get(TIER_EXHAUSTION,0)}</div><div class="lbl">🔴 EXHAUST</div></div>
      </div>
    </div>
    <div class="filters">
      <div class="f wide"><label>🔍 স্টকের নাম</label><input type="text" id="hk_fSearch" placeholder="যেমন: SAPORTL" oninput="applyFiltersHike()"></div>
      <div class="f"><label>🥾 Hike Tier</label><select id="hk_fTier" onchange="applyFiltersHike()">
        <option value="all">সব</option><option value="NONE">⚪ NONE</option><option value="EARLY_SETUP">🟢 EARLY_SETUP</option><option value="MID_HIKE">🟡 MID_HIKE</option><option value="LATE_HIKE">🟠 LATE_HIKE</option><option value="EXHAUSTION_WATCH">🔴 EXHAUSTION_WATCH</option></select></div>
      <div class="f"><label>📈 Final Signal</label><select id="hk_fSignal" onchange="applyFiltersHike()">
        <option value="all">সব</option><option value="BUY">🟢 BUY</option><option value="HOLD">🟡 HOLD</option><option value="WAIT">⏳ WAIT</option><option value="SELL">🔴 SELL</option></select></div>
      <div class="f narrow"><label>📊 Extension % ≥</label><input type="number" id="hk_fExtMin" placeholder="-10" step="0.5" oninput="applyFiltersHike()"></div>
      <div class="f narrow"><label>📉 Vol-Div Days ≥</label><input type="number" id="hk_fVdMin" placeholder="0" step="1" min="0" max="10" oninput="applyFiltersHike()"></div>
      <div class="f"><label>✅ Entry Allowed</label><select id="hk_fAllowed" onchange="applyFiltersHike()">
        <option value="all">সব</option><option value="yes">✅ হ্যাঁ</option><option value="no">⛔ না</option></select></div>
      <button class="btn ghost" onclick="resetFiltersHike()">↺ রিসেট</button>
      <div class="show-count">দেখানো হচ্ছে <b id="hk_shown">0</b> / <span id="hk_total">{hike_total}</span> স্টক</div>
    </div>
    <div id="hikeGrid" class="grid wide"></div>
  </div>''')

    parts.append(f'''
  <div id="tab-summary" class="tab-content">
    <div class="sum-head">
      <h2>📄 সব স্টকের সহজ বাংলা রিপোর্ট</h2>
      <div class="sub">মোট: <b>{s_total}</b></div>
      <div class="counts">
        <div class="cnt buy"><div class="num">{s_buy}</div><div class="lbl">🟢 কেনার যোগ্য</div></div>
        <div class="cnt hold"><div class="num">{s_hold}</div><div class="lbl">🟡 হাতে রাখুন</div></div>
        <div class="cnt wait"><div class="num">{s_wait}</div><div class="lbl">⏳ অপেক্ষা করুন</div></div>
        <div class="cnt sell"><div class="num">{s_sell}</div><div class="lbl">🔴 বিক্রি</div></div>
      </div>
    </div>
    <div class="filters">
      <div class="f wide"><label>🔍 স্টকের নাম</label><input type="text" id="s_fSearch" placeholder="যেমন: BATBC" oninput="applyFiltersSummary()"></div>
      <div class="f"><label>📌 কী করবেন</label><select id="s_fRec" onchange="applyFiltersSummary()">
        <option value="all">সব</option><option value="BUY">🟢 কেনার যোগ্য</option><option value="HOLD">🟡 হাতে রাখুন</option><option value="WAIT">⏳ অপেক্ষা করুন</option><option value="SELL">🔴 বিক্রি</option></select></div>
      <div class="f"><label>📡 Radar</label><select id="s_fRadar" onchange="applyFiltersSummary()">
        <option value="all">সব</option><option value="yes">৩ বা বেশি</option><option value="hot">🔥 Ignition</option><option value="no">৩ এর কম</option></select></div>
      <div class="f"><label>🏷️ Verdict</label><select id="s_fVerdict" onchange="applyFiltersSummary()">
        <option value="all">সব</option><option value="GREEN">GREEN</option><option value="YELLOW">YELLOW</option><option value="RED">RED</option></select></div>
      <div class="f"><label>🧠 Fuzzy</label><select id="s_fFuzzy" onchange="applyFiltersSummary()">
        <option value="all">সব</option><option value="Strong Buy">🟢🟢 Strong Buy</option><option value="Mild Buy">🟢 Mild Buy</option><option value="Neutral / Watch">⏳ Neutral</option><option value="Avoid">🔴 Avoid</option></select></div>
      <div class="f"><label>🥾 Hike Tier</label><select id="s_fHikeTier" onchange="applyFiltersSummary()">
        <option value="all">সব</option><option value="NONE">⚪ NONE</option><option value="EARLY_SETUP">🟢 EARLY_SETUP</option><option value="MID_HIKE">🟡 MID_HIKE</option><option value="LATE_HIKE">🟠 LATE_HIKE</option><option value="EXHAUSTION_WATCH">🔴 EXHAUSTION_WATCH</option></select></div>
      <button class="btn ghost" onclick="resetFiltersSummary()">↺ রিসেট</button>
      <div class="show-count">দেখানো হচ্ছে <b id="s_shown">0</b> / <span id="s_total">{s_total}</span> স্টক</div>
    </div>
    <div id="summaryGrid" class="grid"></div>
  </div>''')

    parts.append(f'''
  <div id="tab-ab" class="tab-content">
    <div class="sum-head">
      <h2>📊 Swing A/B Testing MCMC (v1.31, Risk 1.2%)</h2>
      <div class="counts">
        <div class="cnt stat-blue"><div class="num">{ab_total}</div><div class="lbl">মোট</div></div>
        <div class="cnt stat-blue"><div class="num">{ab_a}</div><div class="lbl">🅰️ মোমেন্টাম</div></div>
        <div class="cnt stat-green"><div class="num">{ab_b}</div><div class="lbl">🅱️ ভ্যালু</div></div>
        <div class="cnt stat-orange"><div class="num">{ab_sp}</div><div class="lbl">⚖️ ৫০/৫০</div></div>
        <div class="cnt sell"><div class="num">{ab_no}</div><div class="lbl">❌ ট্রেড নেই</div></div>
        <div class="cnt stat-purple"><div class="num">{ab_tradeable}</div><div class="lbl">✅ ট্রেডযোগ্য</div></div>
      </div>
    </div>
    <div class="filters">
      <div class="f wide"><label>🔍 স্টকের নাম</label><input type="text" id="ab_fSearch" placeholder="যেমন: BATBC" oninput="applyFiltersAB()"></div>
      <div class="f"><label>📌 কৌশল</label><select id="ab_fVariant" onchange="applyFiltersAB()">
        <option value="all">সব</option><option value="A">🅰️ মোমেন্টাম</option><option value="B">🅱️ ভ্যালু</option><option value="SPLIT">⚖️ ৫০/৫০</option><option value="NONE">❌ ট্রেড নেই</option></select></div>
      <div class="f"><label>📊 Product Score ≥</label><input type="number" id="ab_fProductMin" placeholder="০.০০" step="0.05" min="0" max="1" oninput="applyFiltersAB()"></div>
      <div class="f"><label>📈 RPD ≥</label><input type="number" id="ab_fRpdMin" placeholder="০.০০" step="0.01" min="0" oninput="applyFiltersAB()"></div>
      <button class="btn ghost" onclick="resetFiltersAB()">↺ রিসেট</button>
      <div class="show-count">দেখানো হচ্ছে <b id="ab_shown">0</b> / <span id="ab_total">{ab_total}</span> স্টক</div>
    </div>
    <div id="abGrid" class="grid"></div>
  </div>''')

    parts.append(f'''
  <div id="tab-fuzzy" class="tab-content">
    <div class="sum-head">
      <h2>🧠 ফাজি লজিক বাংলা রিপোর্ট — v1.31</h2>
      <div class="counts">
        <div class="cnt"><div class="num">{fz_total}</div><div class="lbl">মোট</div></div>
        <div class="cnt stat-green"><div class="num">{fz_sb}</div><div class="lbl">🟢🟢 দৃঢ় ক্রয়</div></div>
        <div class="cnt stat-green"><div class="num">{fz_mb}</div><div class="lbl">🟢 মৃদু ক্রয়</div></div>
        <div class="cnt wait"><div class="num">{fz_n}</div><div class="lbl">⏳ নিরপেক্ষ</div></div>
        <div class="cnt sell"><div class="num">{fz_av}</div><div class="lbl">🔴 এড়িয়ে যান</div></div>
      </div>
    </div>
    <div class="filters">
      <div class="f wide"><label>🔍 স্টকের নাম</label><input type="text" id="fz_fSearch" placeholder="যেমন: BATBC" oninput="applyFiltersFuzzy()"></div>
      <div class="f"><label>📌 সংকেত</label><select id="fz_fAction" onchange="applyFiltersFuzzy()">
        <option value="all">সব</option><option value="Strong Buy">🟢🟢 দৃঢ় ক্রয়</option><option value="Mild Buy">🟢 মৃদু ক্রয়</option><option value="Neutral / Watch">⏳ নিরপেক্ষ</option><option value="Avoid">🔴 এড়িয়ে যান</option></select></div>
      <div class="f"><label>🥾 Hike Tier</label><select id="fz_fHikeTier" onchange="applyFiltersFuzzy()">
        <option value="all">সব</option><option value="NONE">⚪ NONE</option><option value="EARLY_SETUP">🟢 EARLY_SETUP</option><option value="MID_HIKE">🟡 MID_HIKE</option><option value="LATE_HIKE">🟠 LATE_HIKE</option><option value="EXHAUSTION_WATCH">🔴 EXHAUSTION_WATCH</option></select></div>
      <div class="f"><label>📊 Score ≥</label><input type="number" id="fz_fScoreMin" placeholder="-1..1" step="0.05" min="-1" max="1" oninput="applyFiltersFuzzy()"></div>
      <div class="f"><label>📈 Product Score ≥</label><input type="number" id="fz_fProductMin" placeholder="০.০০" step="0.05" min="0" max="1" oninput="applyFiltersFuzzy()"></div>
      <button class="btn ghost" onclick="resetFiltersFuzzy()">↺ রিসেট</button>
      <div class="show-count">দেখানো হচ্ছে <b id="fz_shown">0</b> / <span id="fz_total">{fz_total}</span> স্টক</div>
    </div>
    <div id="fuzzyGrid" class="grid"></div>
  </div>''')

    parts.append(f'''
  <div id="tab-sme" class="tab-content">
    <div class="sum-head">
      <h2>🚀 Swing Momentum Engine (v1.31)</h2>
      <div class="counts">
        <div class="cnt"><div class="num">{sme_total}</div><div class="lbl">মোট</div></div>
        <div class="cnt sell"><div class="num">{sme_strong}</div><div class="lbl">🔥 শক্তিশালী</div></div>
        <div class="cnt stat-green"><div class="num">{sme_core}</div><div class="lbl">✅ মূল</div></div>
        <div class="cnt wait"><div class="num">{sme_none}</div><div class="lbl">⏭️ এড়িয়ে যান</div></div>
        <div class="cnt stat-warn"><div class="num">{sme_mcap_missing}</div><div class="lbl">⚠️ MCap নেই</div></div>
        <div class="cnt stat-blue"><div class="num">{sme_breakout}</div><div class="lbl">🚀 ব্রেকআউট</div></div>
        <div class="cnt stat-purple"><div class="num">{sme_pullback}</div><div class="lbl">📉 পুলব্যাক</div></div>
        <div class="cnt stat-orange"><div class="num">{sme_cont}</div><div class="lbl">➡️ কন্টিনিউয়েশন</div></div>
      </div>
    </div>
    <div class="filters">
      <div class="f wide"><label>🔍 স্টকের নাম</label><input type="text" id="sme_fSearch" placeholder="যেমন: BATBC" oninput="applyFiltersSME()"></div>
      <div class="f"><label>📌 সন্ধান স্তর</label><select id="sme_fCatcher" onchange="applyFiltersSME()">
        <option value="all">সব</option><option value="STRONG">🔥 শক্তিশালী</option><option value="CORE">✅ মূল</option><option value="NONE">⏭️ এড়িয়ে যান</option></select></div>
      <div class="f"><label>📌 প্রবেশ</label><select id="sme_fEntry" onchange="applyFiltersSME()">
        <option value="all">সব</option><option value="Breakout">🚀 ব্রেকআউট</option><option value="Pullback">📉 পুলব্যাক</option><option value="Continuation">➡️ কন্টিনিউয়েশন</option><option value="None">⏭️ নেই</option></select></div>
      <div class="f"><label>📊 L_score ≥</label><input type="number" id="sme_fLMin" placeholder="০.০০" step="0.05" min="0" max="1" oninput="applyFiltersSME()"></div>
      <div class="f"><label>📈 Product Score ≥</label><input type="number" id="sme_fProductMin" placeholder="০.০০" step="0.05" min="0" max="1" oninput="applyFiltersSME()"></div>
      <div class="f"><label>⚠️ Market Cap</label><select id="sme_fMcap" onchange="applyFiltersSME()">
        <option value="all">সব</option><option value="missing">ডেটা নেই</option><option value="ok">ডেটা আছে</option></select></div>
      <button class="btn ghost" onclick="resetFiltersSME()">↺ রিসেট</button>
      <div class="show-count">দেখানো হচ্ছে <b id="sme_shown">0</b> / <span id="sme_total">{sme_total}</span> স্টক</div>
    </div>
    <div id="smeGrid" class="grid"></div>
  </div>''')

    parts.append(f'''
  <div class="foot">
    DSE Canvas Scanner • ৬টি ট্যাব • v1.31 • {dts}<br>
    ⚠️ এই রিপোর্ট শুধুমাত্র তথ্যসূত্র। বিনিয়োগের আগে নিজে যাচাই করুন।
  </div>
</div>
<button class="print-btn" onclick="window.print()" title="PDF হিসেবে সংরক্ষণ করুন">📥</button>''')

    parts.append('<script>')
    parts.append('''
function switchTab(name) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  var tab = document.getElementById('tab-' + name);
  if (tab) tab.classList.add('active');
  document.querySelectorAll('.tab-btn').forEach(b => {
    var oc = b.getAttribute('onclick') || '';
    if (oc.indexOf("'" + name + "'") !== -1) b.classList.add('active');
  });
}
function esc(s) {
  return String(s).replace(/[&<>"']/g, function(c) {
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
  });
}
''')

    parts.append(f'const SP_DATA = {sp_json};')
    parts.append('''
function spCardHtml(d) {
  const radarCls = d.radarScore >= 5 ? 'hot' : '';
  const ignBadge = d.hasIgnition ? '<span style="background:#f97316;color:#fff;padding:2px 8px;border-radius:10px;font-size:10.5px;font-weight:700;margin-left:6px;">🔥</span>' : '';
  const vCls = (d.verdict || '').toLowerCase();
  const flagsHtml = (d.radarFlagLines && d.radarFlagLines.length) ? d.radarFlagLines.map(function(f){ return '<span class="sp-flag-chip">' + esc(f) + '</span>'; }).join('') : '<span style="font-size:11.5px;color:#64748b;">বিশেষ ইঙ্গিত নেই</span>';
  const fzReasonsHtml = (d.fuzzyReasons && d.fuzzyReasons.length) ? d.fuzzyReasons.map(function(r){ return '<div style="font-size:12px;color:#5b21b6;margin-top:2px;">• ' + esc(r) + '</div>'; }).join('') : '';
  const tierBadge = d.hikeTierBn ? '<span class="tier-pill ' + d.hikeTierCls + '">🥾 ' + esc(d.hikeTierBn) + '</span>' : '';
  const hikeSection = '<div class="sp-section hike"><h4><span>🥾 Hike-Tier</span>' + tierBadge + '</h4>' +
    '<div class="row"><span class="k">Extension %</span><span class="v ' + (d.extensionPct > 32 ? 'red' : d.extensionPct > 20 ? 'orange' : 'green') + '">' + d.extensionPct + '%</span></div>' +
    '<div class="row"><span class="k">SMA20</span><span class="v">' + d.sma20 + '৳</span></div>' +
    '<div class="row"><span class="k">Vol-Div Days</span><span class="v ' + (d.volDivDays >= 3 ? 'red' : d.volDivDays >= 2 ? 'orange' : '') + '">' + d.volDivDays + '</span></div>' +
    '<div class="row"><span class="k">Entry Allowed</span><span class="v ' + (d.tierEntryAllowed ? 'green' : 'red') + '">' + (d.tierEntryAllowed ? '✅ হ্যাঁ' : '⛔ না') + '</span></div></div>';
  return '<div class="sp-card"><div class="sp-card-top">' +
      '<div class="sp-sym">' + esc(d.symbol) + '<span class="radar-pill ' + radarCls + '">📡 ' + d.radarScore + '</span>' + ignBadge + '</div>' +
      '<div class="sp-price-row"><div class="pr">' + d.price + '<small>৳</small></div>' +
        '<div class="proj">১সপ্তাহ: <b>' + d.proj + '৳</b> (' + d.projArrow + ' ' + Math.abs(d.projDiff) + '%)</div></div></div>' +
    '<div class="sp-rec ' + d.recClass + '">' + esc(d.recText) + '</div><div class="sp-body">' +
      '<div class="sp-section"><h4><span>📄 সারসংক্ষেপ</span><span class="sp-verdict ' + vCls + '">' + d.verdict + '</span></h4>' +
        '<div class="row"><span class="k">Canvas</span><span class="v">' + d.canvasSignal + '</span></div>' +
        '<div class="row"><span class="k">Final Score</span><span class="v purple">' + d.finalScore + '</span></div>' +
        '<div class="row"><span class="k">কারণ</span><span class="v" style="font-size:11px;">' + esc(d.canvasReason || '') + '</span></div>' +
        '<div class="row"><span class="k">কেনার সীমা</span><span class="v green">' + d.buyZone + '৳</span></div>' +
        '<div class="row"><span class="k">লক্ষ্য</span><span class="v blue">' + d.target + '৳</span></div>' +
        '<div class="row"><span class="k">সতর্কতার সীমা</span><span class="v red">' + d.stop + '৳</span></div>' +
        '<div class="row"><span class="k">RSI / ADX</span><span class="v">' + d.rsi + ' / ' + d.adx + '</span></div>' +
        '<div class="row"><span class="k">aMQS / Asset</span><span class="v">' + d.aMQS + ' / ' + d.assetClass + '</span></div></div>' +
      hikeSection +
      '<div class="sp-section ab"><h4><span>📊 A/B Testing MCMC</span><span class="sp-badge ' + d.abVariantCls + '">' + esc(d.abVariantBn) + '</span></h4>' +
        '<div class="row"><span class="k">RPD (A / B)</span><span class="v">' + d.abRpdA + ' / ' + d.abRpdB + '</span></div>' +
        '<div class="row"><span class="k">প্রত্যাশিত RPD</span><span class="v blue">' + d.abExpectedRpd + '</span></div>' +
        '<div class="row"><span class="k">Position Size</span><span class="v">' + d.abPositionSize + ' শেয়ার</span></div>' +
        '<div class="row"><span class="k">Confidence</span><span class="v">' + d.abConfidence + '%</span></div></div>' +
      '<div class="sp-section fz"><h4><span>🧠 ফাজি লজিক v3</span><span class="sp-badge ' + d.fuzzyActionCls + '">' + esc(d.fuzzyAction) + '</span></h4>' +
        '<div class="row"><span class="k">M / P / C</span><span class="v">' + d.fuzzyM + ' / ' + d.fuzzyP + ' / ' + d.fuzzyC + '</span></div>' +
        '<div class="row"><span class="k">R / L</span><span class="v">' + d.fuzzyR + ' / ' + d.fuzzyL + '</span></div>' +
        '<div class="row"><span class="k">Gate × Rank</span><span class="v purple">' + d.fuzzySGate + ' × ' + d.fuzzySRank + '</span></div>' +
        '<div class="row"><span class="k">Size</span><span class="v">' + esc(d.fuzzySizeBn) + '</span></div>' +
        (fzReasonsHtml ? '<div style="margin-top:6px;">' + fzReasonsHtml + '</div>' : '') + '</div>' +
      '<div class="sp-section sme"><h4><span>🚀 Swing Momentum Engine</span><span class="sp-badge ' + d.smeCatcherCls + '">' + esc(d.smeCatcherBn) + '</span></h4>' +
        '<div class="row"><span class="k">Entry Type</span><span class="v">' + esc(d.smeEntryBn) + '</span></div>' +
        '<div class="row"><span class="k">L_score</span><span class="v green">' + d.smeLScore + ' (' + d.smeLScoreBn + ')</span></div>' +
        '<div class="row"><span class="k">Position %</span><span class="v orange">' + d.smePositionSizePct + '%</span></div>' +
        '<div class="row"><span class="k">Volume Ratio</span><span class="v">' + d.smeVolumeRatio + 'x</span></div></div>' +
      '<div class="sp-section" style="border-left-color:#0ea5e9; background:#f0f9ff;"><h4 style="color:#0369a1;">📡 RADAR</h4>' +
        '<div class="sp-flags">' + flagsHtml + '</div></div></div></div>';
}
function applyFiltersSP() {
  const s      = (document.getElementById('sp_fSearch').value || '').trim().toLowerCase();
  const canvas = document.getElementById('sp_fCanvas').value;
  const verdict= document.getElementById('sp_fVerdict').value;
  const abVar  = document.getElementById('sp_fABVariant').value;
  const fzAct  = document.getElementById('sp_fFuzzyAction').value;
  const smeCat = document.getElementById('sp_fSMECatcher').value;
  const smeEnt = document.getElementById('sp_fSMEEntry').value;
  const radar  = document.getElementById('sp_fRadar').value;
  const hikeT  = document.getElementById('sp_fHikeTier').value;
  const fsMin  = document.getElementById('sp_fFinalScore').value;
  const fzMin  = document.getElementById('sp_fFuzzyMin').value;
  const rpdMin = document.getElementById('sp_fRpdMin').value;
  const lMin   = document.getElementById('sp_fLScoreMin').value;
  const posMin = document.getElementById('sp_fPosMin').value;
  const rows = SP_DATA.filter(function(d) {
    if (s && d.symbol.toLowerCase().indexOf(s) === -1) return false;
    if (canvas !== 'all' && d.canvasSignal !== canvas) return false;
    if (verdict !== 'all' && d.verdict !== verdict) return false;
    if (abVar !== 'all' && d.abVariant !== abVar) return false;
    if (fzAct !== 'all' && d.fuzzyActionEn !== fzAct) return false;
    if (smeCat !== 'all' && d.smeCatcherLevel !== smeCat) return false;
    if (smeEnt !== 'all' && d.smeEntryType !== smeEnt) return false;
    if (hikeT !== 'all' && d.hikeTier !== hikeT) return false;
    const rs = d.radarScore || 0;
    if (radar === 'yes' && rs < 3) return false;
    if (radar === 'no' && rs >= 3) return false;
    if (radar === 'ignition' && !d.hasIgnition) return false;
    if (fsMin !== '' && (d.finalScore === null || d.finalScore === undefined || d.finalScore < parseFloat(fsMin))) return false;
    if (fzMin !== '' && (d.fuzzyS === null || d.fuzzyS === undefined || d.fuzzyS < parseFloat(fzMin))) return false;
    if (rpdMin !== '') { const rpd = Math.max(d.abRpdA || 0, d.abRpdB || 0); if (rpd < parseFloat(rpdMin)) return false; }
    if (lMin !== '' && (d.smeLScore === null || d.smeLScore === undefined || d.smeLScore < parseFloat(lMin))) return false;
    if (posMin !== '' && (d.smePositionSizePct === null || d.smePositionSizePct === undefined || d.smePositionSizePct < parseFloat(posMin))) return false;
    return true;
  });
  const rank = {'BUY':0,'HOLD':1,'WAIT':2,'SELL':3};
  rows.sort(function(a,b) {
    if (a.hasIgnition !== b.hasIgnition) return a.hasIgnition ? -1 : 1;
    if (b.radarScore !== a.radarScore) return b.radarScore - a.radarScore;
    if (b.finalScore !== a.finalScore) return b.finalScore - a.finalScore;
    return (rank[a.canvasSignal] === undefined ? 9 : rank[a.canvasSignal]) - (rank[b.canvasSignal] === undefined ? 9 : rank[b.canvasSignal]);
  });
  document.getElementById('sp_shown').textContent = rows.length;
  document.getElementById('sp_total').textContent = SP_DATA.length;
  document.getElementById('swingParamGrid').innerHTML = rows.length ? rows.map(spCardHtml).join('') : '<div class="empty"><div class="big">🔍</div>এই শর্তে কোনো স্টক পাওয়া যায়নি।</div>';
}
function resetFiltersSP() {
  ['sp_fSearch','sp_fFinalScore','sp_fFuzzyMin','sp_fRpdMin','sp_fLScoreMin','sp_fPosMin'].forEach(function(id){ document.getElementById(id).value = ''; });
  ['sp_fCanvas','sp_fVerdict','sp_fABVariant','sp_fFuzzyAction','sp_fSMECatcher','sp_fSMEEntry','sp_fRadar','sp_fHikeTier'].forEach(function(id){ document.getElementById(id).value = 'all'; });
  applyFiltersSP();
}
''')

    parts.append(f'const HIKE_DATA = {hike_json};')
    parts.append('''
function hikeCardHtml(d) {
  const tierCls = d.hikeTierCls || 'none';
  const extPos = Math.max(0, Math.min(100, (d.extensionPct + 5) / 50 * 100));
  const tierBn = d.hikeTierBn || '';
  const allowed = d.tierEntryAllowed ? '✅ হ্যাঁ' : '⛔ না';
  const allowedCls = d.tierEntryAllowed ? 'green' : 'red';
  const vdCls = d.volDivDays >= 3 ? 'red' : d.volDivDays >= 2 ? 'orange' : 'green';
  const extCls = d.extensionPct > 32 ? 'red' : d.extensionPct > 20 ? 'orange' : d.extensionPct >= 4 ? 'green' : 'blue';
  const exhaustStrip = d.hikeTier === 'EXHAUSTION_WATCH' ? '<div class="exhaust-strip">🔴 EXHAUSTION_WATCH — প্রবেশ করবেন না।</div>' : '';
  return '<div class="hike-card tier-' + tierCls + '"><div class="hike-card-top">' +
      '<div class="hike-sym">' + esc(d.symbol) + '</div>' +
      '<div class="hike-price">৳ ' + d.price + ' &nbsp;•&nbsp; SMA20: ৳ ' + d.sma20 + '</div></div>' +
    '<div class="hike-tier-badge ' + tierCls + '">' + esc(tierBn) + '</div>' + exhaustStrip + '<div class="hike-body">' +
      '<div class="hike-metrics">' +
        '<div class="hike-metric"><div class="lbl">Extension %</div><div class="val ' + extCls + '">' + d.extensionPct + '%</div></div>' +
        '<div class="hike-metric"><div class="lbl">Vol-Div Days</div><div class="val ' + vdCls + '">' + d.volDivDays + ' / ' + d.volDivTotal + '</div></div>' +
        '<div class="hike-metric"><div class="lbl">RSI</div><div class="val">' + d.rsi + '</div></div>' +
        '<div class="hike-metric"><div class="lbl">Entry Allowed</div><div class="val ' + allowedCls + '">' + allowed + '</div></div>' +
        '<div class="hike-metric"><div class="lbl">Final Signal</div><div class="val blue">' + esc(d.finalSignal) + '</div></div>' +
        '<div class="hike-metric"><div class="lbl">Final Score</div><div class="val">' + d.finalScore + '</div></div></div>' +
      '<div class="hike-ext-bar"><div class="hike-ext-marker" style="left:' + extPos.toFixed(1) + '%"></div></div>' +
      '<div style="font-size:11px;color:#64748b;display:flex;justify-content:space-between;"><span>−5%</span><span>4%</span><span>10%</span><span>20%</span><span>32%</span><span>45%</span></div>' +
      '<div style="margin-top:8px;font-size:11.5px;color:#475569;">কারণ: ' + esc(d.canvasReason || '') + '</div></div></div>';
}
function applyFiltersHike() {
  const s = (document.getElementById('hk_fSearch').value || '').trim().toLowerCase();
  const t = document.getElementById('hk_fTier').value;
  const sig = document.getElementById('hk_fSignal').value;
  const extMin = parseFloat(document.getElementById('hk_fExtMin').value);
  const vdMin = parseFloat(document.getElementById('hk_fVdMin').value);
  const allowed = document.getElementById('hk_fAllowed').value;
  const rows = HIKE_DATA.filter(function(d) {
    if (s && d.symbol.toLowerCase().indexOf(s) === -1) return false;
    if (t !== 'all' && d.hikeTier !== t) return false;
    if (sig !== 'all' && d.finalSignal !== sig) return false;
    if (!isNaN(extMin) && d.extensionPct < extMin) return false;
    if (!isNaN(vdMin) && d.volDivDays < vdMin) return false;
    if (allowed === 'yes' && !d.tierEntryAllowed) return false;
    if (allowed === 'no' && d.tierEntryAllowed) return false;
    return true;
  });
  const order = {'EARLY_SETUP':0,'MID_HIKE':1,'NONE':2,'LATE_HIKE':3,'EXHAUSTION_WATCH':4};
  rows.sort(function(a,b){
    var oa = order[a.hikeTier] === undefined ? 9 : order[a.hikeTier];
    var ob = order[b.hikeTier] === undefined ? 9 : order[b.hikeTier];
    if (oa !== ob) return oa - ob;
    return (b.finalScore || 0) - (a.finalScore || 0);
  });
  document.getElementById('hk_shown').textContent = rows.length;
  document.getElementById('hk_total').textContent = HIKE_DATA.length;
  document.getElementById('hikeGrid').innerHTML = rows.length ? rows.map(hikeCardHtml).join('') : '<div class="empty"><div class="big">🔍</div>এই শর্তে কোনো স্টক পাওয়া যায়নি।</div>';
}
function resetFiltersHike() {
  ['hk_fSearch','hk_fExtMin','hk_fVdMin'].forEach(function(id){ document.getElementById(id).value = ''; });
  ['hk_fTier','hk_fSignal','hk_fAllowed'].forEach(function(id){ document.getElementById(id).value = 'all'; });
  applyFiltersHike();
}
''')

    parts.append(f'const SUMMARY_DATA = {summary_json};')
    parts.append('''
function summaryCardHtml(d) {
  const radarCls = d.radarScore >= 5 ? 'hot' : '';
  const reasons = d.reasons.map(function(r){ return '<li>' + esc(r) + '</li>'; }).join('');
  const flags = d.flags.map(function(f){ return '<span class="flag-chip">' + esc(f) + '</span>'; }).join('');
  const ignStrip = d.hasIgnition ? '<div class="ignition-strip">🔥 হঠাৎ লেনদেন অনেক বেড়েছে।</div>' : '';
  const exhaustStrip = (d.hikeTier === 'EXHAUSTION_WATCH') ? '<div class="exhaust-strip">🔴 EXHAUSTION_WATCH — প্রবেশ করবেন না।</div>' : '';
  const fuzzyStripCls = (d.fuzzyColor || 'wait');
  const fuzzyStrip = '<div class="fuzzy-strip ' + fuzzyStripCls + '">🧠 ফাজি v3: <b>' + esc(d.fuzzyAction) + '</b> | স্কোর: <b>' + d.fuzzyFinal + '</b> | Final: <b>' + d.finalScore + '</b></div>';
  const tierPill = d.hikeTierBn ? '<span class="tier-pill ' + d.hikeTierCls + '">🥾 ' + esc(d.hikeTierBn) + '</span>' : '';
  return '<div class="card"><div class="sum-card-top">' +
      '<div class="sum-sym">' + esc(d.symbol) + '<span class="radar-pill ' + radarCls + '">📡 ' + d.radarScore + '</span>' + tierPill + '</div>' +
      '<div class="price-row"><div class="pr">' + d.price + '<small>৳</small></div>' +
        '<div class="proj">১সপ্তাহ: <b>' + d.proj + ' ৳</b> (' + d.projArrow + ' ' + Math.abs(d.projDiff) + '%)</div></div></div>' +
    '<div class="rec ' + d.recClass + '">' + esc(d.recText) + '</div>' + fuzzyStrip + ignStrip + exhaustStrip + '<div class="sum-body">' +
      '<div class="block"><h3>📌 এখন কী ঘটছে</h3><p>' + esc(d.trend) + '।</p>' +
        '<p style="margin-top:4px;">দামের গতি: <b>' + esc(d.rsiText) + '</b>।</p>' +
        '<p style="margin-top:4px;">🥾 হাইক: <b>' + esc(d.hikeTierBn) + '</b> (ext ' + d.extensionPct + '%)।</p></div>' +
      '<div class="block"><h3>🔍 কারণ</h3><ul>' + reasons + '</ul></div>' +
      '<div class="block"><h3>📡 ইঙ্গিত</h3><div class="flags-inline">' + (flags || '<span style="font-size:13.5px;color:#64748b;">বিশেষ কিছু নেই।</span>') + '</div></div>' +
      '<div class="block"><h3>💰 দামের সীমা</h3><div class="levels">' +
          '<div class="level buy"><div class="lbl">কেনার আদর্শ</div><div class="val">' + d.buyZone + '<small>৳</small></div></div>' +
          '<div class="level tgt"><div class="lbl">লক্ষ্য</div><div class="val">' + d.target + '<small>৳</small></div></div>' +
          '<div class="level stop"><div class="lbl">সতর্কতার সীমা</div><div class="val">' + d.stop + '<small>৳</small></div></div></div></div></div></div>';
}
function applyFiltersSummary() {
  const s   = (document.getElementById('s_fSearch').value || '').trim().toLowerCase();
  const rec = document.getElementById('s_fRec').value;
  const rad = document.getElementById('s_fRadar').value;
  const vd  = document.getElementById('s_fVerdict').value;
  const fz  = document.getElementById('s_fFuzzy').value;
  const hk  = document.getElementById('s_fHikeTier').value;
  const rows = SUMMARY_DATA.filter(function(d) {
    if (s && d.symbol.toLowerCase().indexOf(s) === -1) return false;
    if (rec !== 'all' && d.canvasSignal !== rec) return false;
    if (vd !== 'all' && d.verdict !== vd) return false;
    if (rad === 'yes' && d.radarScore < 3) return false;
    if (rad === 'no' && d.radarScore >= 3) return false;
    if (rad === 'hot' && !d.hasIgnition) return false;
    if (fz !== 'all' && d.fuzzyActionEn !== fz) return false;
    if (hk !== 'all' && d.hikeTier !== hk) return false;
    return true;
  });
  const rank = {'BUY':0,'HOLD':1,'WAIT':2,'SELL':3};
  rows.sort(function(a,b) {
    if (a.hasIgnition !== b.hasIgnition) return a.hasIgnition ? -1 : 1;
    if (b.radarScore !== a.radarScore) return b.radarScore - a.radarScore;
    if (b.finalScore !== a.finalScore) return b.finalScore - a.finalScore;
    return (rank[a.canvasSignal] === undefined ? 9 : rank[a.canvasSignal]) - (rank[b.canvasSignal] === undefined ? 9 : rank[b.canvasSignal]);
  });
  document.getElementById('s_shown').textContent = rows.length;
  document.getElementById('s_total').textContent = SUMMARY_DATA.length;
  document.getElementById('summaryGrid').innerHTML = rows.length ? rows.map(summaryCardHtml).join('') : '<div class="empty"><div class="big">🔍</div>এই শর্তে কোনো স্টক পাওয়া যায়নি।</div>';
}
function resetFiltersSummary() {
  document.getElementById('s_fSearch').value = '';
  ['s_fRec','s_fRadar','s_fVerdict','s_fFuzzy','s_fHikeTier'].forEach(function(id){ document.getElementById(id).value = 'all'; });
  applyFiltersSummary();
}
''')

    parts.append(f'const AB_DATA = {ab_json};')
    parts.append('''
function abCardHtml(d) {
  const maxRpd = Math.max(d.rpdA, d.rpdB, 0.001);
  const barAWidth = (d.rpdA / maxRpd * 100).toFixed(1);
  const barBWidth = (d.rpdB / maxRpd * 100).toFixed(1);
  const actionClass = d.variant === 'NONE' ? 'none' : '';
  const showTradePlan = d.positionSize > 0;
  return '<div class="card"><div class="ab-card-header">' +
      '<div class="sym">' + esc(d.symbol) + '</div>' +
      '<div class="price">৳ ' + d.price + '</div>' +
      '<span class="ab-variant ' + d.variantCls + '">' + esc(d.variantBn) + '</span></div>' +
    '<div class="sum-body"><div class="ab-action ' + actionClass + '">' + esc(d.actionBn) + '</div>' +
      '<div class="rpd-compare"><h4>📊 RPD তুলনা</h4>' +
        '<div class="rpd-bar"><span class="label">🅰️</span><div class="bar"><div class="fill" style="width:' + barAWidth + '%"></div></div><span class="value">' + d.rpdA + '</span></div>' +
        '<div class="rpd-bar"><span class="label">🅱️</span><div class="bar"><div class="fill b" style="width:' + barBWidth + '%"></div></div><span class="value">' + d.rpdB + '</span></div></div>' +
      '<div class="metrics"><div class="metric"><div class="lbl">প্রত্যাশিত RPD</div><div class="val blue">' + d.expectedRpd + '</div></div>' +
        '<div class="metric"><div class="lbl">আত্মবিশ্বাস</div><div class="val">' + d.confidence + '%</div></div></div>' +
      (showTradePlan ?
        '<div class="trade-plan"><h4>📋 ট্রেড প্ল্যান</h4>' +
          '<div class="row"><span class="k">প্রবেশ মূল্য</span><span class="v">৳ ' + d.entryPrice + '</span></div>' +
          '<div class="row"><span class="k">স্টপ লস</span><span class="v red">৳ ' + d.stopLoss + '</span></div>' +
          '<div class="row"><span class="k">টেক প্রফিট</span><span class="v green">৳ ' + d.takeProfit + '</span></div>' +
          '<div class="row"><span class="k">শেয়ার সংখ্যা</span><span class="v">' + d.positionSize + '</span></div>' +
          '<div class="row"><span class="k">সর্বোচ্চ ঝুঁকি (১.২%)</span><span class="v red">৳ ' + d.maxLoss + '</span></div>' +
          '<div class="row"><span class="k">সর্বোচ্চ দিন</span><span class="v">' + d.maxHoldingDays + ' দিন</span></div></div>' : '') +
      '<div class="product-score"><span class="lbl">📊 Product Score</span><span class="val">' + d.productScore + '</span></div></div></div>';
}
function applyFiltersAB() {
  const s = (document.getElementById('ab_fSearch').value || '').trim().toLowerCase();
  const v = document.getElementById('ab_fVariant').value;
  const pMin = parseFloat(document.getElementById('ab_fProductMin').value) || 0;
  const rMin = parseFloat(document.getElementById('ab_fRpdMin').value) || 0;
  const rows = AB_DATA.filter(function(d) {
    if (s && d.symbol.toLowerCase().indexOf(s) === -1) return false;
    if (v !== 'all' && d.variant !== v) return false;
    if (d.productScore < pMin) return false;
    if (d.expectedRpd < rMin) return false;
    return true;
  });
  rows.sort(function(a,b){ return b.productScore - a.productScore; });
  document.getElementById('ab_shown').textContent = rows.length;
  document.getElementById('ab_total').textContent = AB_DATA.length;
  document.getElementById('abGrid').innerHTML = rows.length ? rows.map(abCardHtml).join('') : '<div class="empty"><div class="big">🔍</div>এই শর্তে কোনো স্টক পাওয়া যায়নি।</div>';
}
function resetFiltersAB() {
  document.getElementById('ab_fSearch').value = '';
  document.getElementById('ab_fVariant').value = 'all';
  document.getElementById('ab_fProductMin').value = '';
  document.getElementById('ab_fRpdMin').value = '';
  applyFiltersAB();
}
''')

    parts.append(f'const FUZZY_DATA = {fuzzy_json};')
    parts.append('''
function fuzzyCardHtml(d) {
  const mPct = (d.M * 100).toFixed(0); const pPct = (d.P * 100).toFixed(0);
  const cPct = (d.C * 100).toFixed(0); const rPct = (d.R * 100).toFixed(0);
  const lPct = (d.L * 100).toFixed(0); const crPct = (d.C_raw * 100).toFixed(0);
  const reasonsHtml = d.reasons.map(function(r){ return '<li>' + esc(r) + '</li>'; }).join('');
  const gateNote = d.hard_gate_pass ? '' : '<div style="background:#fef2f2;color:#991b1b;border-radius:10px;padding:8px 12px;font-size:12px;margin-top:8px;">⛔ ' + esc(d.skip_reason) + '</div>';
  return '<div class="card"><div class="fz-card-header"><span class="sym">' + esc(d.symbol) + '</span><span class="price">৳ ' + d.price + '</span></div>' +
    '<div class="action-badge ' + d.actionCls + '">' + esc(d.actionBn) + '</div><div class="sum-body">' +
      '<div class="final-score"><span class="lbl">Rank Score</span><span class="val">' + d.S_rank + '</span></div>' +
      '<div class="score-section"><h4>📊 ৫টি স্তর</h4>' +
        '<div class="score-row"><span class="label">M</span><div class="bar"><div class="fill" style="width:' + mPct + '%"></div></div><span class="value">' + d.M + '</span></div>' +
        '<div class="score-row"><span class="label">P</span><div class="bar"><div class="fill" style="width:' + pPct + '%"></div></div><span class="value">' + d.P + '</span></div>' +
        '<div class="score-row"><span class="label">C</span><div class="bar"><div class="fill green" style="width:' + cPct + '%"></div></div><span class="value">' + d.C + '</span></div>' +
        '<div class="score-row"><span class="label">C_raw</span><div class="bar"><div class="fill" style="width:' + crPct + '%"></div></div><span class="value">' + d.C_raw + '</span></div>' +
        '<div class="score-row"><span class="label">R</span><div class="bar"><div class="fill blue" style="width:' + rPct + '%"></div></div><span class="value">' + d.R + '</span></div>' +
        '<div class="score-row"><span class="label">L</span><div class="bar"><div class="fill" style="width:' + lPct + '%"></div></div><span class="value">' + d.L + '</span></div></div>' +
      '<div class="fz-reasons"><h4>🔍 কারণ</h4><ul>' + reasonsHtml + '</ul></div>' +
      '<div class="size-box"><span class="lbl">💰 বিনিয়োগ</span><span class="val">' + esc(d.sizeBn) + '</span></div>' +
      '<div class="params">' +
        '<div class="param"><div class="lbl">Gate</div><div class="val">' + d.S_gate + '</div></div>' +
        '<div class="param"><div class="lbl">Rank</div><div class="val">' + d.S_rank + '</div></div>' +
        '<div class="param"><div class="lbl">TV</div><div class="val">' + d.turnover_velocity + '</div></div>' +
        '<div class="param"><div class="lbl">t</div><div class="val">' + d.t + '</div></div>' +
        '<div class="param"><div class="lbl">N</div><div class="val">' + d.N + '</div></div>' +
        '<div class="param"><div class="lbl">Th</div><div class="val">' + d.th_strong + '/' + d.th_mild + '/' + d.th_watch + '</div></div></div>' +
      gateNote + '</div></div>';
}
function applyFiltersFuzzy() {
  const s = (document.getElementById('fz_fSearch').value || '').trim().toLowerCase();
  const a = document.getElementById('fz_fAction').value;
  const hk = document.getElementById('fz_fHikeTier').value;
  const sMin = parseFloat(document.getElementById('fz_fScoreMin').value) || -1;
  const pMin = parseFloat(document.getElementById('fz_fProductMin').value) || 0;
  const rows = FUZZY_DATA.filter(function(d) {
    if (s && d.symbol.toLowerCase().indexOf(s) === -1) return false;
    if (a !== 'all' && d.action !== a) return false;
    if (hk !== 'all' && d.hikeTier !== hk) return false;
    if (d.finalScore < sMin) return false;
    if (d.productScore < pMin) return false;
    return true;
  });
  rows.sort(function(a,b){ return b.productScore - a.productScore; });
  document.getElementById('fz_shown').textContent = rows.length;
  document.getElementById('fz_total').textContent = FUZZY_DATA.length;
  document.getElementById('fuzzyGrid').innerHTML = rows.length ? rows.map(fuzzyCardHtml).join('') : '<div class="empty"><div class="big">🔍</div>এই শর্তে কোনো স্টক পাওয়া যায়নি।</div>';
}
function resetFiltersFuzzy() {
  document.getElementById('fz_fSearch').value = '';
  document.getElementById('fz_fAction').value = 'all';
  document.getElementById('fz_fHikeTier').value = 'all';
  document.getElementById('fz_fScoreMin').value = '';
  document.getElementById('fz_fProductMin').value = '';
  applyFiltersFuzzy();
}
''')

    parts.append(f'const SME_DATA = {sme_json};')
    parts.append('''
function smeCardHtml(d) {
  const lPct = (d.lScore * 100).toFixed(0);
  const ema20Icon = d.ema20Rising ? '📈 ঊর্ধ্বমুখী' : '📉 নিম্নমুখী';
  const isNone = d.catcherLevel === 'NONE';
  const positionClass = isNone ? 'skip' : '';
  const cardClass = isNone ? 'none' : '';
  const mcapWarn = d.marketCapMissing ? '<span class="mcap-warn">⚠️ MCap নেই</span>' : '';
  const fsClass = isNone ? 'none' : '';
  return '<div class="card ' + cardClass + '"><div class="sme-card-header"><span class="sym">' + esc(d.symbol) + mcapWarn + '</span><span class="price">৳ ' + d.price + '</span></div>' +
    '<div class="catcher-badge ' + d.catcherCls + '">' + esc(d.catcherBn) + '</div><div class="sum-body">' +
      '<div class="entry-box"><span class="lbl">🎯 প্রবেশের ধরন</span><span class="val">' + esc(d.entryBn) + '</span></div>' +
      '<div class="sme-metrics">' +
        '<div class="sme-metric"><div class="lbl">L_score</div><div class="val green">' + d.lScore + ' (' + d.lScoreBn + ')</div></div>' +
        '<div class="sme-metric"><div class="lbl">Volume Ratio</div><div class="val">' + d.volumeRatio + 'x</div></div>' +
        '<div class="sme-metric"><div class="lbl">দাম vs 20 EMA</div><div class="val">' + d.priceVsEma20 + '%</div></div>' +
        '<div class="sme-metric"><div class="lbl">20 EMA</div><div class="val">' + ema20Icon + '</div></div></div>' +
      '<div class="sme-score-section"><h4>📊 তরলতার স্কোর</h4>' +
        '<div class="sme-score-row"><span class="label">L_score</span><div class="bar"><div class="fill" style="width:' + lPct + '%"></div></div><span class="value">' + d.lScore + '</span></div></div>' +
      '<div class="position-box ' + positionClass + '"><span class="lbl">💰 বিনিয়োগ</span><span class="val">' + esc(d.positionBn) + '</span></div>' +
      '<div class="sme-final-score ' + fsClass + '"><span class="lbl">📊 Product Score</span><span class="val">' + d.productScore + '</span></div></div></div>';
}
function applyFiltersSME() {
  const s = (document.getElementById('sme_fSearch').value || '').trim().toLowerCase();
  const c = document.getElementById('sme_fCatcher').value;
  const e = document.getElementById('sme_fEntry').value;
  const lMin = parseFloat(document.getElementById('sme_fLMin').value) || 0;
  const pMin = parseFloat(document.getElementById('sme_fProductMin').value) || 0;
  const mcap = document.getElementById('sme_fMcap').value;
  const rows = SME_DATA.filter(function(d) {
    if (s && d.symbol.toLowerCase().indexOf(s) === -1) return false;
    if (c !== 'all' && d.catcherLevel !== c) return false;
    if (e !== 'all' && d.entryType !== e) return false;
    if (d.lScore < lMin) return false;
    if (d.productScore < pMin) return false;
    if (mcap === 'missing' && !d.marketCapMissing) return false;
    if (mcap === 'ok' && d.marketCapMissing) return false;
    return true;
  });
  rows.sort(function(a,b){ return b.productScore - a.productScore; });
  document.getElementById('sme_shown').textContent = rows.length;
  document.getElementById('sme_total').textContent = SME_DATA.length;
  document.getElementById('smeGrid').innerHTML = rows.length ? rows.map(smeCardHtml).join('') : '<div class="empty"><div class="big">🔍</div>এই শর্তে কোনো স্টক পাওয়া যায়নি।</div>';
}
function resetFiltersSME() {
  document.getElementById('sme_fSearch').value = '';
  document.getElementById('sme_fCatcher').value = 'all';
  document.getElementById('sme_fEntry').value = 'all';
  document.getElementById('sme_fLMin').value = '';
  document.getElementById('sme_fProductMin').value = '';
  document.getElementById('sme_fMcap').value = 'all';
  applyFiltersSME();
}
applyFiltersSP();
applyFiltersHike();
applyFiltersSummary();
applyFiltersAB();
applyFiltersFuzzy();
applyFiltersSME();
switchTab('swingparam');
''')
    parts.append('</script></body></html>')

    html = ''.join(parts)
    file_name = f"Bangla_All_In_One_{timestamp}.html"
    file_path = os.path.join(BANGLA_DIR, file_name)
    with open(file_path, 'w', encoding='utf-8') as f: f.write(html)
    return file_path

# ==================== DIAGNOSTICS ====================
def dump_fuzzy_diagnostics(reports, timestamp):
    rows = []
    for r in reports:
        try: rows.append({"symbol": str(r.symbol), "asset_class": str(r.asset_class), "price": round(float(r.current_price or 0), 2), "t_stat": round(float(r.mcmc_t_stat or 0), 4), "mu_hat": round(float(r.mcmc_mu or 0), 6), "M": round(float(r.fuzzy_v3_M or 0), 4), "P": round(float(r.fuzzy_v3_P or 0), 4), "C": round(float(r.fuzzy_v3_C or 0), 4), "R": round(float(r.fuzzy_v3_R or 0), 4), "L": round(float(r.fuzzy_v3_L or 0), 4), "S_gate": round(float(r.fuzzy_v3_S_gate or 0), 4), "S_rank": round(float(r.fuzzy_v3_S_rank or 0), 4), "hard_gate_pass": bool(r.fuzzy_v3_hard_gate_pass), "fuzzy_action": str(r.fuzzy_Action or " "), "radar_score": int(r.radar_score or 0), "sme_l_score": round(float(r.sme_l_score or 0), 4), "sme_catcher": str(r.sme_catcher_level or "NONE"), "final_score": round(float(r.final_score or 0), 4), "final_signal": str(r.final_signal or "WAIT"), "hike_tier": str(getattr(r, "hike_tier", TIER_NONE)), "extension_pct": round(float(getattr(r, "extension_pct_from_sma20", 0) or 0), 3), "vol_div_days": int(getattr(r, "volume_divergence_days", 0) or 0)})
        except Exception as e: rows.append({"symbol": str(getattr(r, "symbol", "UNK")), "error": str(e)})
    pd.DataFrame(rows).to_csv(f"{DIAG_DIR}/fuzzy_diag_{timestamp}.csv", index=False, encoding="utf-8-sig")
    print(f"\n🧪 Fuzzy diagnostic CSV: {DIAG_DIR}/fuzzy_diag_{timestamp}.csv")

# ==================== BATCH SCAN ====================
def _save_intermediate_csv(reports, path):
    rows = [{'Symbol': r.symbol, 'Price': round(r.current_price, 2), 'FinalSignal': r.final_signal, 'FinalScore': round(r.final_score, 4), 'Canvas': r.canvas_signal, 'Fuzzy': r.fuzzy_Action, 'HikeTier': r.hike_tier, 'SME': r.sme_catcher_level} for r in reports]
    pd.DataFrame(rows).to_csv(path, index=False, encoding='utf-8-sig')

def _print_clean_summary(reports, failed, total, failures_csv_path=None):
    print("\n" + "=" * 60); print("📊 FULL SCAN SUMMARY"); print("=" * 60)
    print(f"Total symbols          : {total}"); print(f"Successfully analysed  : {len(reports)}"); print(f"Failed / skipped       : {len(failed)}"); print()
    buy_n, hold_n, wait_n, sell_n = (sum(1 for r in reports if r.canvas_signal == s) for s in ['BUY','HOLD','WAIT','SELL'])
    print("📌 Signal Distribution"); print(f"🟢 BUY   : {buy_n}"); print(f"🟡 HOLD  : {hold_n}"); print(f"⏳ WAIT  : {wait_n}"); print(f"🔴 SELL  : {sell_n}"); print()
    fz_sb, fz_mb, fz_n, fz_av = (sum(1 for r in reports if r.fuzzy_Action == a) for a in ['Strong Buy','Mild Buy','Neutral / Watch','Avoid'])
    print("🧠 Fuzzy v3"); print(f"🟢🟢 Strong Buy : {fz_sb}"); print(f"🟢 Mild Buy    : {fz_mb}"); print(f"⏳ Neutral     : {fz_n}"); print(f"🔴 Avoid       : {fz_av}"); print()
    print("🥾 Hike-Tier")
    for t in TIER_ORDER:
        cnt = sum(1 for r in reports if r.hike_tier == t)
        if cnt > 0: print(f"  {t:<18s}: {cnt}")
    print(); print("🏆 Top 5 by Final Score")
    top5 = sorted(reports, key=lambda x: x.final_score, reverse=True)[:5]
    for i, r in enumerate(top5, 1): print(f"  {i}. {r.symbol:12s}  Score: {r.final_score:.3f}  Signal: {r.final_signal}")
    print()
    if failed:
        print("=" * 60); print("⚠️  FAILURE BREAKDOWN"); print("=" * 60)
        reasons = Counter(f.get("reason", "UNKNOWN") for f in failed)
        for reason, count in reasons.most_common(): print(f"  {reason:<22s}: {count}")
        print(); print("--- Failed symbols ---")
        for f in failed[:40]: print(f"  {f['symbol']:<14s} [{f.get('reason','?'):<20s}] {(f.get('details','') or '')[:60]}")
        if len(failed) > 40: print(f"  ... and {len(failed) - 40} more (see CSV)")
        print()
        if failures_csv_path: print(f"📁 Full failure list saved: {failures_csv_path}")
    print("=" * 60); print(f"📁 Bangla Report : {BANGLA_DIR}/"); print(f"📁 CSV Report    : {RESULTS_DIR}/canvas_scan_*.csv"); print("=" * 60)

def _probe_live_api():
    print("🔎 Probing live API …")
    try:
        _LIVE_CACHE.prefetch()
        df = _LIVE_CACHE.get_all()
        if df is not None and not df.empty:
            print(f"   ✅ Live API: {len(df)} symbols returned")
            cols = [c for c in ["code", "ltp", "volume"] if c in df.columns]
            if cols: print(f"   Sample:\n{df.head(3)[cols].to_string(index=False)}")
        else: print("   ❌ Live API: returned no data")
    except Exception as e: print(f"   ❌ Live API: Failed with error: {e}")
    print()

def _probe_history_sources():
    sym = "BATBC"; end = dt.date.today(); start = end - timedelta(days=60)
    start_str, end_str = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    print(f"🔎 Probing history sources for {sym} ({start_str} → {end_str}) …")
    try:
        df = _HIST_COLLECTOR._from_new_json_api(sym, start_str, end_str)
        if df is not None and len(df) >= 20: print(f"   ✅ New JSON API: {len(df)} rows")
        else: print(f"   ⚠️  New JSON API: no rows")
    except Exception as e: print(f"   ❌ New JSON API: {type(e).__name__}: {str(e)[:60]}")
    try:
        df = _HIST_COLLECTOR._from_bdshare(sym, start_str, end_str)
        if df is not None and len(df) >= 20: print(f"   ✅ bdshare     : {len(df)} rows")
        else: print(f"   ⚠️  bdshare     : no rows")
    except Exception as e: print(f"   ❌ bdshare     : {type(e).__name__}: {str(e)[:60]}")
    print()

def canvas_batch_scan(account_equity=1000000.0, generate_buy_cond=True, combined=True, ab_mode='LONG'):
    print("\n" + "=" * 80); print(f"📊 CANVAS BATCH SCAN – v1.31 (JSON API + Weekend Fix) | Mode: {ab_mode}"); print("=" * 80 + "\n")
    print("🔄 Prefetching live prices + market data ...")
    try: _LIVE_CACHE.prefetch()
    except Exception: print("⚠️ Prefetch failed.")
    _probe_live_api(); _probe_history_sources()
    stock_list = get_dse_stock_list(force_refresh=False); total = len(stock_list); print(f"📋 Total symbols to scan: {total}\n")
    dsex_series = get_dsex_historical(); breadth = compute_breadth_metrics(); regime = determine_regime(dsex_series)
    vol_label = 'Low' if regime['regime'] in ['Bull', 'Sideways'] else 'High'
    print(f"📈 DSEX Regime: {regime['regime']} (factor {regime['factor']:.2f})"); print(f"📊 Breadth θ: {breadth['theta']:.3f} ({breadth['condition']})"); print(f"⚙️  Max Workers: {MAX_WORKERS}\n")
    reports = []; failed = []; completed = 0
    temp_csv_path = f"{RESULTS_DIR}/canvas_scan_TEMP_{dt.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    print("🚀 Starting scan...\n")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_stock_canvas, sym, dsex_series, breadth['theta'], regime['factor'], regime['regime'], vol_label, account_equity, ab_mode): sym for sym in stock_list}
        for fut in as_completed(futures):
            sym = futures[fut]; completed += 1; time.sleep(random.uniform(0.05, 0.15))
            try:
                res = fut.result()
                if res: reports.append(res); print(f"{sym:12s}  ({completed:3d}/{total})  ✅")
                else:
                    info = classify_failure(sym, exception=None); failed.append({"symbol": sym, **info}); print(f"{sym:12s}  ({completed:3d}/{total})  ⚠️ {info['reason']}")
            except Exception as e:
                info = classify_failure(sym, exception=e); failed.append({"symbol": sym, **info}); print(f"{sym:12s}  ({completed:3d}/{total})  ❌ {info['reason']}")
            if completed % 50 == 0: print(f"   ── 💾 Saving intermediate progress ({completed}/{total}) ──"); _save_intermediate_csv(reports, temp_csv_path)
    timestamp = dt.datetime.now().strftime('%Y%m%d_%H%M')
    final_csv_path = f"{RESULTS_DIR}/canvas_scan_{timestamp}.csv"; failures_csv_path = f"{RESULTS_DIR}/scan_failures_{timestamp}.csv"
    rows = [{'Symbol': r.symbol, 'Price': round(r.current_price, 2), 'Canvas': r.canvas_signal, 'Canvas_Reason': r.canvas_reason, 'FinalSignal': r.final_signal, 'FinalScore': r.final_score, 'AssetClass': r.asset_class, 'Verdict': r.verdict, 'Forecast': r.forecast_signal, 'Fuzzy': r.fuzzy_Action, 'Fz_Gate': round(r.fuzzy_v3_S_gate, 4), 'Fz_Rank': round(r.fuzzy_v3_S_rank, 4), 'Fz_C_raw': round(r.fuzzy_v3_C_raw, 4), 'Fz_TV': round(r.fuzzy_v3_turnover_velocity, 6), 'Fz_GatePass': r.fuzzy_v3_hard_gate_pass, 'RR_Prelim': round(r.reward_risk_ratio, 3), 'RR_Final': round(r.reward_risk_final, 3), 'Radar': r.radar_score, 'AB': r.ab_variant, 'SME': r.sme_catcher_level, 'SME_L': round(r.sme_l_score, 3), 'SME_Entry': r.sme_entry_type, 'SME_Health': r.sme_data_health, 'ADTV20': round(r.sme_adtv_20, 0), 'HikeTier': r.hike_tier, 'ExtensionPct': round(r.extension_pct_from_sma20, 3), 'SMA20': round(r.sma20_value, 3), 'VolDivDays': r.volume_divergence_days, 'VolDivTotal': r.volume_divergence_total, 'TierEntryOK': r.hike_tier_entry_allowed} for r in reports]
    pd.DataFrame(rows).to_csv(final_csv_path, index=False, encoding='utf-8-sig')
    if failed:
        try: pd.DataFrame(failed).to_csv(failures_csv_path, index=False, encoding='utf-8-sig')
        except Exception as e: print(f"⚠️ Could not write failure CSV: {e}"); failures_csv_path = None
    if os.path.exists(temp_csv_path):
        try: os.remove(temp_csv_path)
        except Exception: pass
    try: dump_fuzzy_diagnostics(reports, timestamp)
    except Exception as e: print(f"⚠️ Diagnostic dump failed: {e}")
    try:
        path = generate_bangla_all_in_one_html(reports, timestamp, account_equity, ab_mode)
        print(f"\n📝 একীভূত বাংলা রিপোর্ট সংরক্ষিত: {os.path.abspath(path)}")
    except Exception: print("\n❌ Bangla report generation FAILED:"); traceback.print_exc()
    _print_clean_summary(reports, failed, total, failures_csv_path); return pd.DataFrame(rows)

def canvas_analyze_one(symbol):
    print(f"\n🔍 Analysing {symbol}... ")
    try: _LIVE_CACHE.prefetch()
    except Exception: pass
    dsex_series = get_dsex_historical(); breadth = compute_breadth_metrics(); regime = determine_regime(dsex_series)
    vol_label = 'Low' if regime['regime'] in ['Bull', 'Sideways'] else 'High'
    report = analyze_stock_canvas(symbol.upper(), dsex_series, breadth['theta'], regime['factor'], regime['regime'], vol_label)
    if report is None:
        print("❌ Could not analyse. "); info = classify_failure(symbol)
        print(f"   Reason: {info['reason']} — {info['details']}"); return
    print(f"\n📊 {symbol} – Canvas (consensus): {report.canvas_signal} ({report.canvas_reason}) ")
    print(f"📊 Final Signal: {report.final_signal}  |  Final Score: {report.final_score} ")
    print(f"📊 Verdict: {report.verdict} ")
    print(f"🥾 Hike-Tier: {report.hike_tier}  |  Extension: {report.extension_pct_from_sma20:+.2f}%  |  SMA20: {report.sma20_value:.2f} ")
    print(f"🥾 Vol-Div Days: {report.volume_divergence_days}  (total {report.volume_divergence_total})  |  Entry Allowed: {report.hike_tier_entry_allowed} ")
    print(f"⚖️  RR_prelim: {report.reward_risk_ratio:.3f}  |  RR_final: {report.reward_risk_final:.3f} ")
    print(f"🧠 Fuzzy v3: {report.fuzzy_Action}  (Gate={report.fuzzy_v3_S_gate:.3f} × Rank={report.fuzzy_v3_S_rank:.3f}) ")
    print(f"🚀 SME: {report.sme_catcher_level} | L={report.sme_l_score} | Pos={report.sme_position_size_pct}% ")
    print(f"📊 Data Health: {report.sme_data_health} | Asset: {report.asset_class} ")
    print(f"📡 Radar: {report.radar_score} ")

# ==================== MAIN ====================
if __name__ == "__main__":
    print("\n🚀 Starting Automated DSE Scan for GitHub Actions...")
    try: canvas_batch_scan(account_equity=1000000.0, ab_mode='LONG')
    except Exception as e:
        print(f"\n❌ Scan failed with error: {e}")
        import traceback; traceback.print_exc()
