#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════════════╗
║          AKSA MICROSTRUCTURE SCREENER  v2.0                           ║
║          Interdependency Scoring Matrix │ Market Microstructure       ║
║          Binance Spot + Futures (fapi)  │ Async  │ Telegram Alerts    ║
╠═══════════════════════════════════════════════════════════════════════╣
║  Engine    : Interdependency Scoring Matrix (0–100)                   ║
║  Variables : Rolling VWAP · CVD Spot · CVD Futures                    ║
║              Open Interest Momentum · Funding Rate                    ║
╠═══════════════════════════════════════════════════════════════════════╣
║  Scenarios:                                                           ║
║    A  → Organic Spot Accumulation     (max +40)  Healthy move         ║
║    B  → Speculative Futures Rally     (max +15)  Leverage-driven      ║
║    C  → Short Squeeze Fuel            (+25)      OI drops + neg FR    ║
║    D  → Exhaustion / Overleverage     (−35)      Long squeeze risk    ║
║    E  → Distribution / Reversion Trap (cap 30)   Spot distributing    ║
║                                                                       ║
║  NO lagging indicators (no ADX, no MACD). Pure microstructure.        ║
╚═══════════════════════════════════════════════════════════════════════╝

Install deps:
    pip install aiohttp pandas numpy

Run:
    python aksa_microstructure_screener_v2.py
"""

from __future__ import annotations

import asyncio
import ssl
import sys
import logging
import time
import warnings
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np
import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

import requests as _requests

# ═══════════════════════════════════════════════════════════════════════
# 📡  AXANCTUM INTELLIGENCE 911 — Dashboard Simulation
# ═══════════════════════════════════════════════════════════════════════
# URL ini diupdate otomatis oleh start.sh setiap kali ngrok di-restart
DASHBOARD_URL = "https://sanctum2-production.up.railway.app"

def send_signal_to_dashboard(symbol, direction, entry, tp, sl, grade="B", leverage=5):
    """Kirim sinyal ke dashboard simulasi. Tidak crash bot kalau dashboard offline."""
    try:
        _requests.post(
            f"{DASHBOARD_URL}/signal",
            json={
                "symbol":    symbol.upper(),
                "direction": direction.upper(),
                "entry":     float(entry),
                "tp":        float(tp),
                "sl":        float(sl),
                "grade":     grade,
                "leverage":  int(leverage),
                "source":    "bot",
            },
            timeout=3,
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════
# ⚙️  CONFIGURATION  ─ Edit values below
# ═══════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── Telegram ─────────────────────────────────────────────────────────
    "TELEGRAM_TOKEN":   "8770070849:AAHHmQsjwxtwhuXqldVSYMsaxjURKuY7te0",
    "TELEGRAM_CHAT_ID": "5938324882",

    # ── 3-Cycle Settings ──────────────────────────────────────────────────
    # Setiap siklus punya batas koin dan minimum volume sendiri.
    # Koin dari siklus sebelumnya TIDAK akan muncul lagi.
    "CYCLES": [
        {"label": "Siklus 1 (Large Cap)",  "max_coins": 700, "min_volume": 10_000_000},
        {"label": "Siklus 2 (Mid Cap)",    "max_coins": 700, "min_volume":  3_000_000},
        {"label": "Siklus 3 (Small Cap)",  "max_coins": 700, "min_volume":  100_000},
    ],
    # Total pool yang di-fetch dari Binance sebelum dipartisi
    "TOTAL_POOL":         700,

    # ── Scan interval (berlaku untuk SETIAP siklus, bukan total) ─────────
    "SCAN_INTERVAL_MIN":  0.20,
    "MIN_SCORE":          70,

    # ── Lookback & Timeframe ──────────────────────────────────────────────
    "TIMEFRAME":          "1h",
    "LOOKBACK_N":         48,
    "FETCH_LIMIT":        100,

    # ── Threshold Scoring Matrix ──────────────────────────────────────────
    "VWAP_OVEREXT_PCT":   5.0,    # 3% → 5% (lebih longgar untuk crypto)
    "VWAP_DANGER_PCT":    8.0,    # > 8% = sangat overextended / bahaya
    "VWAP_SIDEWAYS_PCT":  1.5,
    "OI_PARABOLIC_PCT":   15.0,   # 10% → 15% (konfirmasi bandar baru)
    "OI_NOISE_PCT":       1.5,    # < 1.5% = noise, abaikan untuk squeeze
    "OI_IGNITION_MIN":    1.5,    # 1.5%-4% = goldilocks squeeze range
    "OI_IGNITION_MAX":    4.0,
    "OI_EXHAUSTED_PCT":   8.0,    # > 8% turun = squeeze exhausted
    "OI_SQUEEZE_PCT":    -1.5,    # threshold squeeze (dipakai di OI modifier)
    "SQUEEZE_MIN_FUEL":   45,
    "FR_OVERLEVERAGE":    0.0001,
    "FR_NEGATIVE":        0.0,
    "CVD_SPOT_FLAT":      5.0,
    "CVD_MIN_VOL_RATIO":  0.15,   # CVD diabaikan jika vol < 15% dari rata-rata

    # ── Concurrency & Network ─────────────────────────────────────────────
    "CONCURRENT_TASKS":   25,
    "REQUEST_TIMEOUT":    15,
    "MSG_DELAY":          0.6,
    "SSL_VERIFY":         False,
}
# ── In-memory FR Velocity Tracker ────────────────────────────────────
# _FR_HISTORY_4H: simpan (fr_value, timestamp) per simbol
# FR Binance berubah setiap 4-8 jam — velocity diukur vs 4 jam lalu
_FR_HISTORY_4H: Dict[str, Tuple[float, float]] = {}
# Interval minimum (detik) sebelum FR history diupdate: 4 jam
_FR_VELOCITY_INTERVAL = 4 * 3600

# ── API Base URLs ─────────────────────────────────────────────────────
SPOT_BASE    = "https://api.binance.com"
FUTURES_BASE = "https://fapi.binance.com"
TELEGRAM_API = "https://api.telegram.org"

# ── Grade scale ───────────────────────────────────────────────────────
GRADE_SCALE = [
    (85, "A+", "🔥 PRIME SIGNAL"),
    (70, "A",  "✅ STRONG SIGNAL"),
    (55, "B+", "🟡 DECENT SIGNAL"),
    (40, "B",  "📊 WATCH SIGNAL"),
    (25, "C",  "⚠️ WEAK SIGNAL"),
    (0,  "D",  "⚪ NO SIGNAL"),
]

# ── Timeframe ke menit ────────────────────────────────────────────────
TF_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360,
    "8h": 480, "12h": 720, "1d": 1440,
}

# ── OI period mapping (timeframe → Binance OI period string) ─────────
OI_PERIOD_MAP = {
    "1m": "5m", "3m": "5m", "5m": "5m",
    "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h",
    "6h": "4h", "8h": "4h", "12h": "4h",
    "1d": "4h",
}


# ═══════════════════════════════════════════════════════════════════════
# 📋  LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("aksa_v2.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("AksaV2")


# ═══════════════════════════════════════════════════════════════════════
# 🔌  HTTP SESSION FACTORY
# ═══════════════════════════════════════════════════════════════════════
def _build_ssl_ctx() -> ssl.SSLContext:
    """Buat SSL context dengan opsi bypass untuk ISP Indonesia."""
    ctx = ssl.create_default_context()
    if not CONFIG["SSL_VERIFY"]:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        log.info("🔓 SSL verification disabled (ISP bypass mode)")
    return ctx


def create_session() -> aiohttp.ClientSession:
    """Buat aiohttp session dengan connector yang tepat."""
    connector = aiohttp.TCPConnector(
        ssl=_build_ssl_ctx(),
        limit=60,
        limit_per_host=10,
        ttl_dns_cache=300,
    )
    return aiohttp.ClientSession(connector=connector)


# ═══════════════════════════════════════════════════════════════════════
# 📥  ASYNC DATA FETCHERS
# ═══════════════════════════════════════════════════════════════════════
async def _get(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None,
) -> Optional[any]:
    """Generic async GET → JSON. Returns None on any error."""
    try:
        timeout = aiohttp.ClientTimeout(total=CONFIG["REQUEST_TIMEOUT"])
        async with session.get(url, params=params, timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.json(content_type=None)
            log.debug(f"HTTP {resp.status} | {url} | params={params}")
            return None
    except asyncio.TimeoutError:
        log.debug(f"Timeout: {url}")
        return None
    except Exception as exc:
        log.debug(f"GET error [{url}]: {exc}")
        return None


async def fetch_full_symbol_pool(
    session: aiohttp.ClientSession,
) -> List[Tuple[str, float]]:
    """
    Fetch seluruh pool simbol dari Binance Futures.
    Return: list of (symbol, volume_24h_usd), sudah diurutkan volume DESC.
    Diambil hingga TOTAL_POOL koin teratas.
    """
    try:
        info = await _get(session, f"{FUTURES_BASE}/fapi/v1/exchangeInfo")
        if not info:
            return []

        perp_set = {
            s["symbol"]
            for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        }

        tickers = await _get(session, f"{FUTURES_BASE}/fapi/v1/ticker/24hr")
        if not tickers:
            log.warning("Ticker tidak tersedia — pool kosong.")
            return []

        # Volume minimum absolut = volume floor siklus terkecil
        min_vol_global = min(c["min_volume"] for c in CONFIG["CYCLES"])

        pool = [
            (t["symbol"], float(t.get("quoteVolume", 0) or 0))
            for t in tickers
            if t["symbol"] in perp_set
            and float(t.get("quoteVolume", 0) or 0) >= min_vol_global
        ]
        pool.sort(key=lambda x: x[1], reverse=True)
        pool = pool[: CONFIG["TOTAL_POOL"]]

        log.info(
            f"📦 Pool fetched: {len(pool)} simbol "
            f"(min vol ${min_vol_global/1e6:.0f}M, max {CONFIG['TOTAL_POOL']} koin)"
        )
        return pool

    except Exception as exc:
        log.error(f"fetch_full_symbol_pool error: {exc}")
        return []


def partition_symbols(
    pool: List[Tuple[str, float]],
) -> List[List[str]]:
    """
    Partisi pool simbol menjadi 3 siklus tanpa overlap.

    Algoritma:
      - Pool sudah diurutkan volume DESC (terbesar duluan).
      - Siklus 1 ambil dari atas (volume >= min_vol_1), max N koin.
      - Siklus 2 ambil dari sisa (volume >= min_vol_2), max N koin.
      - Siklus 3 ambil dari sisa (volume >= min_vol_3), max N koin.
      - Koin yang sudah masuk siklus sebelumnya TIDAK bisa masuk lagi.

    Return: list of 3 list (masing-masing = simbol untuk satu siklus).
    """
    cycles_cfg  = CONFIG["CYCLES"]
    used        = set()
    partitions  = []

    for cfg in cycles_cfg:
        batch = []
        for sym, vol in pool:
            if sym in used:
                continue
            if vol < cfg["min_volume"]:
                continue
            batch.append(sym)
            if len(batch) >= cfg["max_coins"]:
                break

        for sym in batch:
            used.add(sym)

        partitions.append(batch)
        log.info(
            f"  📋 {cfg['label']}: {len(batch)} koin "
            f"(min vol ${cfg['min_volume']/1e6:.0f}M, max {cfg['max_coins']})"
        )

    total = sum(len(p) for p in partitions)
    log.info(f"  ✅ Total unik ter-partisi: {total} koin dari {len(pool)} pool")
    return partitions


async def fetch_futures_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    limit: int,
) -> Optional[pd.DataFrame]:
    """
    Ambil klines Binance Futures (fapi) beserta taker buy volume.
    Binance kline field index:
      [0] open_time  [1] open  [2] high  [3] low  [4] close
      [5] volume     [9] taker_buy_base_asset_volume
    """
    raw = await _get(
        session,
        f"{FUTURES_BASE}/fapi/v1/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
    if not raw or len(raw) < 3:
        return None

    rows = []
    for k in raw:
        total_vol = float(k[5])
        taker_buy = float(k[9])
        rows.append(
            {
                "ts":             int(k[0]),
                "open":           float(k[1]),
                "high":           float(k[2]),
                "low":            float(k[3]),
                "close":          float(k[4]),
                "volume":         total_vol,
                "taker_buy_vol":  taker_buy,
                "taker_sell_vol": total_vol - taker_buy,
            }
        )
    return pd.DataFrame(rows)


async def fetch_spot_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    limit: int,
) -> Optional[pd.DataFrame]:
    """
    Ambil klines Binance SPOT beserta taker buy volume.
    Format kline identik dengan futures — field index sama.
    """
    raw = await _get(
        session,
        f"{SPOT_BASE}/api/v3/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
    if not raw or len(raw) < 3:
        return None

    rows = []
    for k in raw:
        total_vol = float(k[5])
        taker_buy = float(k[9])
        rows.append(
            {
                "ts":             int(k[0]),
                "open":           float(k[1]),
                "high":           float(k[2]),
                "low":            float(k[3]),
                "close":          float(k[4]),
                "volume":         total_vol,
                "taker_buy_vol":  taker_buy,
                "taker_sell_vol": total_vol - taker_buy,
            }
        )
    return pd.DataFrame(rows)


async def fetch_funding_rate(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Tuple[Optional[float], float]:
    """
    Ambil funding rate terkini dan hitung Spot-Futures Basis.

    Basis = (MarkPrice − IndexPrice) / IndexPrice × 100
      Basis > 0 → Futures lebih mahal dari Spot → leverage driven ⚠️
      Basis < 0 → Spot memimpin → organik ✅
      Basis ≈ 0 → netral

    premiumIndex endpoint menyediakan keduanya dalam 1 request:
      markPrice  = harga mark futures
      indexPrice = harga index spot (sangat dekat dengan spot aktual)

    Returns: (funding_rate, basis_pct)
    """
    data = await _get(
        session,
        f"{FUTURES_BASE}/fapi/v1/premiumIndex",
        {"symbol": symbol},
    )
    if not data:
        return None, 0.0
    try:
        fr         = float(data.get("lastFundingRate", 0) or 0)
        mark_price = float(data.get("markPrice", 0) or 0)
        idx_price  = float(data.get("indexPrice", 0) or 0)

        if idx_price > 0:
            basis_pct = (mark_price - idx_price) / idx_price * 100
        else:
            basis_pct = 0.0

        return fr, round(basis_pct, 4)
    except (ValueError, TypeError):
        return None, 0.0


async def fetch_oi_history(
    session: aiohttp.ClientSession,
    symbol: str,
    period: str,
    limit: int,
) -> Optional[List[float]]:
    """
    Ambil riwayat Open Interest dari Binance Futures.
    Mendukung period: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    Return: list nilai OI dari lama → baru.
    """
    data = await _get(
        session,
        f"{FUTURES_BASE}/futures/data/openInterestHist",
        {"symbol": symbol, "period": period, "limit": limit},
    )
    if not data or len(data) < 2:
        return None
    try:
        return [float(r["sumOpenInterest"]) for r in data]
    except (KeyError, TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════
# 📐  MICROSTRUCTURE INDICATORS
# ═══════════════════════════════════════════════════════════════════════
def calc_rolling_vwap(df: pd.DataFrame, n: int) -> Tuple[float, float]:
    """
    Rolling VWAP berbasis N candle terakhir.

        VWAP = Σ(TP_i × Vol_i) / Σ(Vol_i)
        TP   = (High + Low + Close) / 3   ← Typical Price

        D_vwap = (Price_current − VWAP) / VWAP × 100

    Returns: (vwap, d_vwap_pct)
      d_vwap > 0 → harga di atas VWAP (premium)
      d_vwap < 0 → harga di bawah VWAP (discount)
    """
    tail = df.tail(n).copy()
    typical_price = (tail["high"] + tail["low"] + tail["close"]) / 3
    total_vol = tail["volume"].sum()

    if total_vol == 0:
        return float(df["close"].iloc[-1]), 0.0

    vwap = float((typical_price * tail["volume"]).sum() / total_vol)
    price_now = float(df["close"].iloc[-1])

    if vwap == 0:
        return price_now, 0.0

    d_vwap = (price_now - vwap) / vwap * 100
    return round(vwap, 8), round(d_vwap, 4)


def calc_cvd_and_momentum(df: pd.DataFrame, n: int) -> Tuple[float, float]:
    """
    Hitung Cumulative Volume Delta (CVD) dan momentum perubahan-nya.

        CVD = Σ(TakerBuyVol_i − TakerSellVol_i)   dari candle ke-1 s/d N

    Momentum (ΔCVD):
        Split N candle menjadi dua paruh.
        CVD_past    = CVD dari paruh pertama (candle awal)
        CVD_current = CVD dari paruh kedua  (candle terbaru)
        ΔCVD = (CVD_current − CVD_past) / |CVD_past| × 100

        Nilai positif → tekanan beli semakin kuat
        Nilai negatif → tekanan jual semakin kuat

    Returns: (cvd_total, delta_cvd_pct)
    """
    tail  = df.tail(n).copy()
    delta = tail["taker_buy_vol"] - tail["taker_sell_vol"]
    cvd_total = float(delta.sum())

    half = max(1, n // 2)
    cvd_past    = float(delta.iloc[:half].sum())
    cvd_current = float(delta.iloc[half:].sum())

    # Normalisasi by total taker volume — stabil, tidak bisa meledak
    total_vol = float(tail["taker_buy_vol"].sum() + tail["taker_sell_vol"].sum())
    if total_vol < 1e-10:
        delta_cvd_pct = 0.0
    else:
        delta_cvd_pct = (cvd_current - cvd_past) / total_vol * 100.0

    # ── CVD Noise Filter ──────────────────────────────────────────────
    # Jika total volume terlalu kecil relatif terhadap candle sebelumnya,
    # sinyal CVD tidak dapat dipercaya (market mati / debu)
    # avg_vol_per_candle = total_vol / N — bandingkan dengan threshold minimum
    avg_vol_per_candle = total_vol / max(n, 1)
    # Ambil rata-rata volume dari seluruh tail untuk referensi "normal"
    ref_vol = float(tail["volume"].mean())
    if ref_vol > 1e-10 and avg_vol_per_candle < ref_vol * CONFIG.get("CVD_MIN_VOL_RATIO", 0.15):
        # Volume terlalu kecil → set CVD ke nol (jangan beri sinyal pada debu)
        delta_cvd_pct = 0.0
        cvd_total     = 0.0

    delta_cvd_pct = max(-500.0, min(500.0, delta_cvd_pct))
    return round(cvd_total, 4), round(delta_cvd_pct, 2)


def calc_oi_momentum(oi_list: List[float]) -> float:
    """
    ΔOI = (OI_current − OI_past) / OI_past × 100

    OI_past    = nilai OI candle pertama dalam list
    OI_current = nilai OI candle terakhir

    Nilai positif → posisi terbuka bertambah (leverage naik)
    Nilai negatif → posisi terbuka berkurang (deleveraging)
    """
    if not oi_list or len(oi_list) < 2:
        return 0.0
    oi_past    = oi_list[0]
    oi_current = oi_list[-1]
    if oi_past == 0:
        return 0.0
    return round((oi_current - oi_past) / oi_past * 100, 4)


def calc_price_momentum(df: pd.DataFrame, n: int) -> float:
    """
    ΔPrice = (Price_current − Price_past) / Price_past × 100

    Price_past    = close candle ke-(len-N)
    Price_current = close candle terakhir
    """
    if len(df) < n + 1:
        n = len(df) - 1
    if n <= 0:
        return 0.0
    p_past    = float(df["close"].iloc[-n - 1])
    p_current = float(df["close"].iloc[-1])
    if p_past == 0:
        return 0.0
    return round((p_current - p_past) / p_past * 100, 4)

def calc_absorption(
    fut_df:         pd.DataFrame,
    delta_cvd_spot: float,
    delta_cvd_fut:  float,
    delta_price:    float,
    n:              int,
) -> Tuple[float, str]:
    """
    Deteksi Absorption menggunakan Combined CVD (Spot + Futures).

    Sebelumnya hanya cek CVD Spot — padahal volume futures jauh lebih
    besar di crypto. Absorption paling berbahaya justru terjadi di futures:
    ada sell wall raksasa di futures yang menyerap semua pembelian tapi
    harga tidak bergerak.

    Combined CVD = (CVD_spot × 1.0 + CVD_fut × 1.3) / 2.3
    Futures diberi bobot lebih besar karena volumenya lebih dominan.

    Kondisi absorption:
      - Combined CVD naik kuat (> 3%)
      - Tapi ΔPrice sangat kecil relatif terhadap ATR (< 0.40×ATR)
      → Ada yang menyerap semua pembelian → bukan akumulasi sehat
    """
    tail    = fut_df.tail(n).copy()
    hl_avg  = float((tail["high"] - tail["low"]).mean())
    price   = float(fut_df["close"].iloc[-1])
    atr_pct = (hl_avg / price * 100) if price > 0 else 0.5
    atr_pct = max(0.1, min(atr_pct, 10.0))

    # Combined CVD — futures diberi bobot lebih besar
    combined_cvd = (delta_cvd_spot * 1.0 + delta_cvd_fut * 1.3) / 2.3

    # Syarat 1: Combined CVD naik signifikan
    cvd_kuat = combined_cvd > 3.0

    # Syarat 2: Harga hampir tidak bergerak relatif terhadap ATR
    price_vs_atr  = abs(delta_price) / atr_pct if atr_pct > 0 else 1.0
    harga_stagnan = price_vs_atr < 0.40

    if cvd_kuat and harga_stagnan:
        strength = min(combined_cvd / 8.0, 1.0)
        penalty  = max(0.40, 1.0 - strength * 0.60)
        desc = (
            f"🧱 Absorption: CVD Combined +{combined_cvd:.1f}% "
            f"(Spot:{delta_cvd_spot:+.1f}% Fut:{delta_cvd_fut:+.1f}%) "
            f"tapi harga {delta_price:+.2f}% ({price_vs_atr:.2f}×ATR)"
        )
        return round(penalty, 3), desc

    return 1.0, ""
    """
    Deteksi Absorption: CVD Spot naik tapi harga tidak bergerak.

    Fenomena ini terjadi ketika ada "iceberg" limit sell order raksasa
    yang secara diam-diam menyerap semua tekanan beli agresif (taker buy).
    Hasilnya CVD naik tapi harga stagnan — ini bukan akumulasi sehat,
    ini bisa jadi exhaustion atau jebakan sebelum reversal.

    Cara ukur:
      - CVD_spot naik signifikan (delta > threshold)
      - Tapi ΔPrice sangat kecil relatif terhadap ATR candle
      - Rasio price_move / ATR < 0.25 → harga bergerak < 25% dari normalnya

    Returns: (penalty_multiplier, description)
      penalty_multiplier: 0.40–1.0
        1.0  = tidak ada absorpsi
        0.4  = absorpsi sangat kuat → skor dipotong 60%
    """
    # ATR sederhana dari N candle (High − Low, bukan true range penuh)
    tail    = fut_df.tail(n).copy()
    hl_avg  = float((tail["high"] - tail["low"]).mean())
    price   = float(fut_df["close"].iloc[-1])
    atr_pct = (hl_avg / price * 100) if price > 0 else 0.5
    atr_pct = max(0.1, min(atr_pct, 10.0))

    # Syarat 1: CVD Spot naik cukup kuat (> 3% normalized)
    cvd_kuat = delta_cvd_spot > 3.0

    # Syarat 2: Harga hampir tidak kemana-mana relatif terhadap ATR
    price_vs_atr = abs(delta_price) / atr_pct if atr_pct > 0 else 1.0
    harga_stagnan = price_vs_atr < 0.40

    if cvd_kuat and harga_stagnan:
        # Kekuatan absorpsi: semakin besar CVD vs semakin kecil gerak harga
        strength = min(delta_cvd_spot / 8.0, 1.0)   # normalize 0–1
        penalty  = max(0.40, 1.0 - strength * 0.60)  # range 0.40–1.0
        desc = (
            f"🧱 Absorption: CVD Spot +{delta_cvd_spot:.1f}% "
            f"tapi harga hanya {delta_price:+.2f}% "
            f"({price_vs_atr:.2f}× ATR) — kemungkinan Iceberg Sell"
        )
        return round(penalty, 3), desc

    return 1.0, ""
def calc_volume_anomaly(
    df: pd.DataFrame,
    n: int,
    tf_minutes: int = 60,
) -> Tuple[float, str]:
    """
    Bandingkan volume candle terakhir vs rata-rata N candle sebelumnya.

    FIX UNCLOSED CANDLE:
    Jika candle terakhir belum closed (baru berjalan sebagian),
    volume-nya diproyeksikan ke full candle sebelum dibandingkan.

    Proyeksi: vol_projected = vol_sekarang × (tf_minutes / menit_berjalan)
    Menit berjalan dihitung dari selisih open_time candle terakhir vs sekarang.
    """
    if len(df) < n + 1:
        return 1.0, "─ Normal"

    tail     = df.tail(n + 1)
    last_vol = float(tail["volume"].iloc[-1])
    avg_vol  = float(tail["volume"].iloc[:-1].mean())

    if avg_vol < 1e-10:
        return 1.0, "─ Normal"

    # ── Deteksi dan koreksi unclosed candle ──────────────────────────
    last_candle_open_ts = int(tail["ts"].iloc[-1])
    now_ms              = int(time.time() * 1000)
    elapsed_ms          = now_ms - last_candle_open_ts
    elapsed_min         = elapsed_ms / 60_000

    # Jika candle baru berjalan < 90% dari durasi penuh → proyeksikan
    if 0 < elapsed_min < tf_minutes * 0.9:
        projection_factor = tf_minutes / max(elapsed_min, 1.0)
        # Cap projection: max 10x untuk hindari outlier ekstrem
        projection_factor = min(projection_factor, 10.0)
        last_vol = last_vol * projection_factor

    ratio = last_vol / avg_vol

    if ratio >= 3.5:
        label = f"🔴🔴🔴🔴🔴 {ratio:.1f}x — EKSTREM"
    elif ratio >= 2.5:
        label = f"🔴🔴🔴🔴⚪ {ratio:.1f}x — Sangat Tinggi"
    elif ratio >= 1.5:
        label = f"🔴🔴🔴⚪⚪ {ratio:.1f}x — Elevated"
    elif ratio >= 0.8:
        label = f"⚪⚪⚪⚪⚪ {ratio:.1f}x — Normal"
    else:
        label = f"🔵 {ratio:.1f}x — Sepi / Low Vol"

    return round(ratio, 2), label

def calc_cvd_volume_consistency(
    delta_cvd_spot: float,
    delta_cvd_fut:  float,
    vol_ratio:      float,
) -> Tuple[float, str]:
    """
    Cek apakah volume candle terakhir konsisten dengan CVD momentum.

    Menggunakan Combined CVD (bukan hanya spot) karena:
    - Futures bisa punya CVD kuat meski spot sepi
    - Keduanya perlu dikonfirmasi oleh volume candle terkini

    Combined CVD menentukan arah sinyal, vol_ratio menentukan konfirmasi.
    """
    # Combined CVD — weighted average
    combined_cvd = (delta_cvd_spot * 1.0 + delta_cvd_fut * 1.3) / 2.3

    # Hanya relevan jika ada sinyal beli (combined positif)
    if combined_cvd <= 0:
        return 1.0, ""

    if vol_ratio < 0.3:
        mult = 0.65
        desc = (
            f"⚠️ CVD-Vol Inconsistent: CVD Combined +{combined_cvd:.1f}% "
            f"tapi volume hanya {vol_ratio:.1f}x — momentum memudar"
        )
    elif vol_ratio < 0.5:
        mult = 0.80
        desc = (
            f"🟡 CVD-Vol Lemah: CVD Combined +{combined_cvd:.1f}% "
            f"dengan volume {vol_ratio:.1f}x — konfirmasi tipis"
        )
    elif vol_ratio < 0.8:
        mult = 0.92
        desc = ""
    elif vol_ratio >= 2.0 and combined_cvd > 5.0:
        mult = 1.06
        desc = (
            f"✅ CVD-Vol Kuat: CVD Combined +{combined_cvd:.1f}% "
            f"dikonfirmasi volume {vol_ratio:.1f}x"
        )
    else:
        mult = 1.0
        desc = ""

    return round(mult, 3), desc
    """
    Cek apakah volume candle terakhir konsisten dengan CVD momentum.

    CVD dibangun dari akumulasi N candle. Tapi kalau candle terakhir
    volume-nya sangat sepi, berarti momentum yang terlihat di CVD
    sudah memudar — tidak ada lagi "uang baru" yang masuk sekarang.

    Sebaliknya kalau volume candle terakhir tinggi dan CVD juga naik,
    berarti momentum masih aktif dan terkonfirmasi.

    Contoh kasus:
      CVD +33% tapi vol 0.1x → sinyal lama, tidak ada konfirmasi baru
      CVD +33% tapi vol 2.5x → sinyal kuat dan masih aktif sekarang

    Returns: (consistency_multiplier, description)
      1.0+  = konsisten / volume mengkonfirmasi CVD
      < 1.0 = inkonsisten / momentum mungkin sudah memudar
    """
    # Hanya relevan jika CVD menunjukkan sinyal beli (positif)
    if delta_cvd_spot <= 0:
        return 1.0, ""

    if vol_ratio < 0.3:
        # Volume sangat sepi — momentum CVD hampir pasti sudah habis
        mult = 0.65
        desc = (
            f"⚠️ CVD-Vol Inconsistent: CVD +{delta_cvd_spot:.1f}% "
            f"tapi volume hanya {vol_ratio:.1f}x — momentum memudar"
        )
    elif vol_ratio < 0.5:
        # Volume sepi tapi tidak ekstrem
        mult = 0.80
        desc = (
            f"🟡 CVD-Vol Lemah: CVD +{delta_cvd_spot:.1f}% "
            f"dengan volume {vol_ratio:.1f}x — konfirmasi tipis"
        )
    elif vol_ratio < 0.8:
        # Sedikit di bawah normal — slight concern
        mult = 0.92
        desc = ""
    elif vol_ratio >= 2.0 and delta_cvd_spot > 5.0:
        # Volume tinggi + CVD kuat → momentum sangat aktif
        mult = 1.06
        desc = (
            f"✅ CVD-Vol Kuat: CVD +{delta_cvd_spot:.1f}% "
            f"dikonfirmasi volume {vol_ratio:.1f}x"
        )
    else:
        # Normal — tidak ada koreksi
        mult = 1.0
        desc = ""

    return round(mult, 3), desc


def calc_squeeze_stage(
    delta_oi:     float,
    funding_rate: float,
    delta_price:  float,
) -> Tuple[str, str, str, float]:
    """
    Deteksi tipe dan tahap squeeze — SHORT maupun LONG.

    SHORT SQUEEZE:
      Harga naik + OI turun + FR negatif
      → Shorts terlikuidasi paksa → harga naik lebih lanjut
      → Signal: BULLISH

    LONG SQUEEZE:
      Harga turun + OI turun + FR positif
      → Longs terlikuidasi paksa → harga turun lebih lanjut
      → Signal: SHORT

    LONG SQUEEZE EXHAUSTED:
      Harga sudah turun + OI sudah habis + FR sudah netral/negatif
      → Longs sudah habis dilikuidasi → potensi reversal/bounce
      → Signal: POTENSI REVERSAL

    Returns: (squeeze_type, stage, description, fuel_remaining_pct)
      squeeze_type : "short" | "long" | "long_exhausted" | "none"
      stage        : "early" | "mid" | "late" | "exhausted" | "none"
      fuel         : estimasi % bahan bakar yang tersisa (0–100)
    """
    # ── Short Squeeze ─────────────────────────────────────────────────
    if delta_oi < 0 and delta_price > 0:
        oi_drop = abs(delta_oi)
        if oi_drop < 2:      oi_score = 0.9
        elif oi_drop < 5:    oi_score = 0.65
        elif oi_drop < 10:   oi_score = 0.35
        else:                oi_score = 0.1

        if funding_rate < -0.0010:   fr_score = 1.0
        elif funding_rate < -0.0003: fr_score = 0.75
        elif funding_rate < 0:       fr_score = 0.5
        elif funding_rate < 0.0002:  fr_score = 0.25
        else:                        fr_score = 0.05

        if delta_price < 1:    price_score = 0.9
        elif delta_price < 3:  price_score = 0.65
        elif delta_price < 6:  price_score = 0.35
        else:                  price_score = 0.1

        fuel = (oi_score * 0.4 + fr_score * 0.35 + price_score * 0.25) * 100

        if fuel >= 70:
            stage = "early"
            desc  = f"🔫 Short Squeeze Early — ~{fuel:.0f}% fuel tersisa"
        elif fuel >= 45:
            stage = "mid"
            desc  = f"🔫 Short Squeeze Mid — ~{fuel:.0f}% fuel tersisa"
        elif fuel >= 20:
            stage = "late"
            desc  = f"⚠️ Short Squeeze Late — hampir habis (~{fuel:.0f}%)"
        else:
            stage = "exhausted"
            desc  = f"💨 Short Squeeze Exhausted — fuel ~{fuel:.0f}%, waspadai reversal"

        return "short", stage, desc, round(fuel, 1)

    # ── Long Squeeze ──────────────────────────────────────────────────
    if delta_oi < 0 and delta_price < 0 and funding_rate > 0:
        oi_drop = abs(delta_oi)

        # Seberapa banyak longs yang sudah dilikuidasi
        if oi_drop < 2:      oi_score = 0.9   # baru mulai
        elif oi_drop < 5:    oi_score = 0.65
        elif oi_drop < 10:   oi_score = 0.35
        else:                oi_score = 0.1   # sudah hampir habis

        # FR masih positif = masih ada longs yang belum kena likuidasi
        if funding_rate > 0.0010:    fr_score = 1.0
        elif funding_rate > 0.0005:  fr_score = 0.75
        elif funding_rate > 0.0001:  fr_score = 0.5
        else:                        fr_score = 0.15  # FR hampir netral

        # Seberapa jauh harga sudah turun
        drop_abs = abs(delta_price)
        if drop_abs < 1:    price_score = 0.9
        elif drop_abs < 3:  price_score = 0.65
        elif drop_abs < 6:  price_score = 0.35
        else:               price_score = 0.1

        fuel = (oi_score * 0.4 + fr_score * 0.35 + price_score * 0.25) * 100

        # Long squeeze exhausted = peluang reversal
        if fuel < 25 and funding_rate < 0.0002:
            stage = "exhausted"
            desc  = f"✅ Long Squeeze Exhausted — longs habis, potensi reversal"
            return "long_exhausted", stage, desc, round(fuel, 1)

        if fuel >= 70:
            stage = "early"
            desc  = f"💀 Long Squeeze Early — ~{fuel:.0f}% longs belum likuidasi"
        elif fuel >= 45:
            stage = "mid"
            desc  = f"💀 Long Squeeze Mid — ~{fuel:.0f}% longs tersisa"
        elif fuel >= 20:
            stage = "late"
            desc  = f"⚠️ Long Squeeze Late — hampir selesai (~{fuel:.0f}%)"
        else:
            stage = "exhausted"
            desc  = f"✅ Long Squeeze Exhausted — potensi reversal/bounce"

        return "long", stage, desc, round(fuel, 1)

    return "none", "none", "─ Tidak ada squeeze aktif", 0.0


def calc_price_targets(
    price:      float,
    vwap:       float,
    d_vwap:     float,
    df:         "pd.DataFrame",
    n:          int,
    flags:      List[str],
    squeeze_stage: str,
) -> Dict:
    """
    Hitung target harga dan level invalidasi berdasarkan kondisi microstructure.

    ATR (Average True Range) dihitung dari N candle terakhir sebagai
    proxy volatilitas — menggantikan kebutuhan akan indikator eksternal.

        ATR_candle = max(High, Close_prev) − min(Low, Close_prev)
        ATR_avg    = mean(ATR_candle, N candle terakhir)
        ATR_pct    = ATR_avg / Price × 100

    Target dan stop diekspresikan dalam % dari harga saat ini.
    """
    # ── Hitung ATR sebagai proxy volatilitas ─────────────────────────
    tail      = df.tail(n + 1).copy()
    prev_close = tail["close"].shift(1)
    true_range = (
        tail[["high", "close"]].max(axis=1) -
        tail[["low"]].join(prev_close.rename("prev")).min(axis=1)
    )
    atr_pct = float(true_range.iloc[1:].mean() / price * 100)
    atr_pct = max(0.1, min(atr_pct, 10.0))  # clamp 0.1–10%

    targets: Dict = {
        "atr_pct":    round(atr_pct, 3),
        "target1":    None,
        "target2":    None,
        "invalidasi": None,
        "direction":  "NEUTRAL",
        "horizon":    "─",
        "rationale":  "─",
    }

    # ── Skenario A: Organic Accumulation ─────────────────────────────
    if "A" in flags and "E_DISTRIBUTION" not in flags:
        targets["direction"] = "BULLISH"
        targets["horizon"]   = "3–8 candle"

        if d_vwap < 0:
            # Harga di bawah VWAP → target pertama adalah VWAP itu sendiri
            t1 = vwap
            t2 = vwap * (1 + atr_pct / 100 * 1.5)
            targets["rationale"] = "Akumulasi dari discount — target mean reversion ke VWAP"
        else:
            # Harga sudah di atas VWAP → target ekstensi
            t1 = price * (1 + atr_pct / 100 * 1.2)
            t2 = price * (1 + atr_pct / 100 * 2.2)
            targets["rationale"] = "Akumulasi di atas VWAP — target ekstensi ATR"

        targets["target1"]    = round(t1, 6)
        targets["target2"]    = round(t2, 6)
        targets["invalidasi"] = round(price * (1 - atr_pct / 100 * 0.8), 6)

    # ── Skenario C: Short Squeeze ─────────────────────────────────────
    elif "C_SQUEEZE" in flags:
        targets["direction"] = "BULLISH"

        if squeeze_stage == "early":
            mult1, mult2       = 1.8, 3.2
            targets["horizon"] = "2–6 candle"
            targets["rationale"] = "Early squeeze — momentum belum puncak"
        elif squeeze_stage == "mid":
            mult1, mult2       = 1.2, 2.0
            targets["horizon"] = "2–4 candle"
            targets["rationale"] = "Mid squeeze — target lebih konservatif"
        else:
            mult1, mult2       = 0.6, 1.0
            targets["horizon"] = "1–2 candle"
            targets["rationale"] = "Late/exhausted squeeze — target sangat ketat, waspadai reversal"

        targets["target1"]    = round(price * (1 + atr_pct / 100 * mult1), 6)
        targets["target2"]    = round(price * (1 + atr_pct / 100 * mult2), 6)
        targets["invalidasi"] = round(price * (1 - atr_pct / 100 * 0.6), 6)

    # ── Skenario E: Distribution / Mean Reversion ─────────────────────
    elif "E_DISTRIBUTION" in flags:
        targets["direction"]  = "BEARISH / AVOID LONG"
        targets["horizon"]    = "2–5 candle"
        targets["rationale"]  = "Distribusi di level tinggi — target mean reversion ke VWAP"
        targets["target1"]    = round(vwap, 6)           # kembali ke VWAP
        targets["target2"]    = round(vwap * 0.985, 6)  # sedikit di bawah VWAP
        targets["invalidasi"] = round(price * 1.015, 6) # 1.5% di atas = sinyal invalid

    # ── Skenario B: Speculative ───────────────────────────────────────
    elif "B_SPECULATIVE" in flags:
        targets["direction"]  = "SPECULATIVE BULLISH ⚠️"
        targets["horizon"]    = "1–3 candle"
        targets["rationale"]  = "Futures-driven — target sempit, stop ketat"
        targets["target1"]    = round(price * (1 + atr_pct / 100 * 0.8), 6)
        targets["target2"]    = round(price * (1 + atr_pct / 100 * 1.4), 6)
        targets["invalidasi"] = round(price * (1 - atr_pct / 100 * 0.5), 6)

    # ── Skenario Long Squeeze: SHORT target ───────────────────────────
    elif "LONG_SQUEEZE" in flags:
        targets["direction"]  = "SHORT — Long Squeeze Aktif"
        targets["horizon"]    = "2–6 candle"
        targets["rationale"]  = "Long squeeze berlangsung — target ke bawah VWAP"
        targets["target1"]    = round(price * (1 - atr_pct / 100 * 1.5), 6)
        targets["target2"]    = round(price * (1 - atr_pct / 100 * 2.8), 6)
        targets["invalidasi"] = round(price * (1 + atr_pct / 100 * 0.8), 6)

    # ── Skenario Long Squeeze Exhausted: REVERSAL target ─────────────
    elif "LONG_SQUEEZE_EXHAUSTED" in flags:
        targets["direction"]  = "REVERSAL — Long Squeeze Selesai"
        targets["horizon"]    = "3–8 candle"
        targets["rationale"]  = "Longs sudah habis — setup bounce dari oversold"
        targets["target1"]    = round(vwap, 6)
        targets["target2"]    = round(vwap * (1 + atr_pct / 100 * 1.2), 6)
        targets["invalidasi"] = round(price * (1 - atr_pct / 100 * 0.6), 6)

    return targets


# ═══════════════════════════════════════════════════════════════════════
# 🏆  CORE ENGINE: INTERDEPENDENCY SCORING MATRIX
# ═══════════════════════════════════════════════════════════════════════
def calculate_market_score(
    d_vwap:         float,
    delta_cvd_spot: float,
    delta_cvd_fut:  float,
    delta_oi:       float,
    delta_price:    float,
    funding_rate:   float,
    basis_pct:      float = 0.0,
    fr_velocity:    float = 0.0,
    vol_ratio:      float = 1.0,
    squeeze_type:   str   = "none",
) -> Tuple[float, List[str], List[str]]:
    """
    Interdependency Scoring Matrix — Layer 1 Revisi.

    PERUBAHAN UTAMA dari versi sebelumnya:
      Sebelumnya: spot_dominance menghukum futures CVD
      Sekarang  : Combined Demand Strength — keduanya bisa valid

    Layer 1 — Combined Demand Strength
      Spot dan Futures CVD diukur bersama dengan bobot berbeda:
        Spot  : bobot 1.3 (lebih organik, tetap diutamakan)
        Futures: bobot 1.0 (valid, tidak dihukum kecuali leverage bermasalah)

      Confluence Bonus: kalau keduanya positif dan searah,
      sinyal mendapat booster ekstra karena dua market mengkonfirmasi.

    Layer 2 — Context Quality Multipliers (tidak berubah)
      VWAP, OI, FR sebagai pengali kepercayaan.
      OI dan FR yang menentukan apakah futures CVD "sehat" atau "overdrive".

    Layer 3 — Final Score (tidak berubah)
      quality = vwap_mod × oi_mod × fr_mod
      final   = clamp(raw × quality, 0, 100)
    """
    flags:    List[str] = []
    contexts: List[str] = []
    eps = 1e-6

    # ══════════════════════════════════════════════════════════════════
    # LAYER 1 — Combined Demand Strength
    # ══════════════════════════════════════════════════════════════════

    # Normalisasi masing-masing CVD ke rentang -1 sampai +1
    total_mag  = abs(delta_cvd_spot) + abs(delta_cvd_fut) + eps
    spot_norm  = delta_cvd_spot / total_mag
    fut_norm   = delta_cvd_fut  / total_mag

    # Weighted demand: spot lebih diutamakan tapi futures tidak dihukum
    WEIGHT_SPOT = 1.3
    WEIGHT_FUT  = 1.0
    combined_demand = (
        (spot_norm * WEIGHT_SPOT + fut_norm * WEIGHT_FUT)
        / (WEIGHT_SPOT + WEIGHT_FUT)
    )
    # range: -1.0 (keduanya bearish total) sampai +1.0 (keduanya bullish total)

    # ── Confluence Bonus ──────────────────────────────────────────────
    # Kalau spot DAN futures keduanya bergerak searah ke atas,
    # ini adalah konfirmasi terkuat — dua market bicara hal yang sama
    # Threshold minimum agar confluence bonus aktif
    # CVD keduanya harus cukup signifikan, bukan sekadar positif
    _cvd_min_confluence = 2.0   # minimal 2% normalized CVD

    if delta_cvd_spot > _cvd_min_confluence and delta_cvd_fut > _cvd_min_confluence:
        conf_strength    = min(abs(spot_norm) * abs(fut_norm) * 4, 1.0)
        confluence_bonus = 1.0 + conf_strength * 0.18
        flags.append("CONFLUENCE")
        contexts.append(
            f"🤝 Konfluensi Spot+Futures — "
            f"kedua market bullish (bonus ×{confluence_bonus:.2f})"
        )
    elif delta_cvd_spot > 0 and delta_cvd_fut > 0:
        # Keduanya positif tapi lemah — bonus kecil, tidak ada flag
        confluence_bonus = 1.05
    elif delta_cvd_spot < 0 and delta_cvd_fut < 0:
        confluence_bonus = 0.85
    else:
        confluence_bonus = 1.0

    # ── Basis Modifier ────────────────────────────────────────────────
    # Basis menginformasikan konteks harga, bukan menghukum futures CVD
    if basis_pct > 0.3:
        # Futures jauh lebih mahal → kemungkinan leverage overdrive
        # Nanti OI + FR yang akan menentukan hukumannya
        basis_mod = 0.88
        contexts.append(f"⚠️ Basis +{basis_pct:.3f}% — Futures premium")
    elif basis_pct > 0.1:
        basis_mod = 0.94
    elif basis_pct < -0.05:
        # Spot memimpin futures → organik, perkuat sinyal
        basis_mod = 1.08
    else:
        basis_mod = 1.0   # netral

    # ── Konfirmasi arah harga ─────────────────────────────────────────
    if delta_price > 1.5:
        price_conf = 1.2
    elif delta_price > 0:
        price_conf = 1.0
    elif delta_price > -1.0:
        price_conf = 0.7
    else:
        price_conf = 0.4

    # Raw score (sebelum quality modifier Layer 2)
    raw_score = combined_demand * 60.0 * price_conf * confluence_bonus * basis_mod

    # ══════════════════════════════════════════════════════════════════
    # LAYER 2 — Context Quality Multipliers
    # ══════════════════════════════════════════════════════════════════

    # ── VWAP Modifier — Adaptif berbasis volume ───────────────────────
    # Threshold overextended disesuaikan dengan kondisi volume:
    # Volume tinggi → harga "berhak" jauh dari VWAP (threshold longgar)
    # Volume tipis  → harga jauh dari VWAP lebih mencurigakan (threshold ketat)
    # vol_ratio dipass dari luar melalui parameter (default 1.0 jika tidak ada)

    _vol_ratio_for_vwap = vol_ratio
    # Threshold dasar dari CONFIG, disesuaikan secara proporsional
    _overext  = CONFIG["VWAP_OVEREXT_PCT"]   # 5.0%
    _danger   = CONFIG["VWAP_DANGER_PCT"]    # 8.0%
    _sideways = CONFIG["VWAP_SIDEWAYS_PCT"]  # 1.5%

    # =========================
    # VWAP ADAPTIVE THRESHOLD (BASED ON VOLUME)
    # =========================
    if _vol_ratio_for_vwap > 2.0:
        _overext *= 0.8     # lebih ketat (volume tinggi)
        _danger  *= 0.85

    elif _vol_ratio_for_vwap < 0.7:
        _overext *= 1.4     # lebih longgar (volume tipis)
        _danger  *= 1.3

    # Clamp biar nggak terlalu ekstrem
    _overext = max(2.5, min(_overext, 10))
    _danger  = max(4.0, min(_danger, 12))

    abs_vwap = abs(d_vwap)
    if d_vwap < -_overext:
        # Harga jauh di bawah VWAP → diskon dalam, akumulasi sangat valid
        vwap_mod = 1.45
    elif d_vwap < 0:
        vwap_mod = 1.2
    elif abs_vwap <= _sideways:
        vwap_mod = 1.1
    elif d_vwap <= _overext:
        # Sedikit di atas VWAP → mulai hati-hati
        vwap_mod = 0.85
    elif d_vwap <= _danger:
        # Overextended tapi belum bahaya (5-8%)
        vwap_mod = 0.5
        flags.append("E_DISTRIBUTION")
        contexts.append(f"E: Overextended +{d_vwap:.1f}% dari VWAP 🪤")
    else:
        # > 8% — sangat overextended → bahaya
        vwap_mod = 0.2
        flags.append("E_DISTRIBUTION")
        contexts.append(f"E: Sangat Overextended +{d_vwap:.1f}% 🪤 BAHAYA")

    # ── OI Modifier ───────────────────────────────────────────────────
   # ── OI Modifier — Goldilocks Range ───────────────────────────────
    # Squeeze dideteksi berdasarkan Goldilocks range, bukan threshold tunggal
    #
    # delta_oi < -OI_NOISE_PCT (< -1.5%)    → noise, tidak ada sinyal squeeze
    # -OI_IGNITION_MAX < delta_oi < -OI_IGNITION_MIN (1.5-4% turun) → IGNITION
    # delta_oi < -OI_EXHAUSTED_PCT (< -8%)  → squeeze hampir exhausted
    # delta_oi > OI_PARABOLIC_PCT (> 15%)   → overleverage
    _oi_noise    = CONFIG["OI_NOISE_PCT"]      # 1.5
    _oi_ign_min  = CONFIG["OI_IGNITION_MIN"]   # 1.5
    _oi_ign_max  = CONFIG["OI_IGNITION_MAX"]   # 4.0
    _oi_exhaust  = CONFIG["OI_EXHAUSTED_PCT"]  # 8.0
    _oi_parab    = CONFIG["OI_PARABOLIC_PCT"]  # 15.0

    if delta_oi < -_oi_exhaust:
        if squeeze_type == "long":
            oi_mod = 0.7
            contexts.append(f"⚠️ Long Squeeze Exhausted — OI {delta_oi:+.1f}% turun dalam")
        else:
            oi_mod = 1.1
            flags.append("C_SQUEEZE")
            contexts.append(f"C: Squeeze Exhausted — OI {delta_oi:+.1f}% ⚠️")
    elif delta_oi < -_oi_ign_max:
        if squeeze_type == "long":
            oi_mod = 0.55
            contexts.append(f"💀 Long Squeeze Aktif — OI {delta_oi:+.1f}% turun, harga ikut turun")
        else:
            oi_mod = 1.35
            if "C_SQUEEZE" not in flags:
                flags.append("C_SQUEEZE")
                contexts.append(f"C: Squeeze Aktif — OI {delta_oi:+.1f}% 🔫")
    elif delta_oi < -_oi_ign_min:
        if squeeze_type == "long":
            oi_mod = 0.6
            contexts.append(f"💀 Long Squeeze Ignition — OI {delta_oi:+.1f}% ⚠️")
        else:
            oi_mod = 1.45
            if "C_SQUEEZE" not in flags:
                flags.append("C_SQUEEZE")
                contexts.append(f"C: Squeeze Ignition — OI {delta_oi:+.1f}% 🔫🔥")
    elif delta_oi < 0:
        oi_mod = 1.0
    elif delta_oi <= 4.0:
        oi_mod = 1.0
    elif delta_oi <= _oi_parab:
        oi_mod = 0.75
    else:
        oi_mod = 0.3
        flags.append("D_EXHAUSTION")
        contexts.append(f"D: OI Parabolik {delta_oi:+.1f}% 💀 Overleverage")

    # ── Funding Rate Modifier ─────────────────────────────────────────
    fr = funding_rate
    if fr < -0.0010:
        fr_mod = 1.4
        if "C_SQUEEZE" not in flags:
            flags.append("C_SQUEEZE")
        contexts.append(f"C: FR Sangat Negatif ({fr*100:+.4f}%) 🔫")
    elif fr < -0.0003:
        fr_mod = 1.2
    elif fr < CONFIG["FR_NEGATIVE"]:
        fr_mod = 1.08
    elif fr <= CONFIG["FR_OVERLEVERAGE"]:
        fr_mod = 1.0
    elif fr <= 0.0005:
        fr_mod = 0.82
    elif fr <= 0.0010:
        fr_mod = 0.6
        if "D_EXHAUSTION" not in flags:
            flags.append("D_EXHAUSTION")
            contexts.append(f"D: FR Tinggi ({fr*100:+.4f}%) 💀")
    else:
        fr_mod = 0.35
        if "D_EXHAUSTION" not in flags:
            flags.append("D_EXHAUSTION")
        contexts.append(f"D: FR Ekstrem ({fr*100:+.4f}%) 💀 BAHAYA")

    # ── FR Velocity Modifier ──────────────────────────────────────────
    if fr_velocity > 0.0003:
        fr_mod *= 0.55
        contexts.append(
            f"💥 FR Velocity +{fr_velocity*100:.4f}%/siklus — "
            f"FOMO akut, potensi blow-off top"
        )
    elif fr_velocity > 0.0001:
        fr_mod *= 0.80
    elif fr_velocity < -0.0003:
        fr_mod *= 1.20
        contexts.append(
            f"🔫 FR Velocity {fr_velocity*100:.4f}%/siklus — "
            f"Shorts menumpuk, squeeze fuel bertambah"
        )
    elif fr_velocity < -0.0001:
        fr_mod *= 1.08

    # ══════════════════════════════════════════════════════════════════
    # LAYER 3 — Final Score
    # ══════════════════════════════════════════════════════════════════
    quality = vwap_mod * oi_mod * fr_mod

    # ── Capped Multiplicative ─────────────────────────────────────────
    # Quality boleh menekan skor ke bawah tanpa batas
    # Tapi tidak boleh mengangkat raw_score lebih dari 50%
    # Ini mencegah sinyal lemah meledak jadi 90+ hanya karena
    # semua multiplier kebetulan bagus
    quality_up_cap = 1.50
    if quality > quality_up_cap:
        quality = quality_up_cap
    quality = max(0.08, quality)

    final_score = max(0.0, min(100.0, raw_score * quality))

    # ── Identifikasi skenario utama ───────────────────────────────────
    if combined_demand > 0 and raw_score > 0:
        if spot_norm > 0.25 and spot_norm >= fut_norm:
            # Spot memimpin atau setara
            flags.insert(0, "A")
            contexts.insert(
                0,
                f"A: Spot Dominan {spot_norm*100:.0f}% "
                f"(quality×{quality:.2f} → {final_score:.1f}pts)"
            )
        elif fut_norm > 0.25:
            # Futures memimpin tapi demand sehat (OI dan FR yang akan menentukan)
            flags.insert(0, "A_FUTURES")
            contexts.insert(
                0,
                f"A: Futures Memimpin {fut_norm*100:.0f}% "
                f"(quality×{quality:.2f} → {final_score:.1f}pts)"
            )
    elif combined_demand < -0.25 and delta_price > 0:
        flags.append("B_SPECULATIVE")
        contexts.append(
            f"B: Demand Lemah — harga naik tanpa CVD ({combined_demand*100:.0f}%)"
        )

    return round(final_score, 1), flags, contexts

def calc_long_squeeze_score(
    delta_price:  float,
    delta_oi:     float,
    funding_rate: float,
    delta_cvd_spot: float,
    delta_cvd_fut:  float,
    d_vwap:       float,
    squeeze_type: str,
    squeeze_stage: str,
    squeeze_fuel: float,
) -> Tuple[float, List[str], List[str]]:
    """
    Hitung skor sinyal SHORT dari kondisi Long Squeeze.

    Terpisah dari scoring bullish karena logika dan targetnya berbeda.
    Skor 0–100: semakin tinggi = long squeeze semakin kuat = SHORT semakin valid.

    Returns: (short_score, flags, contexts)
    """
    if squeeze_type not in ("long", "long_exhausted"):
        return 0.0, [], []

    flags:    List[str] = []
    contexts: List[str] = []

    # ── Base score dari kekuatan long squeeze ─────────────────────────
    # Fuel tersisa = seberapa jauh squeeze masih bisa berlanjut
    base = squeeze_fuel * 0.6   # max 60 poin dari fuel

    # ── CVD Konfirmasi ────────────────────────────────────────────────
    # CVD negatif mengkonfirmasi tekanan jual agresif
    both_negative = delta_cvd_spot < 0 and delta_cvd_fut < 0
    if both_negative:
        cvd_bonus = 15.0   # konfluensi bearish
        flags.append("CVD_CONFLUENCE_BEARISH")
        contexts.append("🔴 CVD Spot+Futures keduanya negatif — tekanan jual kuat")
    elif delta_cvd_fut < 0:
        cvd_bonus = 8.0
    else:
        cvd_bonus = 0.0

    # ── VWAP Context ──────────────────────────────────────────────────
    # Harga jauh di atas VWAP saat long squeeze = distribusi dari premium
    if d_vwap > 3.0:
        vwap_bonus = 12.0
        contexts.append(f"📏 Harga +{d_vwap:.1f}% di atas VWAP — distribusi dari premium")
    elif d_vwap > 1.0:
        vwap_bonus = 6.0
    elif d_vwap < 0:
        vwap_bonus = 0.0   # harga sudah di bawah VWAP, squeeze mungkin terlambat
        base *= 0.7
    else:
        vwap_bonus = 3.0

    # ── FR Modifier ───────────────────────────────────────────────────
    if funding_rate > 0.0010:
        fr_bonus = 10.0
        contexts.append(f"💀 FR {funding_rate*100:+.4f}% — banyak longs yang belum kena")
    elif funding_rate > 0.0005:
        fr_bonus = 6.0
    elif funding_rate > 0.0001:
        fr_bonus = 3.0
    else:
        fr_bonus = 0.0

    short_score = base + cvd_bonus + vwap_bonus + fr_bonus
    short_score = max(0.0, min(100.0, short_score))

    # ── Flags ──────────────────────────────────────────────────────────
    if squeeze_type == "long":
        flags.insert(0, "LONG_SQUEEZE")
        contexts.insert(0, f"💀 Long Squeeze {squeeze_stage.title()} — SHORT signal")
    elif squeeze_type == "long_exhausted":
        flags.insert(0, "LONG_SQUEEZE_EXHAUSTED")
        contexts.insert(0, "✅ Long Squeeze Exhausted — potensi reversal LONG")

    return round(short_score, 1), flags, contexts

def calc_distribution_score(
    delta_cvd_spot: float,
    delta_cvd_fut:  float,
    delta_oi:       float,
    funding_rate:   float,
    d_vwap:         float,
    delta_price:    float,
) -> Tuple[float, List[str], List[str]]:
    """
    Deteksi Distribusi Aktif — sinyal SHORT standalone.

    Kondisi yang terdeteksi:
      CVD keduanya negatif (Spot & Futures sama-sama jual agresif)
      + OI naik (posisi baru dibuka = longs baru masuk tapi sia-sia)
      + FR positif (market condong long = orang-orang belum sadar distribusi)
      + Harga turun atau stagnan

    Ini adalah kondisi distribusi aktif paling jelas:
    "Smart money jual agresif, retail masih beli dengan leverage"

    Berbeda dari Long Squeeze (yang butuh OI TURUN),
    ini adalah distribusi awal sebelum squeeze terjadi.

    Skor: 0–100 (semakin tinggi = distribusi semakin kuat = SHORT semakin valid)
    """
    flags:    List[str] = []
    contexts: List[str] = []

    # Syarat minimum: keduanya harus bearish CVD
    both_bearish = delta_cvd_spot < 0 and delta_cvd_fut < 0
    if not both_bearish:
        return 0.0, [], []

    # Tambahan: harga tidak boleh sedang naik kencang
    # (kalau naik kencang dengan CVD negatif, itu skenario lain)
    if delta_price > 2.0:
        return 0.0, [], []

    score = 0.0

    # ── Base: kekuatan bearish CVD ──────────────────────────────────────
    # Semakin negatif combined CVD → distribusi semakin kuat
    combined_cvd = (delta_cvd_spot * 1.0 + delta_cvd_fut * 1.3) / 2.3
    cvd_strength = min(abs(combined_cvd) / 10.0, 1.0)   # normalize 0–1
    base_score   = cvd_strength * 40.0                   # max 40 poin dari CVD
    score += base_score
    flags.append("DISTRIBUTION")
    contexts.append(
        f"📉 Distribusi Aktif: CVD Spot{delta_cvd_spot:+.1f}% "
        f"Fut{delta_cvd_fut:+.1f}% — keduanya jual agresif"
    )

    # ── OI naik + harga turun = longs baru masuk sia-sia ────────────────
    if delta_oi > 2.0 and delta_price <= 0:
        oi_bonus = min((delta_oi - 2.0) / 8.0, 1.0) * 20.0
        score += oi_bonus
        contexts.append(
            f"⚠️ OI +{delta_oi:.1f}% tapi harga {delta_price:+.2f}% — "
            f"longs baru masuk sia-sia"
        )

    # ── FR positif = banyak longs yang akan kena ────────────────────────
    if funding_rate > 0.0005:
        fr_bonus = 15.0
        flags.append("D_EXHAUSTION")
        contexts.append(f"💀 FR {funding_rate*100:+.4f}% — longs menumpuk")
        score += fr_bonus
    elif funding_rate > 0.0001:
        score += 8.0

    # ── VWAP: distribusi dari premium lebih berbahaya ────────────────────
    if d_vwap > 5.0:
        vwap_bonus = 15.0
        contexts.append(f"📏 Distribusi dari +{d_vwap:.1f}% di atas VWAP — premium tinggi")
        score += vwap_bonus
    elif d_vwap > 2.0:
        score += 8.0
    elif d_vwap < -2.0:
        # Distribusi di bawah VWAP — sinyal lebih lemah
        score *= 0.7

    # ── Harga turun mengkonfirmasi distribusi ────────────────────────────
    if delta_price < -1.0:
        score *= 1.15
    elif delta_price < 0:
        score *= 1.05

    score = max(0.0, min(100.0, score))
    return round(score, 1), flags, contexts


# ═══════════════════════════════════════════════════════════════════════
# 🎯  GRADE SYSTEM
# ═══════════════════════════════════════════════════════════════════════
def get_grade(score: float) -> Tuple[str, str]:
    """Convert skor 0–100 ke (grade_letter, display_label)."""
    for threshold, grade, label in GRADE_SCALE:
        if score >= threshold:
            return grade, label
    return "D", "⚪ NO SIGNAL"


def _scenario_summary(flags: List[str]) -> str:
    """Buat ringkasan emoji dari skenario aktif."""
    parts = []
    if "A"              in flags: parts.append("🟢 Spot Accum")
    if "B_SPECULATIVE"  in flags: parts.append("🟡 Spec Rally")
    if "C_SQUEEZE"      in flags: parts.append("🔫 Squeeze")
    if "D_EXHAUSTION"   in flags: parts.append("💀 Overleveraged")
    if "E_DISTRIBUTION" in flags: parts.append("🪤 Distribution")
    return " │ ".join(parts) if parts else "─ Neutral"


# ═══════════════════════════════════════════════════════════════════════
# 📤  TELEGRAM NOTIFIER
# ═══════════════════════════════════════════════════════════════════════
async def send_telegram(
    session: aiohttp.ClientSession,
    message: str,
) -> bool:
    """Kirim pesan HTML ke Telegram chat/group yang dikonfigurasi."""
    url = f"{TELEGRAM_API}/bot{CONFIG['TELEGRAM_TOKEN']}/sendMessage"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with session.post(
            url,
            json={
                "chat_id":    CONFIG["TELEGRAM_CHAT_ID"],
                "text":       message,
                "parse_mode": "HTML",
            },
            timeout=timeout,
        ) as resp:
            ok = resp.status == 200
            if not ok:
                body = await resp.text()
                log.warning(f"Telegram HTTP {resp.status}: {body[:120]}")
            return ok
    except Exception as exc:
        log.error(f"send_telegram error: {exc}")
        return False


def build_telegram_message(r: Dict, signal_type: str = "LONG") -> str:
    """Format pesan Telegram — ringkas dan decisional."""
    from datetime import timezone, timedelta

    sym     = r["symbol"].replace("USDT", "")
    wib     = timezone(timedelta(hours=7))
    now     = datetime.now(wib).strftime("%d %b %Y  %H:%M WIB")

    # ── Pilih data berdasarkan tipe sinyal ────────────────────────────
    if signal_type == "SHORT":
        score      = r.get("ls_score", 0)
        flags      = r.get("ls_flags", [])
        contexts   = r.get("ls_contexts", [])
        tgt        = r.get("ls_targets", {})
        sq_type    = r.get("squeeze_type", "none")
        is_reversal = sq_type == "long_exhausted"
    else:
        score      = r["score"]
        flags      = r.get("flags", [])
        contexts   = r.get("contexts", [])
        tgt        = r.get("targets", {})
        is_reversal = False
        
    # ── Skor & Grade ─────────────────────────────────────────────────
    if signal_type == "SHORT":
        if is_reversal:
            grade_emoji = "🔄"
            grade_label = "🔄 REVERSAL SIGNAL"
            grade       = "REV"
        else:
            # Grade SHORT berdasarkan ls_score
            if score >= 85:   grade_emoji, grade_label, grade = "🔥", "🔥 PRIME SHORT", "A+"
            elif score >= 70: grade_emoji, grade_label, grade = "🩸", "🩸 STRONG SHORT", "A"
            elif score >= 55: grade_emoji, grade_label, grade = "🟠", "🟠 DECENT SHORT", "B+"
            else:             grade_emoji, grade_label, grade = "📊", "📊 WATCH SHORT", "B"
    else:
        grade_emoji = {
            "A+": "🔥", "A": "✅", "B+": "🟡",
            "B": "📊", "C": "⚠️", "D": "⚪",
        }.get(r["grade"], "📊")
        grade_label = r["grade_label"]
        grade       = r["grade"]

    # ── Konteks utama (satu kalimat) ──────────────────────────────────
    spot_pct = ""
    main_ctx = ""
    for c in r["contexts"]:
        if c.startswith("A:") and "Futures Memimpin" in c:
            try:
                fut_pct = c.split("Futures Memimpin")[1].split("%")[0].strip()
                spot_pct = f"Futures Demand {fut_pct}%"
            except Exception:
                spot_pct = "Futures Demand"
            break
        elif c.startswith("A:"):
            try:
                spot_pct = c.split("Spot Dominan")[1].split("%")[0].strip()
                spot_pct = f"Spot Dominan {spot_pct}%"
            except Exception:
                spot_pct = "Spot Dominan"
            break
        elif c.startswith("B:") and "Futures Memimpin" in c:
            spot_pct = "Futures Memimpin ⚠️"
            break
        elif c.startswith("B:"):
            spot_pct = "Demand Lemah ⚠️"
            break

    # Sub-konteks: volume dan squeeze
    sub_parts = []
    vol_ratio = r.get("vol_ratio", 1.0)
    if vol_ratio >= 2.5:
        sub_parts.append(f"volume {vol_ratio:.1f}x konfirmasi")
    elif vol_ratio >= 1.5:
        sub_parts.append(f"volume {vol_ratio:.1f}x elevated")
    elif vol_ratio < 0.5:
        sub_parts.append("volume sepi")

    sq_stage = r.get("squeeze_stage", "none")
    if sq_stage == "early":
        sub_parts.append("squeeze fresh")
    elif sq_stage == "mid":
        sub_parts.append("squeeze aktif")
    elif sq_stage == "late":
        sub_parts.append("squeeze hampir habis")
    elif sq_stage == "exhausted":
        sub_parts.append("squeeze exhausted ⚠️")

    # Arah
    direction = tgt.get("direction", "NEUTRAL")
    if "BULLISH" in direction and "SPECULATIVE" not in direction:
        arah = "BULLISH"
    elif "SPECULATIVE" in direction:
        arah = "SPECULATIVE BULLISH ⚠️"
    elif "BEARISH" in direction:
        arah = "BEARISH / HINDARI LONG"
    else:
        arah = direction

    main_ctx = f"🧭 {arah} — {spot_pct}"
    sub_ctx  = f"    {', '.join(sub_parts)}" if sub_parts else ""

    # ── Target & Invalidasi ───────────────────────────────────────────
    pr = r["price"]

    def _fmt(p):
        if p is None:
            return "─"
        return f"${p:,.5g}"

    def _pct(p):
        if p is None:
            return ""
        return f"  ({(p - pr) / pr * 100:+.2f}%)"

    t1 = tgt.get("target1")
    t2 = tgt.get("target2")
    iv = tgt.get("invalidasi")

    # ── Signals ringkas ───────────────────────────────────────────────
    signal_parts = []
    if "A"                       in flags: signal_parts.append("🟢 Spot Accum")
    if "A_FUTURES"               in flags: signal_parts.append("🟡 Futures Demand")
    if "CONFLUENCE"              in flags: signal_parts.append("🤝 Konfluensi")
    if "B_SPECULATIVE"           in flags: signal_parts.append("🔴 Spec Rally")
    if "C_SQUEEZE"               in flags:
        fuel = r.get("squeeze_fuel", 0)
        signal_parts.append(f"🔫 Short Squeeze {fuel:.0f}%")
    if "LONG_SQUEEZE"            in flags:
        fuel = r.get("squeeze_fuel", 0)
        signal_parts.append(f"💀 Long Squeeze {fuel:.0f}%")
    if "LONG_SQUEEZE_EXHAUSTED"  in flags: signal_parts.append("🔄 LS Exhausted")
    if "CVD_CONFLUENCE_BEARISH"  in flags: signal_parts.append("🔴 CVD Bearish")
    if "D_EXHAUSTION"            in flags: signal_parts.append("💀 Overleveraged")
    if "E_DISTRIBUTION"          in flags: signal_parts.append("🪤 Distribution")
    if "ABSORPTION"              in flags: signal_parts.append("🧱 Absorption")
    signals_str = "  │  ".join(signal_parts) if signal_parts else "─ Neutral"

    # ── Warnings ──────────────────────────────────────────────────────
    warnings = []
    if "B_SPECULATIVE" in flags and "A" not in flags:
        warnings.append("⚠️ Speculative — tidak ada konfirmasi spot")
    if "E_DISTRIBUTION" in flags:
        warnings.append("🪤 Distribution trap — hindari long")
    if "D_EXHAUSTION" in flags:
        warnings.append("💀 Overleverage — long squeeze risk")
    if "ABSORPTION" in flags:
        warnings.append("🧱 Absorption detected — iceberg sell")
    if "LONG_SQUEEZE" in flags:
        warnings.append("💀 Long Squeeze aktif — HINDARI LONG, pertimbangkan SHORT")
    if "LONG_SQUEEZE_EXHAUSTED" in flags:
        warnings.append("🔄 Long Squeeze selesai — setup reversal/bounce terbentuk")
    # CVD-Vol warning dari contexts
    for c in r.get("contexts", []):
        if "CVD-Vol Inconsistent" in c:
            warnings.append("⚠️ CVD momentum memudar — volume tidak konfirmasi")
            break
        elif "CVD-Vol Lemah" in c:
            warnings.append("🟡 CVD momentum lemah — konfirmasi tipis")
            break
    # FR velocity warning
    for c in r.get("contexts", []):
        if "FOMO akut" in c:
            warnings.append("💥 FR naik cepat — potensi blow-off top")
            break
        elif "Shorts menumpuk" in c:
            warnings.append("🔫 FR turun cepat — squeeze fuel bertambah")
            break

    # ── Rakit pesan ───────────────────────────────────────────────────
    sep = "─" * 34
    lines = [
        f"{grade_emoji} {grade_label}",
        "",
        f"<b>📌 #{sym}USDT</b>  │  <code>${pr:,.5g}</code>",
        f"⏰ {now}",
        sep,
        f"<b>🏆 {score:.1f} / 100  [ {grade} ]</b>",
        "",
        main_ctx,
    ]

    if sub_ctx:
        lines.append(sub_ctx)

    lines.append(sep)

    # Target hanya tampil kalau ada
    if t1 or t2 or iv:
        if t1:
            lines.append(f"📈 Target 1  :  <code>{_fmt(t1)}</code>{_pct(t1)}")
        if t2:
            lines.append(f"📈 Target 2  :  <code>{_fmt(t2)}</code>{_pct(t2)}")
        if iv:
            lines.append(f"🛑 Invalidasi:  <code>{_fmt(iv)}</code>{_pct(iv)}")
        lines.append(sep)

    lines.append(f"🎯 {signals_str}")

    # Warning di paling bawah
    if warnings:
        lines.append(sep)
        for w in warnings:
            lines.append(w)

    return "\n".join(lines)


def build_startup_message() -> str:
    cycle_lines = "\n".join(
        f"  {i+1}. {c['label']}: max {c['max_coins']} koin, "
        f"min vol ${c['min_volume']/1e6:.0f}M"
        for i, c in enumerate(CONFIG["CYCLES"])
    )
    return (
        "🤖 <b>AKSA Microstructure Screener v2.0 aktif!</b>\n\n"
        "📐 Engine  : <b>Interdependency Scoring Matrix</b>\n"
        "📡 Data    : Spot CVD + Futures CVD + OI + FR + VWAP\n"
        f"⏱ Interval: <b>{CONFIG['SCAN_INTERVAL_MIN']} menit per siklus</b>\n"
        f"🎯 Min Score: <b>{CONFIG['MIN_SCORE']}/100</b>\n\n"
        f"<b>🔄 Sistem 3 Siklus:</b>\n{cycle_lines}\n\n"
        "<b>Skenario Aktif:</b>\n"
        "  🟢 A — Organic Spot Accumulation (max +40)\n"
        "  🟡 B — Speculative Futures Rally  (max +15)\n"
        "  🔫 C — Short Squeeze Fuel         (+25)\n"
        "  💀 D — Exhaustion Penalty         (−35)\n"
        "  🪤 E — Distribution Trap          (cap 30)"
    )


# ═══════════════════════════════════════════════════════════════════════
# 🔍  SINGLE-COIN SCANNER  (async)
# ═══════════════════════════════════════════════════════════════════════
async def scan_coin(
    session:   aiohttp.ClientSession,
    symbol:    str,
    semaphore: asyncio.Semaphore,
) -> Optional[Dict]:
    """
    Pipeline analisis lengkap untuk satu simbol.
    Semua HTTP call dijalankan secara paralel (asyncio.gather).
    Return: dict hasil, atau None jika data tidak cukup.
    """
    async with semaphore:
        tf      = CONFIG["TIMEFRAME"]
        limit   = CONFIG["FETCH_LIMIT"]
        N       = CONFIG["LOOKBACK_N"]
        oi_per  = OI_PERIOD_MAP.get(tf, "15m")

        try:
            # ── Ambil semua data secara paralel ────────────────────────
            fut_df, spot_df, funding_data, oi_hist = await asyncio.gather(
                fetch_futures_klines(session, symbol, tf, limit),
                fetch_spot_klines(session, symbol, tf, limit),
                fetch_funding_rate(session, symbol),
                fetch_oi_history(session, symbol, oi_per, N + 5),
                return_exceptions=False,
            )
            # Unpack tuple (fr, basis_pct) dari fetch_funding_rate
            if funding_data and isinstance(funding_data, tuple):
                _raw_fr, basis_pct = funding_data
            else:
                _raw_fr, basis_pct = None, 0.0

            # ── Validasi data minimal ──────────────────────────────────
            if fut_df is None or len(fut_df) < N + 2:
                return None   # data futures tidak cukup → skip

            current_price = float(fut_df["close"].iloc[-1])

            # ── Rolling VWAP ───────────────────────────────────────────
            vwap, d_vwap = calc_rolling_vwap(fut_df, N)

            # ── CVD Futures ────────────────────────────────────────────
            _, delta_cvd_fut = calc_cvd_and_momentum(fut_df, N)

            # ── CVD Spot ───────────────────────────────────────────────
            spot_available = spot_df is not None and len(spot_df) >= N + 2
            if spot_available:
                _, delta_cvd_spot = calc_cvd_and_momentum(spot_df, N)
            else:
                # Spot tidak tersedia (perp-only atau listing baru)
                # Asumsikan nol — skenario A tidak akan aktif tanpa spot data
                delta_cvd_spot = 0.0

            # ── OI Momentum ────────────────────────────────────────────
            delta_oi = calc_oi_momentum(oi_hist) if oi_hist else 0.0

            # ── Price Momentum ─────────────────────────────────────────
            delta_price = calc_price_momentum(fut_df, N)

            # ── Short-term Price Momentum (5 candle terakhir) ──────────
            delta_price_short = calc_price_momentum(fut_df, 5)

            # ── Funding Rate & Velocity ────────────────────────────────
            fr = _raw_fr if _raw_fr is not None else 0.0

            # FR Velocity: selisih FR sekarang vs FR scan sebelumnya
            now_ts = time.time()
            prev_entry = _FR_HISTORY_4H.get(symbol)

            if prev_entry is None:
                fr_velocity = 0.0
                _FR_HISTORY_4H[symbol] = (fr, now_ts)
            else:
                prev_fr, prev_ts = prev_entry
                if now_ts - prev_ts >= _FR_VELOCITY_INTERVAL:
                    fr_velocity = fr - prev_fr
                    _FR_HISTORY_4H[symbol] = (fr, now_ts)
                else:
                    fr_velocity = fr - prev_fr  # gunakan data lama, tidak update dulu

            # ── FORECAST ENGINE ────────────────────────────────────────
            tf_min = TF_MINUTES.get(CONFIG["TIMEFRAME"], 60)
            vol_ratio, vol_label = calc_volume_anomaly(fut_df, N, tf_min)

            # ── Early exit: short-term momentum sudah berbalik ─────────
            # Kalau 5 candle terakhir sudah turun > 1%, skip sinyal LONG
            # meskipun long-term (48 candle) masih positif
            if delta_price_short < -1.0:
                return None

            # ── Squeeze Stage —───────────
            squeeze_type, squeeze_stage, squeeze_desc, squeeze_fuel = calc_squeeze_stage(
                delta_oi     = delta_oi,
                funding_rate = fr,
                delta_price  = delta_price,
            )

            # ── SCORING MATRIX ─────────────────────────────────────────
            score, flags, contexts = calculate_market_score(
                d_vwap         = d_vwap,
                delta_cvd_spot = delta_cvd_spot,
                delta_cvd_fut  = delta_cvd_fut,
                delta_oi       = delta_oi,
                delta_price    = delta_price,
                funding_rate   = fr,
                basis_pct      = basis_pct,
                fr_velocity    = fr_velocity,
                squeeze_type   = squeeze_type,
            )
            
            # ── ABSORPTION DETECTION ───────────────────────────────────
            absorption_penalty, absorption_desc = calc_absorption(
                fut_df         = fut_df,
                delta_cvd_spot = delta_cvd_spot,
                delta_cvd_fut  = delta_cvd_fut,
                delta_price    = delta_price,
                n              = N,
            )
            if absorption_penalty < 1.0:
                score = round(score * absorption_penalty, 1)
                contexts.append(absorption_desc)
                flags.append("ABSORPTION")
                
            # ── CVD-VOLUME CONSISTENCY ─────────────────────────────────
            cvd_vol_mult, cvd_vol_desc = calc_cvd_volume_consistency(
                delta_cvd_spot = delta_cvd_spot,
                delta_cvd_fut  = delta_cvd_fut,
                vol_ratio      = vol_ratio,
            )
            if cvd_vol_mult != 1.0:
                score = round(min(100.0, score * cvd_vol_mult), 1)
            if cvd_vol_desc:
                contexts.append(cvd_vol_desc)

            targets = calc_price_targets(
                price         = current_price,
                vwap          = vwap,
                d_vwap        = d_vwap,
                df            = fut_df,
                n             = N,
                flags         = flags,
                squeeze_stage = squeeze_stage,
            )

            # ── LONG SQUEEZE SCORE ─────────────────────────────────────
            ls_score, ls_flags, ls_contexts = calc_long_squeeze_score(
                delta_price   = delta_price,
                delta_oi      = delta_oi,
                funding_rate  = fr,
                delta_cvd_spot = delta_cvd_spot,
                delta_cvd_fut  = delta_cvd_fut,
                d_vwap        = d_vwap,
                squeeze_type  = squeeze_type,
                squeeze_stage = squeeze_stage,
                squeeze_fuel  = squeeze_fuel,
            )

            # Untuk target long squeeze, pass flags yang sudah include ls_flags
            if ls_score >= CONFIG["MIN_SCORE"] and squeeze_type in ("long", "long_exhausted"):
                ls_targets = calc_price_targets(
                    price         = current_price,
                    vwap          = vwap,
                    d_vwap        = d_vwap,
                    df            = fut_df,
                    n             = N,
                    flags         = ls_flags,
                    squeeze_stage = squeeze_stage,
                )
            else:
                ls_targets = {}

            # ── DISTRIBUTION SHORT SCORE ───────────────────────────────
            dist_score, dist_flags, dist_contexts = calc_distribution_score(
                delta_cvd_spot = delta_cvd_spot,
                delta_cvd_fut  = delta_cvd_fut,
                delta_oi       = delta_oi,
                funding_rate   = fr,
                d_vwap         = d_vwap,
                delta_price    = delta_price,
            )

            # Volume anomaly langsung memodifikasi skor akhir
            # Sinyal kuat + volume sepi = kurang konvinsif
            # Sinyal kuat + volume anomali = lebih dipercaya
            if vol_ratio >= 2.5:
                vol_multiplier = 1.10   # +10% kepercayaan
            elif vol_ratio >= 1.5:
                vol_multiplier = 1.04
            elif vol_ratio >= 0.8:
                vol_multiplier = 1.0    # netral
            else:
                vol_multiplier = 0.88   # volume sepi → kurangi kepercayaan
            score = round(min(100.0, score * vol_multiplier), 1)

            grade, grade_label = get_grade(score)

            # ── Print live per koin ───────────────────────────────────
            fr_pct    = fr * 100 if fr else 0.0
            flags_str = ",".join(flags) if flags else "─"
            marker    = " ★" if score >= CONFIG["MIN_SCORE"] else "  "
            log.info(
                f"{marker} {symbol:<18} {score:>6.1f} {grade:>3} │ "
                f"ΔP:{delta_price:>+6.2f}% "
                f"VWAP:{d_vwap:>+5.2f}% "
                f"CVDs:{delta_cvd_spot:>+5.1f}% "
                f"CVDf:{delta_cvd_fut:>+5.1f}% "
                f"OI:{delta_oi:>+5.2f}% "
                f"FR:{fr_pct:>+6.4f}% │ "
                f"{flags_str}"
            )
            
            return {
                "symbol":         symbol,
                "price":          current_price,
                "vwap":           vwap,
                "d_vwap":         d_vwap,
                "delta_cvd_spot": delta_cvd_spot,
                "delta_cvd_fut":  delta_cvd_fut,
                "delta_oi":       delta_oi,
                "delta_price":    delta_price,
                "funding_rate":   fr,
                "score":          score,
                "grade":          grade,
                "grade_label":    grade_label,
                "flags":          flags,
                "contexts":       contexts,
                "spot_available":    spot_available,
                "basis_pct":         basis_pct,
                "fr_velocity":       fr_velocity,
                "absorption_penalty": absorption_penalty,
                # ── forecast fields ────────────────────────────────────
                "vol_ratio":      vol_ratio,
                "vol_label":      vol_label,
                "squeeze_stage":  squeeze_stage,
                "squeeze_desc":   squeeze_desc,
                "squeeze_fuel":   squeeze_fuel,
                "targets":        targets,
                # ── long squeeze fields ────────────────────────────────
                "squeeze_type":   squeeze_type,
                "ls_score":       ls_score,
                "ls_flags":       ls_flags,
                "ls_contexts":    ls_contexts,
                "ls_targets":     ls_targets,
                # ── distribution short fields ──────────────────────────
                "dist_score":    dist_score,
                "dist_flags":    dist_flags,
                "dist_contexts": dist_contexts,
            }
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.info(f"scan_coin [{symbol}] error: {exc}")
            return None


# ═══════════════════════════════════════════════════════════════════════
# 🔁  MAIN SCAN CYCLE
# ═══════════════════════════════════════════════════════════════════════
async def run_scan_batch(
    session: aiohttp.ClientSession,
    symbols: List[str],
    cycle_label: str = "",
) -> None:
    t_start = time.time()
    log.info("═" * 60)
    log.info(f"  🔍 {cycle_label} — {len(symbols)} koin")
    log.info("═" * 60)

    if not symbols:
        log.warning("Batch kosong — skip.")
        return

    semaphore = asyncio.Semaphore(CONFIG["CONCURRENT_TASKS"])
    tasks     = [scan_coin(session, sym, semaphore) for sym in symbols]

    # Jalankan semua scan secara paralel
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    errors  = 0
    for r in raw_results:
        if isinstance(r, Exception):
            errors += 1
        elif r is not None:
            results.append(r)

    # Filter koin yang lolos threshold minimum
    alerts = [
        r for r in results
        if r["score"] >= CONFIG["MIN_SCORE"]
        and not (
            "C_SQUEEZE" in r.get("flags", [])
            and r.get("squeeze_fuel", 100) < 45
            and "A" not in r.get("flags", [])
        )
    ]
    alerts.sort(key=lambda x: x["score"], reverse=True)

    t_elapsed = time.time() - t_start
    log.info("═" * 60)
    log.info(
        f"  ✅ Scan selesai — {len(alerts)} alert / {len(results)} hasil "
        f"/ {errors} error | waktu: {t_elapsed:.1f}s"
    )
    log.info("═" * 60)

    # Ringkasan akhir setelah semua koin selesai
    log.info("  " + "─" * 115)
    log.info(
        f"  📊 Selesai — {len(results)} koin discan │ "
        f"{len(alerts)} lolos (★) │ "
        f"{errors} error │ {t_elapsed:.1f}s"
    )
    if alerts:
        top = alerts[0]
        log.info(
            f"  🏆 Tertinggi: {top['symbol']} "
            f"score={top['score']:.1f} [{top['grade']}] "
            f"flags={','.join(top['flags'])}"
        )
    log.info("  " + "─" * 115)

    # Kirim alert ke Telegram — LONG signals
    for r in alerts:
        msg = build_telegram_message(r, signal_type="LONG")
        ok  = await send_telegram(session, msg)
        status = "✓ terkirim" if ok else "✗ gagal"
        log.info(f"  📤 {r['symbol']:<18} [{r['grade']:>2}] score={r['score']:5.1f} → {status}")
        # ── Dashboard Simulasi ──
        tgt = r.get("targets", {})
        send_signal_to_dashboard(
            symbol=r["symbol"], direction="LONG", entry=r["price"],
            tp=tgt.get("target1") or round(r["price"] * 1.02, 6),
            sl=tgt.get("invalidasi") or round(r["price"] * 0.995, 6),
            grade=r["grade"], leverage=5,
        )
        await asyncio.sleep(CONFIG["MSG_DELAY"])

    # Kirim alert SHORT (long squeeze)
    short_alerts = [
        r for r in results
        if r.get("ls_score", 0) >= CONFIG["MIN_SCORE"]
        and r.get("squeeze_type") in ("long", "long_exhausted")
    ]
    short_alerts.sort(key=lambda x: x.get("ls_score", 0), reverse=True)

    for r in short_alerts:
        msg = build_telegram_message(r, signal_type="SHORT")
        ok  = await send_telegram(session, msg)
        sq_type = "REVERSAL" if r["squeeze_type"] == "long_exhausted" else "SHORT"
        log.info(
            f"  📤 {r['symbol']:<18} [{sq_type}] "
            f"ls_score={r['ls_score']:5.1f} → {'✓ terkirim' if ok else '✗ gagal'}"
        )
        # ── Dashboard Simulasi ──
        tgt = r.get("ls_targets", {})
        send_signal_to_dashboard(
            symbol=r["symbol"], direction="SHORT", entry=r["price"],
            tp=tgt.get("target1") or round(r["price"] * 0.98, 6),
            sl=tgt.get("invalidasi") or round(r["price"] * 1.005, 6),
            grade="A+" if r.get("ls_score", 0) >= 85 else "B", leverage=5,
        )
        await asyncio.sleep(CONFIG["MSG_DELAY"])

    # Kirim DISTRIBUTION SHORT alerts
    dist_alerts = [
        r for r in results
        if r.get("dist_score", 0) >= CONFIG["MIN_SCORE"]
    ]
    dist_alerts.sort(key=lambda x: x.get("dist_score", 0), reverse=True)

    for r in dist_alerts:
        # Buat pesan distribution dengan data yang sudah ada
        dist_r = dict(r)
        dist_r["ls_score"]    = r["dist_score"]
        dist_r["ls_flags"]    = r["dist_flags"]
        dist_r["ls_contexts"] = r["dist_contexts"]
        dist_r["squeeze_type"] = "distribution"
        # Hitung target distribution: ke bawah VWAP
        dist_r["ls_targets"] = {
            "direction":  "SHORT — Distribusi Aktif",
            "target1":    round(r["vwap"], 6),
            "target2":    round(r["vwap"] * 0.985, 6),
            "invalidasi": round(r["price"] * 1.015, 6),
        }
        msg = build_telegram_message(dist_r, signal_type="SHORT")
        ok  = await send_telegram(session, msg)
        log.info(
            f"  📤 {r['symbol']:<18} [DIST SHORT] "
            f"dist_score={r['dist_score']:5.1f} → "
            f"{'✓ terkirim' if ok else '✗ gagal'}"
        )
        # ── Dashboard Simulasi ──
        send_signal_to_dashboard(
            symbol=r["symbol"], direction="SHORT", entry=r["price"],
            tp=round(r["vwap"] * 0.985, 6),
            sl=round(r["price"] * 1.015, 6),
            grade="A+" if r.get("dist_score", 0) >= 85 else "B", leverage=5,
        )
        await asyncio.sleep(CONFIG["MSG_DELAY"])


# ═══════════════════════════════════════════════════════════════════════
# 🚀  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════
async def main_async() -> None:
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║      AKSA MICROSTRUCTURE SCREENER  v2.0                  ║")
    log.info("║      3-Cycle System │ Spot + Futures Async               ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    cycles_cfg = CONFIG["CYCLES"]
    log.info("  Konfigurasi Siklus:")
    for i, c in enumerate(cycles_cfg, 1):
        log.info(f"    Siklus {i}: max {c['max_coins']} koin, min vol ${c['min_volume']/1e6:.0f}M")

    async with create_session() as session:
        await send_telegram(session, build_startup_message())

        full_cycle_count = 0   # berapa kali 3 siklus penuh selesai
        cycle_idx        = 0   # 0, 1, 2 → berputar terus
        partitions       = []  # [list_siklus1, list_siklus2, list_siklus3]

        while True:
            # ── Refresh partisi setiap awal putaran baru (setiap 3 siklus) ──
            if cycle_idx == 0:
                full_cycle_count += 1
                log.info(f"\n{'═'*60}")
                log.info(f"  🔄 Putaran ke-{full_cycle_count} — Refresh daftar koin dari Binance")
                log.info(f"{'═'*60}")
                pool       = await fetch_full_symbol_pool(session)
                partitions = partition_symbols(pool)

            current_cfg    = cycles_cfg[cycle_idx]
            current_batch  = partitions[cycle_idx] if cycle_idx < len(partitions) else []
            cycle_label    = current_cfg["label"]

            log.info(f"\n{'─'*60}")
            log.info(f"  ▶ {cycle_label} ({len(current_batch)} koin)")
            log.info(f"{'─'*60}")

            if not current_batch:
                log.warning(f"  ⚠️ {cycle_label}: batch kosong, skip.")
            else:
                try:
                    await run_scan_batch(session, current_batch, cycle_label)
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    log.error(f"Siklus crash [{cycle_label}]: {exc}", exc_info=True)
                    await send_telegram(
                        session,
                        f"⚠️ <b>Error pada {cycle_label}:</b>\n"
                        f"<code>{str(exc)[:300]}</code>",
                    )

            # Maju ke siklus berikutnya
            cycle_idx = (cycle_idx + 1) % len(cycles_cfg)

            wait_sec = CONFIG["SCAN_INTERVAL_MIN"] * 60
            log.info(f"\n  ⏳ Jeda {CONFIG['SCAN_INTERVAL_MIN']} menit sebelum siklus berikutnya…\n")
            await asyncio.sleep(wait_sec)

def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("\n⛔ Dihentikan oleh user (Ctrl+C).")
    except Exception as exc:
        log.critical(f"Fatal error: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
