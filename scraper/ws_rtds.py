"""
Feed WebSocket RTDS Polymarket — prix BTC Binance + Chainlink.
URL : wss://ws-live-data.polymarket.com
PING toutes les 5s obligatoire.
Deux connexions separees (une par topic — limitation Polymarket).

Stockage :
  btc_spot_ticks         = Chainlink uniquement (reference officielle Polymarket)
  btc_spot_ticks_binance = Binance uniquement (source secondaire)
"""
import asyncio
import json
import math
import time
from collections import deque
from typing import Dict, Optional

import websockets

from monitoring.logger import log
from storage.writer import push

RTDS_URL = "wss://ws-live-data.polymarket.com"

_state = {
    "price_binance":   0.0,
    "price_chainlink": 0.0,
    "price":           0.0,
    "ts_ms":           0,
}

# Historiques separes par source pour calculs delta/volatilite
_chainlink_history: deque = deque(maxlen=60)
_chainlink_30s_buf: deque = deque(maxlen=30)
_binance_history:   deque = deque(maxlen=60)
_binance_30s_buf:   deque = deque(maxlen=30)

# (ts_ms, price) — historique long pour deltas T+1/2/3 min vs open (éviction FIFO)
_CHAINLINK_TICKS_MAX = 7200
_BINANCE_TICKS_MAX = 7200
_chainlink_ticks: deque = deque(maxlen=_CHAINLINK_TICKS_MAX)
_binance_ticks: deque = deque(maxlen=_BINANCE_TICKS_MAX)


def chainlink_price_at_or_after(target_ts_ms: int) -> Optional[float]:
    """Premier tick Chainlink avec ts >= target_ts_ms (prix à horizon fixe)."""
    for t, p in _chainlink_ticks:
        if t >= target_ts_ms:
            return p
    return None


def chainlink_price_at_or_before(target_ts_ms: int) -> Optional[float]:
    """Dernier tick Chainlink avec ts <= target_ts_ms (deque ordonnée par temps croissant)."""
    last: Optional[float] = None
    for t, p in _chainlink_ticks:
        if t <= target_ts_ms:
            last = p
        else:
            break
    return last


def _nearest_tick_price(ticks: deque, target_ts_ms: int, window_ms: int) -> Optional[float]:
    """Tick dont le timestamp minimise |t - target| parmi ceux dans [target ± window]."""
    best_p: Optional[float] = None
    best_d: Optional[int] = None
    for t, p in ticks:
        d = abs(t - target_ts_ms)
        if d > window_ms:
            continue
        if best_d is None or d < best_d:
            best_d = d
            best_p = p
    return best_p


def chainlink_price_for_horizon(target_ts_ms: int) -> Optional[float]:
    """
    Prix de référence Chainlink pour un instant cible (ex. open + 60s).

    Ordre : premier tick >= T ; sinon dernier tick <= T (gap / tick rare) ;
    sinon plus proche dans une fenêtre autour de T (RTDS irrégulier).
    """
    pa = chainlink_price_at_or_after(target_ts_ms)
    if pa is not None:
        return pa
    pb = chainlink_price_at_or_before(target_ts_ms)
    if pb is not None:
        return pb
    return _nearest_tick_price(_chainlink_ticks, target_ts_ms, window_ms=180_000)


def _binance_price_for_horizon(target_ts_ms: int) -> Optional[float]:
    """Même logique sur l'historique Binance RTDS (fallback si Chainlink insuffisant)."""
    first_after: Optional[float] = None
    for t, p in _binance_ticks:
        if t >= target_ts_ms:
            first_after = p
            break
    if first_after is not None:
        return first_after
    last_before: Optional[float] = None
    for t, p in _binance_ticks:
        if t <= target_ts_ms:
            last_before = p
        else:
            break
    if last_before is not None:
        return last_before
    return _nearest_tick_price(_binance_ticks, target_ts_ms, window_ms=180_000)


def btc_price_for_horizon(target_ts_ms: int) -> Optional[float]:
    """
    Prix BTC pour un instant absolu : Chainlink prioritaire, Binance en secours.
    Utilisé pour btc_delta_1min/2min/3min vs prix d'ouverture marché.
    """
    p = chainlink_price_for_horizon(target_ts_ms)
    if p is not None and p > 0:
        return p
    p2 = _binance_price_for_horizon(target_ts_ms)
    if p2 is not None and p2 > 0:
        return p2
    return None


def _chainlink_log_return_stdev(t_start_ms: int, t_end_ms: int) -> float:
    """
    Ecart-type des log-rendements entre ticks Chainlink consecutifs dans [t_start_ms, t_end_ms].
    Mesure de volatilite realisee (depend de la frequence des ticks RTDS).
    """
    if t_end_ms <= t_start_ms:
        return 0.0
    ticks = [(t, p) for t, p in _chainlink_ticks if t_start_ms <= t <= t_end_ms]
    if len(ticks) < 2:
        return 0.0
    ticks.sort(key=lambda x: x[0])
    rets: list[float] = []
    for i in range(1, len(ticks)):
        p0, p1 = ticks[i - 1][1], ticks[i][1]
        if p0 > 0 and p1 > 0:
            rets.append(math.log(p1 / p0))
    if len(rets) < 2:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var)


def get_chainlink_realized_vol_fields(now_ms: int, open_ts_ms: Optional[int] = None) -> Dict[str, float]:
    """
    Vol realisee BTC (Chainlink) : fenetres glissantes 1/2/5 min + depuis open du marche.
    """
    out: Dict[str, float] = {
        "btc_vol_1m": _chainlink_log_return_stdev(now_ms - 60_000, now_ms),
        "btc_vol_2m": _chainlink_log_return_stdev(now_ms - 120_000, now_ms),
        "btc_vol_5m": _chainlink_log_return_stdev(now_ms - 300_000, now_ms),
        "btc_vol_since_open": 0.0,
    }
    if open_ts_ms and open_ts_ms > 0 and now_ms > open_ts_ms:
        out["btc_vol_since_open"] = _chainlink_log_return_stdev(open_ts_ms, now_ms)
    return out


def get_btc_spot() -> float:
    """Chainlink en priorite, Binance en fallback."""
    return _state["price_chainlink"] or _state["price_binance"]


def get_btc_state() -> dict:
    return dict(_state)


def _calc_volatility(history: deque) -> float:
    if len(history) < 2:
        return 0.0
    deltas = [history[i] - history[i-1] for i in range(1, len(history))]
    mean   = sum(deltas) / len(deltas)
    return (sum((d - mean)**2 for d in deltas) / len(deltas)) ** 0.5


async def _handle_price(payload: dict, source: str) -> None:
    symbol = str(payload.get("symbol", "")).lower()
    if "btc" not in symbol:
        return

    price = float(payload.get("value", 0))
    ts_ms = int(payload.get("timestamp", time.time() * 1000))

    if source == "chainlink":
        _state["price_chainlink"] = price
        _chainlink_ticks.append((ts_ms, price))
        _chainlink_history.append(price)
        _chainlink_30s_buf.append(price)

        delta_1s  = price - (_chainlink_history[-2] if len(_chainlink_history) >= 2 else price)
        delta_30s = price - (_chainlink_30s_buf[0]  if len(_chainlink_30s_buf) == 30 else price)

        # Table principale — reference officielle Polymarket
        await push("btc_spot_ticks", {
            "ts_ms":            ts_ms,
            "price":            price,
            "price_delta_1s":   delta_1s,
            "price_delta_30s":  delta_30s,
            "volatility_1min":  _calc_volatility(_chainlink_history),
        })

    else:  # binance
        _state["price_binance"] = price
        _binance_ticks.append((ts_ms, price))
        _binance_history.append(price)
        _binance_30s_buf.append(price)

        delta_1s  = price - (_binance_history[-2] if len(_binance_history) >= 2 else price)
        delta_30s = price - (_binance_30s_buf[0]  if len(_binance_30s_buf) == 30 else price)

        # Table secondaire — bonus
        await push("btc_spot_ticks_binance", {
            "ts_ms":            ts_ms,
            "price":            price,
            "bid":              price,
            "ask":              price,
            "volume_24h":       0.0,
            "binance_trade_id": 0,
            "qty":              0.0,
            "buyer_maker":      False,
            "price_delta_1s":   delta_1s,
            "price_delta_30s":  delta_30s,
            "volatility_1min":  _calc_volatility(_binance_history),
        })

    # Mise a jour etat global
    _state["ts_ms"] = ts_ms
    _state["price"] = _state["price_chainlink"] or _state["price_binance"]


async def _rtds_connect(topic: str, sub_msg: dict, source: str) -> None:
    """Connexion a un topic RTDS avec PING toutes les 5s."""
    backoff = 1
    while True:
        try:
            async with websockets.connect(RTDS_URL, ping_interval=None) as ws:
                await ws.send(json.dumps(sub_msg))
                log.info(f"RTDS {source} connecte")
                backoff = 1

                async def ping_loop():
                    while True:
                        await asyncio.sleep(5)
                        try:
                            await ws.send(json.dumps({"action": "ping"}))
                        except Exception:
                            break

                ping_task = asyncio.create_task(ping_loop())

                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            if msg.get("topic") == topic:
                                await _handle_price(msg.get("payload", {}), source)
                        except Exception as e:
                            log.warning(f"RTDS {source} parse error: {e}")
                finally:
                    ping_task.cancel()

        except Exception as e:
            log.warning(f"RTDS {source} disconnected: {e} — reconnexion dans {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def _binance_feed() -> None:
    await _rtds_connect(
        topic="crypto_prices",
        sub_msg={"action": "subscribe", "subscriptions": [
            {"topic": "crypto_prices", "type": "update", "filters": "btcusdt"}
        ]},
        source="binance",
    )


async def _chainlink_feed() -> None:
    await _rtds_connect(
        topic="crypto_prices_chainlink",
        sub_msg={"action": "subscribe", "subscriptions": [
            {"topic": "crypto_prices_chainlink", "type": "*",
             "filters": '{"symbol":"btc/usd"}'}
        ]},
        source="chainlink",
    )


async def rtds_loop() -> None:
    """Deux connexions paralleles — Binance + Chainlink."""
    await asyncio.gather(_binance_feed(), _chainlink_feed())


async def binance_ws_loop() -> None:
    """Alias pour compatibilite main.py."""
    await rtds_loop()