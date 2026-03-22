"""
Feed WebSocket Binance — prix BTC spot en temps réel.
Utilise btcusdt@bookTicker (bid/ask) + btcusdt@aggTrade (last trade).
Gratuit, public, aucune auth requise.
"""
import asyncio
import json
import time
from collections import deque

import websockets

from config.settings import BINANCE_WS_URL
from monitoring.logger import log
from storage.writer import push

# ── État partagé — accès thread-safe via get_btc_spot() ──────────────────────
_state = {
    "price":       0.0,
    "bid":         0.0,
    "ask":         0.0,
    "volume_24h":  0.0,
    "trade_id":    0,
    "qty":         0.0,
    "buyer_maker": False,
    "ts_ms":       0,
}

# Historique des 60 derniers prix pour calcul de volatilité
_price_history: deque = deque(maxlen=60)
_last_price_1s: float = 0.0
_last_price_30s_buf: deque = deque(maxlen=30)


def get_btc_spot() -> float:
    """Retourne le dernier prix BTC spot connu. Utilisé par le discoverer."""
    return _state["price"]


def get_btc_state() -> dict:
    return dict(_state)


# ── Calculs dérivés ───────────────────────────────────────────────────────────

def _calc_volatility() -> float:
    """Écart-type des 60 derniers deltas de prix (proxy vol 1 min)."""
    if len(_price_history) < 2:
        return 0.0
    deltas = [_price_history[i] - _price_history[i-1]
              for i in range(1, len(_price_history))]
    if not deltas:
        return 0.0
    mean = sum(deltas) / len(deltas)
    variance = sum((d - mean) ** 2 for d in deltas) / len(deltas)
    return variance ** 0.5


# ── WebSocket handlers ────────────────────────────────────────────────────────

async def _handle_book_ticker(msg: dict) -> None:
    """btcusdt@bookTicker — meilleur bid/ask en temps réel."""
    _state["bid"] = float(msg.get("b", 0))
    _state["ask"] = float(msg.get("a", 0))
    mid = (_state["bid"] + _state["ask"]) / 2
    if mid > 0:
        _state["price"] = mid


async def _handle_agg_trade(msg: dict) -> None:
    """btcusdt@aggTrade — dernier trade agrégé."""
    ts_ms    = int(msg.get("T", time.time() * 1000))
    price    = float(msg.get("p", 0))
    qty      = float(msg.get("q", 0))
    trade_id = int(msg.get("a", 0))
    buyer_maker = bool(msg.get("m", False))

    _state.update({
        "price":       price,
        "qty":         qty,
        "trade_id":    trade_id,
        "buyer_maker": buyer_maker,
        "ts_ms":       ts_ms,
    })

    # Historique pour la vol
    _price_history.append(price)
    _last_price_30s_buf.append(price)

    # Dérivés
    delta_1s  = price - (_price_history[-2] if len(_price_history) >= 2 else price)
    delta_30s = price - (_last_price_30s_buf[0] if len(_last_price_30s_buf) == 30 else price)
    vol_1min  = _calc_volatility()

    row = {
        "ts_ms":            ts_ms,
        "price":            price,
        "bid":              _state["bid"],
        "ask":              _state["ask"],
        "volume_24h":       _state["volume_24h"],
        "binance_trade_id": trade_id,
        "qty":              qty,
        "buyer_maker":      buyer_maker,
        "price_delta_1s":   delta_1s,
        "price_delta_30s":  delta_30s,
        "volatility_1min":  vol_1min,
    }
    await push("btc_spot_ticks", row)


# ── Boucle WebSocket principale ───────────────────────────────────────────────

async def binance_ws_loop() -> None:
    """
    Se connecte au combined stream Binance.
    Reconnexion automatique avec backoff exponentiel.
    """
    url = f"{BINANCE_WS_URL}/btcusdt@bookTicker/btcusdt@aggTrade"
    backoff = 1

    log.info("Binance WS: connexion...")

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                log.info("Binance WS: connecté")
                backoff = 1  # reset backoff après succès

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        stream = data.get("stream", "")
                        msg   = data.get("data", data)

                        if "bookTicker" in stream:
                            await _handle_book_ticker(msg)
                        elif "aggTrade" in stream:
                            await _handle_agg_trade(msg)

                    except Exception as e:
                        log.warning(f"Binance WS message error: {e}")

        except Exception as e:
            log.warning(f"Binance WS disconnected: {e} — reconnexion dans {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
