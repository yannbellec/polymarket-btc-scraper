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
import time
from collections import deque
from typing import Optional

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

# (ts_ms, price) Chainlink — pour horizons BTC depuis open_ts marché (~10 min)
_chainlink_ticks: deque = deque(maxlen=600)


def chainlink_price_at_or_after(target_ts_ms: int) -> Optional[float]:
    """Premier tick Chainlink avec ts >= target_ts_ms (prix à horizon fixe)."""
    for t, p in _chainlink_ticks:
        if t >= target_ts_ms:
            return p
    return None


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