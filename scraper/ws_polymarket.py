"""
WebSocket Polymarket CLOB — events prix et trades en temps réel.
Gère le subscribe dynamique quand de nouveaux marchés sont découverts.
"""
import asyncio
import json
import time
from typing import Callable

import websockets

from config.settings import POLYMARKET_WS_URL
from monitoring.logger import log
from storage.writer import push
from scraper.ws_binance import get_btc_spot

# ── État interne ──────────────────────────────────────────────────────────────
_subscribed_tokens: set[str] = set()
_ws_ref = None          # référence à la connexion WS active
_ws_lock = asyncio.Lock()

# token_id → market_id (pour enrichir les events)
_token_to_market: dict[str, str] = {}
# token_id → "YES" ou "NO"
_token_to_outcome: dict[str, str] = {}
# market_id → strike_price (pour moneyness)
_market_to_strike: dict[str, float] = {}
# market_id → expiry_ts_ms
_market_to_expiry: dict[str, int] = {}
# market_id → dernier mid YES connu (pour slippage)
_last_mid: dict[str, float] = {}


def register_market(market) -> None:
    """Enregistre un marché pour enrichissement des events WS."""
    if market.token_id_yes:
        _token_to_market[market.token_id_yes] = market.market_id
        _token_to_outcome[market.token_id_yes] = "YES"
    if market.token_id_no:
        _token_to_market[market.token_id_no] = market.market_id
        _token_to_outcome[market.token_id_no] = "NO"
    _market_to_strike[market.market_id] = market.strike_price
    _market_to_expiry[market.market_id] = market.expiry_ts_ms


async def subscribe_tokens(token_ids: list[str]) -> None:
    """Subscribe aux tokens passés si pas encore abonnés."""
    new_tokens = [t for t in token_ids if t and t not in _subscribed_tokens]
    if not new_tokens:
        return

    async with _ws_lock:
        _subscribed_tokens.update(new_tokens)
        if _ws_ref is not None:
            try:
                msg = json.dumps({
                    "assets_ids": new_tokens,
                    "type": "market"
                })
                await _ws_ref.send(msg)
                log.info(f"WS Polymarket: subscribed {len(new_tokens)} nouveaux tokens")
            except Exception as e:
                log.warning(f"WS subscribe error: {e}")


# ── Handlers d'événements ─────────────────────────────────────────────────────

async def _handle_price_change(event: dict) -> None:
    """
    Event 'price_change' — changement de prix bid/ask.
    Structure Polymarket : asset_id, price, side, size, ...
    """
    token_id  = event.get("asset_id") or event.get("market", "")
    market_id = _token_to_market.get(token_id, "")
    outcome   = _token_to_outcome.get(token_id, "")
    ts_ms     = int(event.get("timestamp", time.time() * 1000))

    if not market_id:
        return

    # On stocke le dernier mid pour calcul slippage ultérieur
    price = float(event.get("price", 0))
    if outcome == "YES" and price > 0:
        _last_mid[market_id] = price

    # Note : les ticks complets (bid/ask/imbalance) sont gérés
    # par book_poller.py qui poll le REST endpoint toutes les 1s.
    # L'event WS sert ici de trigger pour signaler un changement.


async def _handle_last_trade(event: dict) -> None:
    """
    Event 'last_trade_price' — trade exécuté sur le CLOB.
    """
    token_id  = event.get("asset_id") or event.get("market", "")
    market_id = _token_to_market.get(token_id, "")
    outcome   = _token_to_outcome.get(token_id, "")

    if not market_id:
        return

    trade_id  = event.get("id") or event.get("trade_id") or f"{token_id}_{time.time_ns()}"
    price     = float(event.get("price", 0))
    size      = float(event.get("size", 0))
    side      = event.get("side", "").upper()
    ts_ms     = int(event.get("timestamp", time.time() * 1000))
    fee_bps   = float(event.get("fee_rate_bps", 0))

    btc_spot  = get_btc_spot()
    strike    = _market_to_strike.get(market_id, 0.0)
    expiry_ms = _market_to_expiry.get(market_id, 0)
    tte_ms    = max(0, expiry_ms - ts_ms)
    mid       = _last_mid.get(market_id, price)
    slippage  = price - mid if mid else 0.0

    row = {
        "trade_id":                    trade_id,
        "market_id":                   market_id,
        "token_id":                    token_id,
        "outcome":                     outcome,
        "price":                       price,
        "size":                        size,
        "side":                        side,
        "trade_ts_ms":                 ts_ms,
        "fee_rate_bps":                fee_bps,
        "trade_type":                  event.get("type", "TRADE"),
        "time_to_expiry_at_trade_ms":  tte_ms,
        "btc_spot_at_trade":           btc_spot,
        "moneyness_at_trade":          btc_spot - strike if strike else 0.0,
        "slippage_vs_mid":             slippage,
    }
    await push("trades", row)


# ── Boucle WebSocket principale ───────────────────────────────────────────────

async def polymarket_ws_loop() -> None:
    """
    Connexion au WebSocket Polymarket CLOB.
    Reconnexion automatique avec backoff.
    """
    global _ws_ref
    backoff = 1

    log.info("Polymarket WS: connexion...")

    while True:
        try:
            async with websockets.connect(
                POLYMARKET_WS_URL,
                ping_interval=30,
                ping_timeout=15,
            ) as ws:
                _ws_ref = ws
                log.info("Polymarket WS: connecté")
                backoff = 1

                # Re-subscribe à tous les tokens connus
                if _subscribed_tokens:
                    await ws.send(json.dumps({
                        "assets_ids": list(_subscribed_tokens),
                        "type": "market"
                    }))
                    log.info(f"Polymarket WS: re-subscribed {len(_subscribed_tokens)} tokens")

                async for raw in ws:
                    try:
                        events = json.loads(raw)
                        if isinstance(events, dict):
                            events = [events]

                        for event in events:
                            etype = event.get("event_type") or event.get("type", "")

                            if etype in ("price_change", "book"):
                                await _handle_price_change(event)
                            elif etype in ("last_trade_price", "trade"):
                                await _handle_last_trade(event)

                    except Exception as e:
                        log.warning(f"Polymarket WS message error: {e}")

        except Exception as e:
            log.warning(f"Polymarket WS disconnected: {e} — reconnexion dans {backoff}s")
            _ws_ref = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
