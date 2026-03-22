"""
WebSocket CLOB Polymarket — market channel.
Capture TOUS les events en temps réel :
  - price_change → orderbook_ticks (chaque mouvement de book)
  - book         → orderbook_ticks (snapshot complet)
  - last_trade_price → trades
Remplace le book_poller REST — fréquence event-driven vs poll 1s.
"""
import asyncio
import json
import time
from typing import Callable

import websockets

from config.settings import POLYMARKET_WS_URL
from monitoring.logger import log
from storage.writer import push, next_tick_id
from scraper.ws_rtds import get_btc_spot

_subscribed_tokens: set[str] = set()
_ws_ref = None
_ws_lock = asyncio.Lock()

_token_to_market: dict[str, str] = {}
_token_to_outcome: dict[str, str] = {}
_market_to_expiry: dict[str, int] = {}
_market_to_btc_open: dict[str, float] = {}
_last_mid: dict[str, float] = {}
_last_bid: dict[str, float] = {}
_last_ask: dict[str, float] = {}
_trade_count: dict[str, int] = {}
_volume_cumul: dict[str, float] = {}



def register_market(market) -> None:
    if market.token_id_yes:
        _token_to_market[market.token_id_yes] = market.market_id
        _token_to_outcome[market.token_id_yes] = "YES"
    if market.token_id_no:
        _token_to_market[market.token_id_no] = market.market_id
        _token_to_outcome[market.token_id_no] = "NO"
    _market_to_expiry[market.market_id] = market.expiry_ts_ms
    _market_to_btc_open[market.market_id] = market.btc_spot_at_open


async def subscribe_tokens(token_ids: list[str]) -> None:
    new_tokens = [t for t in token_ids if t and t not in _subscribed_tokens]
    if not new_tokens:
        return
    async with _ws_lock:
        _subscribed_tokens.update(new_tokens)
        # Ferme la connexion pour forcer une reconnexion complète
        # Polymarket rejette les subscriptions incrementales sur session existante
        if _ws_ref is not None:
            try:
                await _ws_ref.close()
                log.info(f"WS Polymarket: reconnexion forcée pour {len(new_tokens)} nouveaux tokens")
            except Exception:
                pass


async def _handle_price_change(event: dict) -> None:
    """
    price_change — chaque changement de bid/ask.
    C'est l'event le plus fréquent — sub-seconde.
    On le stocke comme un tick orderbook.
    """
    token_id  = event.get("asset_id", "")
    market_id = _token_to_market.get(token_id, "")
    outcome   = _token_to_outcome.get(token_id, "")
    if not market_id or outcome != "YES":
        return

    now_ms    = int(time.time() * 1000)
    btc_spot  = get_btc_spot()
    btc_open  = _market_to_btc_open.get(market_id, 0.0)
    expiry_ms = _market_to_expiry.get(market_id, 0)
    tte_ms    = max(0, expiry_ms - now_ms)

    # Extrait bid/ask depuis price_changes[]
    changes = event.get("price_changes") or [event]
    for change in changes:
        bid = float(change.get("best_bid") or change.get("price") or 0)
        ask = float(change.get("best_ask") or change.get("price") or 0)

        if bid <= 0 and ask <= 0:
            continue

        # Si on a seulement l'un des deux, utilise le dernier connu
        if bid <= 0:
            bid = _last_bid.get(market_id, 0)
        if ask <= 0:
            ask = _last_ask.get(market_id, 0)

        mid    = (bid + ask) / 2 if (bid + ask) > 0 else 0
        spread = ask - bid if ask > bid else 0

        # Delta depuis dernier mid
        prev_mid  = _last_mid.get(market_id, mid)
        delta_1s  = mid - prev_mid

        _last_bid[market_id] = bid
        _last_ask[market_id] = ask
        _last_mid[market_id] = mid

        row = {
            "tick_id":               next_tick_id(),
            "market_id":             market_id,
            "captured_ts_ms":        now_ms,
            "time_to_expiry_ms":     tte_ms,
            # YES
            "yes_best_bid":          bid,
            "yes_best_ask":          ask,
            "yes_bid_size":          float(change.get("bid_size", 0)),
            "yes_ask_size":          float(change.get("ask_size", 0)),
            "yes_total_bid_liq":     0.0,
            "yes_total_ask_liq":     0.0,
            "yes_book_depth":        0,
            # NO (complémentaire)
            "no_best_bid":           round(1 - ask, 4) if ask else 0,
            "no_best_ask":           round(1 - bid, 4) if bid else 0,
            "no_bid_size":           float(change.get("ask_size", 0)),
            "no_ask_size":           float(change.get("bid_size", 0)),
            "no_total_bid_liq":      0.0,
            "no_total_ask_liq":      0.0,
            # Dérivés
            "yes_mid":               mid,
            "yes_spread":            spread,
            "yes_spread_pct":        (spread / mid * 100) if mid > 0 else 0,
            "book_imbalance":        0.0,
            "yes_price_delta_1s":    delta_1s,
            "yes_price_delta_10s":   0.0,
            "volume_since_open":     0.0,
            "trade_count_since_open": 0,
            "btc_spot":              btc_spot,
            "moneyness": btc_spot - btc_open if btc_open else 0.0,
        }
        await push("orderbook_ticks", row)


async def _handle_book(event: dict) -> None:
    """
    book — snapshot complet du carnet après chaque modification.
    Contient bids[] et asks[] complets avec tous les niveaux.
    """
    token_id  = event.get("asset_id", "")
    market_id = _token_to_market.get(token_id, "")
    outcome   = _token_to_outcome.get(token_id, "")
    if not market_id or outcome != "YES":
        return

    now_ms   = int(time.time() * 1000)
    btc_spot  = get_btc_spot()
    btc_open  = _market_to_btc_open.get(market_id, 0.0)
    expiry_ms = _market_to_expiry.get(market_id, 0)
    tte_ms   = max(0, expiry_ms - now_ms)

    bids = sorted(event.get("bids", []),
                  key=lambda x: float(x[0] if isinstance(x, list) else x.get("price", 0)),
                  reverse=True)
    asks = sorted(event.get("asks", []),
                  key=lambda x: float(x[0] if isinstance(x, list) else x.get("price", 0)))

    def price(item): return float(item[0] if isinstance(item, list) else item.get("price", 0))
    def size(item):  return float(item[1] if isinstance(item, list) else item.get("size", 0))

    best_bid      = price(bids[0]) if bids else 0
    best_ask      = price(asks[0]) if asks else 0
    bid_size      = size(bids[0])  if bids else 0
    ask_size      = size(asks[0])  if asks else 0
    total_bid_liq = sum(size(b) for b in bids)
    total_ask_liq = sum(size(a) for a in asks)
    mid           = (best_bid + best_ask) / 2 if (best_bid + best_ask) > 0 else 0
    spread        = best_ask - best_bid if best_ask > best_bid else 0
    imbalance     = ((total_bid_liq - total_ask_liq) / (total_bid_liq + total_ask_liq)
                     if (total_bid_liq + total_ask_liq) > 0 else 0)

    prev_mid  = _last_mid.get(market_id, mid)
    delta_1s  = mid - prev_mid

    _last_bid[market_id] = best_bid
    _last_ask[market_id] = best_ask
    _last_mid[market_id] = mid

    row = {
        "tick_id":               next_tick_id(),
        "market_id":             market_id,
        "captured_ts_ms":        now_ms,
        "time_to_expiry_ms":     tte_ms,
        "yes_best_bid":          best_bid,
        "yes_best_ask":          best_ask,
        "yes_bid_size":          bid_size,
        "yes_ask_size":          ask_size,
        "yes_total_bid_liq":     total_bid_liq,
        "yes_total_ask_liq":     total_ask_liq,
        "yes_book_depth":        len(bids) + len(asks),
        "no_best_bid":           round(1 - best_ask, 4) if best_ask else 0,
        "no_best_ask":           round(1 - best_bid, 4) if best_bid else 0,
        "no_bid_size":           ask_size,
        "no_ask_size":           bid_size,
        "no_total_bid_liq":      total_ask_liq,
        "no_total_ask_liq":      total_bid_liq,
        "yes_mid":               mid,
        "yes_spread":            spread,
        "yes_spread_pct":        (spread / mid * 100) if mid > 0 else 0,
        "book_imbalance":        imbalance,
        "yes_price_delta_1s":    delta_1s,
        "yes_price_delta_10s":   0.0,
        "volume_since_open":     0.0,
        "trade_count_since_open": 0,
        "btc_spot":              btc_spot,
        "moneyness":             btc_spot - btc_open if btc_open else 0.0,
    }
    await push("orderbook_ticks", row)


async def _handle_last_trade(event: dict) -> None:
    token_id  = event.get("asset_id", "")
    market_id = _token_to_market.get(token_id, "")
    outcome   = _token_to_outcome.get(token_id, "")
    if not market_id:
        return

    trade_id  = event.get("id") or f"{token_id}_{time.time_ns()}"
    price     = float(event.get("price", 0))
    size      = float(event.get("size", 0))
    side      = event.get("side", "").upper()
    ts_ms     = int(event.get("timestamp", time.time() * 1000))
    fee_bps   = float(event.get("fee_rate_bps", 0))
    btc_spot  = get_btc_spot()
    expiry_ms = _market_to_expiry.get(market_id, 0)
    tte_ms    = max(0, expiry_ms - ts_ms)
    mid       = _last_mid.get(market_id, price)
    slippage  = price - mid if mid else 0.0

    await push("trades", {
        "trade_id":                   trade_id,
        "market_id":                  market_id,
        "token_id":                   token_id,
        "outcome":                    outcome,
        "price":                      price,
        "size":                       size,
        "side":                       side,
        "trade_ts_ms":                ts_ms,
        "fee_rate_bps":               fee_bps,
        "trade_type":                 event.get("type", "TRADE"),
        "time_to_expiry_at_trade_ms": tte_ms,
        "btc_spot_at_trade":          btc_spot,
        "moneyness_at_trade":         0.0,
        "slippage_vs_mid":            slippage,
    })

    # Met à jour les compteurs dans le state local
    _trade_count[market_id] = _trade_count.get(market_id, 0) + 1
    _volume_cumul[market_id] = _volume_cumul.get(market_id, 0.0) + size


async def polymarket_ws_loop() -> None:
    global _ws_ref
    backoff = 1
    log.info("Polymarket WS: connexion...")

    while True:
        try:
            async with websockets.connect(
                POLYMARKET_WS_URL, ping_interval=30, ping_timeout=15,
            ) as ws:
                _ws_ref = ws
                log.info("Polymarket WS: connecté")
                backoff = 1

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
                            etype = (event.get("event_type") or
                                     event.get("type", "")).lower()
                            if etype == "price_change":
                                await _handle_price_change(event)
                            elif etype == "book":
                                await _handle_book(event)
                            elif etype in ("last_trade_price", "trade"):
                                await _handle_last_trade(event)
                    except Exception as e:
                        log.warning(f"Polymarket WS parse error: {e}")

        except Exception as e:
            log.warning(f"Polymarket WS disconnected: {e} — reconnexion dans {backoff}s")
            _ws_ref = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)