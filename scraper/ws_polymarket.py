"""
WebSocket CLOB Polymarket — market channel.
Capture TOUS les events en temps réel :
  - price_change → orderbook_ticks (chaque mouvement de book)
  - book         → orderbook_ticks (snapshot complet)
  - last_trade_price → trades
"""
import asyncio
import json
import time
from collections import deque
from typing import Any, Callable, Dict, Optional

import websockets

from config.settings import POLYMARKET_WS_URL
from monitoring.logger import log
from storage.writer import push, next_tick_id
from scraper.ws_rtds import (
    get_btc_spot,
    chainlink_price_at_or_after,
    get_chainlink_realized_vol_fields,
)
from scraper.inter_market_context import empty_inter_market_fields

_subscribed_tokens: set[str] = set()
_ws_ref = None
_ws_lock = asyncio.Lock()

# Au-delà de ce seuil, Polymarket cesse d'émettre sans fermer le WS — reconnexion forcée.
# ~2 tokens × 10 marchés actifs max en même temps.
MAX_SUBSCRIBED_TOKENS_BEFORE_RECONNECT = 20

_token_to_market: dict[str, str] = {}
_token_to_outcome: dict[str, str] = {}
_market_to_expiry: dict[str, int] = {}
_market_to_btc_open: dict[str, float] = {}
_market_open_ts_ms: dict[str, int] = {}
_btc_horizon_delta: Dict[str, Dict[str, Optional[float]]] = {}
_last_mid: dict[str, float] = {}
_last_bid: dict[str, float] = {}
_last_ask: dict[str, float] = {}
_trade_count: dict[str, int] = {}
_volume_cumul: dict[str, float] = {}
_inter_market_ctx: Dict[str, Dict[str, Any]] = {}

# OFI : cumul signe (BUY +size, SELL -size) ; deque (ts_ms, signed_size) pour fenêtre 60s
_ofi_cumul: Dict[str, float] = {}
_ofi_ring: Dict[str, deque] = {}
_OFI_WINDOW_MS = 60_000

_YES_MID_RING_LEN = 10
_yes_mid_ring: dict[str, deque] = {}

# Moyennes mobiles spread et liquidite — fenetres glissantes de 60 ticks
_MA_LEN = 60
_spread_ring: dict[str, deque] = {}
_liq_ring: dict[str, deque] = {}


def _horizon_state(market_id: str) -> Dict[str, Optional[float]]:
    return _btc_horizon_delta.setdefault(market_id, {"1": None, "2": None, "3": None})


def get_btc_horizon_fields(market_id: str, now_ms: int) -> Dict[str, Any]:
    p0 = float(_market_to_btc_open.get(market_id, 0.0))
    open_ts = int(_market_open_ts_ms.get(market_id, 0))
    out: Dict[str, Any] = {
        "btc_price_at_open": p0,
        "btc_delta_1min": None,
        "btc_delta_2min": None,
        "btc_delta_3min": None,
    }
    out.update(get_chainlink_realized_vol_fields(now_ms, open_ts if open_ts else None))
    if not open_ts or p0 <= 0:
        return out
    st = _horizon_state(market_id)
    for m, sk in ((1, "1"), (2, "2"), (3, "3")):
        col = f"btc_delta_{m}min" if m > 1 else "btc_delta_1min"
        if st[sk] is not None:
            out[col] = st[sk]
        elif now_ms >= open_ts + m * 60_000:
            px = chainlink_price_at_or_after(open_ts + m * 60_000)
            if px is not None:
                d = px - p0
                st[sk] = d
                out[col] = d
    return out


def _mid_delta_10_ticks(market_id: str, mid: float) -> float:
    d = _yes_mid_ring.setdefault(market_id, deque(maxlen=_YES_MID_RING_LEN))
    if len(d) >= _YES_MID_RING_LEN:
        delta = mid - d[0]
    else:
        delta = 0.0
    d.append(mid)
    return delta


def _update_spread_ma(market_id: str, spread: float) -> float:
    """Moyenne mobile du spread sur les 60 derniers ticks."""
    ring = _spread_ring.setdefault(market_id, deque(maxlen=_MA_LEN))
    ring.append(spread)
    return sum(ring) / len(ring)


def _update_liq_ma(market_id: str, total_bid_liq: float, total_ask_liq: float) -> float:
    """Moyenne mobile de la liquidite totale (bid+ask) sur les 60 derniers ticks."""
    ring = _liq_ring.setdefault(market_id, deque(maxlen=_MA_LEN))
    ring.append(total_bid_liq + total_ask_liq)
    return sum(ring) / len(ring)


def get_inter_market_fields(market_id: str) -> Dict[str, Any]:
    out = empty_inter_market_fields()
    out.update(_inter_market_ctx.get(market_id, {}))
    return out


def _signed_trade_size(side: str, size: float) -> float:
    s = (side or "").upper()
    if s in ("BUY", "B"):
        return size
    if s in ("SELL", "S"):
        return -size
    return 0.0


def _apply_trade_ofi(market_id: str, ts_ms: int, signed: float) -> None:
    if signed == 0.0:
        return
    _ofi_cumul[market_id] = _ofi_cumul.get(market_id, 0.0) + signed
    ring = _ofi_ring.setdefault(market_id, deque())
    ring.append((ts_ms, signed))


def get_ofi_fields(market_id: str, now_ms: int) -> Dict[str, float]:
    """ofi_since_open = cumul depuis le début du suivi ; ofi_last_60s = somme signée sur 60s glissantes."""
    since = float(_ofi_cumul.get(market_id, 0.0))
    ring = _ofi_ring.get(market_id)
    cutoff = now_ms - _OFI_WINDOW_MS
    last_60 = 0.0
    if ring:
        while ring and ring[0][0] < cutoff:
            ring.popleft()
        last_60 = sum(x[1] for x in ring)
    return {"ofi_since_open": since, "ofi_last_60s": last_60}


def purge_market_ws_state(market_id: str) -> None:
    """Retire un marché expiré des mappings WS et des tokens souscrits (évite accumulation)."""
    tokens = [tid for tid, mid in _token_to_market.items() if mid == market_id]
    for tid in tokens:
        _token_to_market.pop(tid, None)
        _token_to_outcome.pop(tid, None)
        _subscribed_tokens.discard(tid)
    _market_to_expiry.pop(market_id, None)
    _market_to_btc_open.pop(market_id, None)
    _market_open_ts_ms.pop(market_id, None)
    _btc_horizon_delta.pop(market_id, None)
    _last_mid.pop(market_id, None)
    _last_bid.pop(market_id, None)
    _last_ask.pop(market_id, None)
    _trade_count.pop(market_id, None)
    _volume_cumul.pop(market_id, None)
    _inter_market_ctx.pop(market_id, None)
    _ofi_cumul.pop(market_id, None)
    _ofi_ring.pop(market_id, None)
    _yes_mid_ring.pop(market_id, None)
    _spread_ring.pop(market_id, None)
    _liq_ring.pop(market_id, None)


def register_market(market) -> None:
    if market.token_id_yes:
        _token_to_market[market.token_id_yes] = market.market_id
        _token_to_outcome[market.token_id_yes] = "YES"
    if market.token_id_no:
        _token_to_market[market.token_id_no] = market.market_id
        _token_to_outcome[market.token_id_no] = "NO"
    _market_to_expiry[market.market_id] = market.expiry_ts_ms
    _market_to_btc_open[market.market_id] = market.btc_spot_at_open
    _market_open_ts_ms[market.market_id] = int(market.open_ts_ms)
    ctx = getattr(market, "inter_context", None)
    _inter_market_ctx[market.market_id] = ctx if isinstance(ctx, dict) else empty_inter_market_fields()


async def subscribe_tokens(token_ids: list[str]) -> None:
    global _ws_ref
    new_tokens = [t for t in token_ids if t and t not in _subscribed_tokens]
    if not new_tokens:
        return
    async with _ws_lock:
        _subscribed_tokens.update(new_tokens)
        ws = _ws_ref
        if ws is None:
            return
        try:
            if len(_subscribed_tokens) > MAX_SUBSCRIBED_TOKENS_BEFORE_RECONNECT:
                await ws.close()
                _ws_ref = None
                log.info(
                    f"Polymarket WS: reconnexion forcee ({len(_subscribed_tokens)} tokens > "
                    f"{MAX_SUBSCRIBED_TOKENS_BEFORE_RECONNECT})"
                )
            else:
                await ws.send(json.dumps({
                    "assets_ids": list(_subscribed_tokens),
                    "type": "market",
                }))
                log.info(
                    f"Polymarket WS: souscription etendue (+{len(new_tokens)} tokens, "
                    f"{len(_subscribed_tokens)} au total)"
                )
        except Exception as e:
            log.warning(
                f"Polymarket WS: envoi souscription sur connexion existante echoue ({e}) — "
                "reconnexion au prochain cycle reprendra les tokens"
            )


async def _handle_price_change(event: dict) -> None:
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

    changes = event.get("price_changes") or [event]
    for change in changes:
        bid = float(change.get("best_bid") or change.get("price") or 0)
        ask = float(change.get("best_ask") or change.get("price") or 0)

        if bid <= 0 and ask <= 0:
            continue
        if bid <= 0:
            bid = _last_bid.get(market_id, 0)
        if ask <= 0:
            ask = _last_ask.get(market_id, 0)

        mid    = (bid + ask) / 2 if (bid + ask) > 0 else 0
        spread = ask - bid if ask > bid else 0

        prev_mid   = _last_mid.get(market_id, mid)
        delta_tick = mid - prev_mid

        _last_bid[market_id] = bid
        _last_ask[market_id] = ask
        _last_mid[market_id] = mid

        vol_cum      = _volume_cumul.get(market_id, 0.0)
        n_trades     = _trade_count.get(market_id, 0)
        delta_10     = _mid_delta_10_ticks(market_id, mid)
        btc_hz       = get_btc_horizon_fields(market_id, now_ms)
        im           = get_inter_market_fields(market_id)
        ofi          = get_ofi_fields(market_id, now_ms)
        spread_ma    = _update_spread_ma(market_id, spread)
        # Sur price_change, liquidite totale = 0 (non fournie) — MA calculee sur les book events
        liq_ma       = _update_liq_ma(market_id, 0.0, 0.0)

        row = {
            "tick_id":               next_tick_id(),
            "market_id":             market_id,
            "captured_ts_ms":        now_ms,
            "time_to_expiry_ms":     tte_ms,
            "yes_best_bid":          bid,
            "yes_best_ask":          ask,
            "yes_bid_size":          float(change.get("bid_size", 0)),
            "yes_ask_size":          float(change.get("ask_size", 0)),
            "yes_total_bid_liq":     0.0,
            "yes_total_ask_liq":     0.0,
            "yes_book_depth":        0,
            "no_best_bid":           round(1 - ask, 4) if ask else 0,
            "no_best_ask":           round(1 - bid, 4) if bid else 0,
            "no_bid_size":           float(change.get("ask_size", 0)),
            "no_ask_size":           float(change.get("bid_size", 0)),
            "no_total_bid_liq":      0.0,
            "no_total_ask_liq":      0.0,
            "yes_mid":               mid,
            "yes_spread":            spread,
            "yes_spread_pct":        (spread / mid * 100) if mid > 0 else 0,
            "book_imbalance":        0.0,
            "yes_price_delta_tick":  delta_tick,
            "yes_price_delta_10s":   delta_10,
            "volume_since_open":     vol_cum,
            "trade_count_since_open": n_trades,
            "btc_spot":              btc_spot,
            "moneyness":             btc_spot - btc_open if btc_open else 0.0,
            "yes_spread_ma_60":      spread_ma,
            "yes_liq_ma_60":         liq_ma,
            **btc_hz,
            **im,
            **ofi,
        }
        await push("orderbook_ticks", row)


async def _handle_book(event: dict) -> None:
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

    prev_mid   = _last_mid.get(market_id, mid)
    delta_tick = mid - prev_mid

    _last_bid[market_id] = best_bid
    _last_ask[market_id] = best_ask
    _last_mid[market_id] = mid

    vol_cum   = _volume_cumul.get(market_id, 0.0)
    n_trades  = _trade_count.get(market_id, 0)
    delta_10  = _mid_delta_10_ticks(market_id, mid)
    btc_hz    = get_btc_horizon_fields(market_id, now_ms)
    im        = get_inter_market_fields(market_id)
    ofi       = get_ofi_fields(market_id, now_ms)
    spread_ma = _update_spread_ma(market_id, spread)
    liq_ma    = _update_liq_ma(market_id, total_bid_liq, total_ask_liq)

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
        "yes_price_delta_tick":  delta_tick,
        "yes_price_delta_10s":   delta_10,
        "volume_since_open":     vol_cum,
        "trade_count_since_open": n_trades,
        "btc_spot":              btc_spot,
        "moneyness":             btc_spot - btc_open if btc_open else 0.0,
        "yes_spread_ma_60":      spread_ma,
        "yes_liq_ma_60":         liq_ma,
        **btc_hz,
        **im,
        **ofi,
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
    _apply_trade_ofi(market_id, ts_ms, _signed_trade_size(side, size))
    btc_hz    = get_btc_horizon_fields(market_id, ts_ms)
    im        = get_inter_market_fields(market_id)

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
        "moneyness_at_trade":         btc_spot - _market_to_btc_open.get(market_id, 0.0),
        "slippage_vs_mid":            slippage,
        "yes_best_bid_at_trade":      _last_bid.get(market_id, 0.0),
        "yes_best_ask_at_trade":      _last_ask.get(market_id, 0.0),
        **btc_hz,
        **im,
    })

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
                log.info("Polymarket WS: connecte")
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
                        log.debug(f"Polymarket WS: message ignore ({e})")

        except Exception as e:
            log.warning(f"Polymarket WS disconnected: {e} — reconnexion dans {backoff}s")
            _ws_ref = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)