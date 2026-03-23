"""
Discoverer BTC 5-min — slug déterministe depuis l'horloge.
slug = btc-updown-5m-{window_ts} où window_ts = now - (now % 300)
"""
import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from config.settings import GAMMA_API_URL, DISCOVER_INTERVAL_SEC
from monitoring.logger import log
from storage.writer import push
from scraper.inter_market_context import get_context_for_window


@dataclass
class BtcMarket:
    market_id:         str
    condition_id:      str
    token_id_yes:      str
    token_id_no:       str
    question:          str
    window_ts:         int
    expiry_ts_ms:      int
    open_ts_ms:        int
    initial_volume:    float
    initial_liquidity: float
    initial_price_yes: float
    initial_price_no:  float
    btc_spot_at_open:  float = 0.0
    inter_context:     Optional[Dict[str, Any]] = None


_active_markets:       dict[str, BtcMarket] = {}
_window_to_market:     dict[int, str]        = {}
_on_new_market_callbacks: list               = []

# Marchés déjà passés dans expiry_watcher (snapshot) — vidé à la purge pour éviter croissance infinie
expiry_snapshot_triggered: set[str] = set()

MARKET_PATTERNS = [("btc-updown-5m", 300)]


def register_on_new_market(cb) -> None:
    _on_new_market_callbacks.append(cb)


def get_active_markets() -> dict[str, BtcMarket]:
    return dict(_active_markets)


def get_token_ids() -> list[str]:
    ids = []
    for m in _active_markets.values():
        if m.token_id_yes: ids.append(m.token_id_yes)
        if m.token_id_no:  ids.append(m.token_id_no)
    return ids


def current_window_ts() -> int:
    now = int(time.time())
    return now - (now % 300)


def next_window_ts() -> int:
    return current_window_ts() + 300


def make_slug(window_ts: int) -> str:
    return f"btc-updown-5m-{window_ts}"


def market_id_for_window(window_ts: int) -> str | None:
    return _window_to_market.get(window_ts)


def _parse_event_metadata(raw: dict) -> dict:
    """eventMetadata peut être un dict ou une chaîne JSON."""
    import json as _json

    em = raw.get("eventMetadata") or {}
    if isinstance(em, str):
        try:
            em = _json.loads(em)
        except Exception:
            em = {}
    return em if isinstance(em, dict) else {}


def _to_positive_float(v) -> float:
    try:
        p = float(v)
        return p if p > 0 else 0.0
    except (TypeError, ValueError):
        return 0.0


def _extract_gamma_price_to_beat(raw: dict, market_data: dict) -> float:
    """
    Prix de référence (strike) pour BTC Up/Down — champs variables selon la réponse Gamma.
    """
    em = _parse_event_metadata(raw)
    for src in (em, market_data, raw):
        if not isinstance(src, dict):
            continue
        for key in ("priceToBeat", "price_to_beat", "line", "strike", "strikePrice"):
            p = _to_positive_float(src.get(key))
            if p:
                return p
    # Parfois imbriqué sous "custom" / "metadata"
    for parent_key in ("metadata", "custom", "settings"):
        sub = raw.get(parent_key) or market_data.get(parent_key)
        if isinstance(sub, dict):
            for key in ("priceToBeat", "line", "strikePrice"):
                p = _to_positive_float(sub.get(key))
                if p:
                    return p
    return 0.0


async def _resolve_btc_spot_with_fallback(
    client: httpx.AsyncClient, btc_spot_fn, max_wait_rtds_sec: float = 12.0
) -> float:
    """
    1) RTDS (Chainlink/Binance) — boucle courte si le scraper vient de démarrer.
    2) HTTP Binance public (dernier recours, pas de clé API).
    """
    deadline = time.perf_counter() + max_wait_rtds_sec
    while time.perf_counter() < deadline:
        p = _to_positive_float(btc_spot_fn())
        if p:
            return p
        await asyncio.sleep(0.5)

    try:
        resp = await client.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"},
            timeout=8,
        )
        resp.raise_for_status()
        p = _to_positive_float(resp.json().get("price"))
        if p:
            log.info(
                f"BTC spot: fallback HTTP Binance {p:.2f} USDT "
                f"(RTDS pas encore prêt après {max_wait_rtds_sec:.0f}s)"
            )
            return p
    except Exception as e:
        log.warning(f"BTC spot fallback HTTP Binance échoué: {e}")

    return 0.0


async def _fetch_market_by_slug(client: httpx.AsyncClient, slug: str) -> dict | None:
    for endpoint, param in [
        (f"{GAMMA_API_URL}/events", "slug"),
        (f"{GAMMA_API_URL}/markets", "slug"),
    ]:
        try:
            resp = await client.get(endpoint, params={param: slug}, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict) and data:
                events = data.get("events") or data.get("markets", [])
                if events:
                    return events[0]
        except Exception as e:
            log.debug(f"Fetch {endpoint} slug={slug}: {e}")
    return None


async def _try_discover_slug(client, slug: str, window_ts: int,
                              window_sec: int, btc_spot_fn) -> bool:
    import json as _json

    if any(m.window_ts == window_ts for m in _active_markets.values()):
        return True

    raw = await _fetch_market_by_slug(client, slug)
    if not raw:
        return False

    markets_list = raw.get("markets", [])
    if not markets_list:
        return False
    market_data = markets_list[0]

    market_id    = str(market_data.get("id", slug))
    condition_id = market_data.get("conditionId", "")

    clob_ids_raw = market_data.get("clobTokenIds", "[]")
    clob_ids     = _json.loads(clob_ids_raw) if isinstance(clob_ids_raw, str) else clob_ids_raw
    yes_id       = clob_ids[0] if len(clob_ids) > 0 else ""
    no_id        = clob_ids[1] if len(clob_ids) > 1 else ""

    prices_raw = market_data.get("outcomePrices", "[0.5,0.5]")
    prices     = _json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    price_yes  = float(prices[0]) if prices else 0.5
    price_no   = float(prices[1]) if len(prices) > 1 else 0.5

    event_metadata = _parse_event_metadata(raw)

    gamma_line = _extract_gamma_price_to_beat(raw, market_data)
    spot_ref = await _resolve_btc_spot_with_fallback(client, btc_spot_fn)

    if gamma_line > 0:
        price_to_beat = gamma_line
        btc_open = gamma_line
        log.debug(f"priceToBeat Gamma: {price_to_beat:.2f}")
    else:
        price_to_beat = spot_ref
        btc_open = spot_ref
        if spot_ref > 0:
            log.debug(
                f"priceToBeat absent dans Gamma — strike = spot RTDS/HTTP: {spot_ref:.2f}"
            )

    if btc_open <= 0:
        log.warning(
            f"btc_spot_at_open / price_to_beat toujours à 0 pour slug={slug} — "
            "RTDS et Binance HTTP indisponibles ; enrichissement impossible pour ce marché"
        )
    expiry_ts_ms = (window_ts + window_sec) * 1000
    open_ts_ms   = window_ts * 1000

    inter_ctx = get_context_for_window(window_ts)

    market = BtcMarket(
        market_id=market_id,
        condition_id=condition_id,
        token_id_yes=yes_id,
        token_id_no=no_id,
        question=raw.get("title") or market_data.get("question") or slug,
        window_ts=window_ts,
        expiry_ts_ms=expiry_ts_ms,
        open_ts_ms=open_ts_ms,
        initial_volume=float(market_data.get("volume", 0) or 0),
        initial_liquidity=float(market_data.get("liquidity", 0) or 0),
        initial_price_yes=price_yes,
        initial_price_no=price_no,
        btc_spot_at_open=btc_open,
        inter_context=inter_ctx,
    )

    _active_markets[market_id]    = market
    _window_to_market[window_ts]  = market_id

    await push("btc_markets", {
        "market_id":                  market_id,
        "condition_id":               condition_id,
        "token_id_yes":               yes_id,
        "token_id_no":                no_id,
        "question":                   market.question,
        "window_ts":                  window_ts,
        "expiry_ts_ms":               expiry_ts_ms,
        "expiry_iso":                 market_data.get("endDate", ""),
        "window_minutes":             window_sec // 60,
        "open_ts_ms":                 open_ts_ms,
        "initial_volume":             market.initial_volume,
        "initial_liquidity":          market.initial_liquidity,
        "initial_price_yes":          price_yes,
        "initial_price_no":           price_no,
        "maker_fee":                  float(market_data.get("makerBaseFee", 0) or 0),
        "taker_fee":                  float(market_data.get("takerBaseFee", 0) or 0),
        "min_order_size":             float(market_data.get("orderMinSize", 5) or 5),
        "min_tick_size":              float(market_data.get("orderPriceMinTickSize", 0.01) or 0.01),
        "btc_spot_at_open":           btc_open,
        "price_to_beat":              price_to_beat,
        "moneyness_at_open":          0.0,
        "moneyness_pct_at_open":      0.0,
        "seconds_to_expiry_at_open":  max(0, (expiry_ts_ms // 1000) - int(time.time())),
        "resolved":                   False,
        "winning_outcome":            None,
        "final_btc_price":            None,
        "closed_ts_ms":               None,
        "resolution_ts_ms":           None,
        **inter_ctx,
    })

    log.info(
        f"Nouveau marché [{window_sec//60}min]: {slug} | "
        f"prix={price_yes:.3f}/{price_no:.3f} | priceToBeat={price_to_beat:.0f}"
    )

    for cb in _on_new_market_callbacks:
        try:
            await cb(market)
        except Exception as e:
            log.warning(f"on_new_market callback error: {e}")

    return True


async def discovery_loop(btc_spot_fn) -> None:
    log.info(f"Discoverer démarré (multi-pattern, poll toutes les {DISCOVER_INTERVAL_SEC}s)")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                now    = int(time.time())
                now_ms = now * 1000

                for prefix, window_sec in MARKET_PATTERNS:
                    wts  = now - (now % window_sec)
                    nwts = wts + window_sec

                    await _try_discover_slug(client, f"{prefix}-{wts}",  wts,  window_sec, btc_spot_fn)

                    if (nwts - now) <= 30:
                        await _try_discover_slug(client, f"{prefix}-{nwts}", nwts, window_sec, btc_spot_fn)

                # Après expiry_watcher (snapshot) : 60s, sinon repli 15 min
                expired = [
                    mid for mid, m in _active_markets.items()
                    if (m.expiry_ts_ms < now_ms - 60_000 and mid in expiry_snapshot_triggered)
                    or m.expiry_ts_ms < now_ms - 900_000
                ]
                for mid in expired:
                    log.info(f"Expiré purgé: {_active_markets[mid].question[:50]}")
                    wts = _active_markets[mid].window_ts
                    del _active_markets[mid]
                    _window_to_market.pop(wts, None)
                expiry_snapshot_triggered.difference_update(expired)

            except Exception as e:
                log.error(f"Discovery error: {e}")

            await asyncio.sleep(DISCOVER_INTERVAL_SEC)