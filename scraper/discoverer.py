"""
Découverte des marchés BTC 5-min Polymarket.
Poll la Gamma API toutes les DISCOVER_INTERVAL_SEC secondes.
Détecte les nouveaux marchés et archive les marchés expirés.
"""
import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx

from config.settings import GAMMA_API_URL, DISCOVER_INTERVAL_SEC
from monitoring.logger import log
from storage.writer import push

# ── Dataclass Market ──────────────────────────────────────────────────────────

@dataclass
class BtcMarket:
    market_id:    str
    condition_id: str
    token_id_yes: str
    token_id_no:  str
    question:     str
    strike_price: float
    outcome_above: bool
    expiry_ts_ms: int
    expiry_iso:   str
    window_minutes: int
    open_ts_ms:   int
    initial_volume: float
    initial_liquidity: float
    initial_price_yes: float
    initial_price_no: float
    maker_fee: float
    taker_fee: float
    min_order_size: float
    min_tick_size: float
    # Dérivés — remplis à la découverte
    btc_spot_at_open: float = 0.0
    moneyness_at_open: float = 0.0
    moneyness_pct_at_open: float = 0.0
    seconds_to_expiry_at_open: int = 0

# ── État global des marchés actifs ────────────────────────────────────────────
# market_id → BtcMarket
_active_markets: dict[str, BtcMarket] = {}

# Callback appelé quand un nouveau marché est découvert
_on_new_market_callbacks: list = []


def register_on_new_market(cb) -> None:
    _on_new_market_callbacks.append(cb)


def get_active_markets() -> dict[str, BtcMarket]:
    return dict(_active_markets)


def get_token_ids() -> list[str]:
    ids = []
    for m in _active_markets.values():
        if m.token_id_yes:
            ids.append(m.token_id_yes)
        if m.token_id_no:
            ids.append(m.token_id_no)
    return ids


# ── Parsing du strike depuis le titre ─────────────────────────────────────────

def _parse_strike(question: str) -> tuple[float, bool]:
    """
    Extrait le strike et le sens (above/below) depuis le titre du marché.
    Exemple : "Will BTC be above $95,000 at 14:05 UTC?" → (95000.0, True)
    """
    above = "above" in question.lower()
    match = re.search(r"\$([0-9,]+(?:\.[0-9]+)?)", question)
    if match:
        strike = float(match.group(1).replace(",", ""))
    else:
        strike = 0.0
    return strike, above


def _parse_token_ids(market: dict) -> tuple[str, str]:
    """Retourne (token_id_yes, token_id_no) depuis la réponse Gamma API."""
    tokens = market.get("tokens", [])
    yes_id, no_id = "", ""
    for t in tokens:
        outcome = t.get("outcome", "").upper()
        if outcome == "YES":
            yes_id = t.get("token_id", "")
        elif outcome == "NO":
            no_id = t.get("token_id", "")
    return yes_id, no_id


def _extract_price(market: dict, outcome: str) -> float:
    tokens = market.get("tokens", [])
    for t in tokens:
        if t.get("outcome", "").upper() == outcome:
            return float(t.get("price", 0.0))
    return 0.0


# ── Fetch Gamma API ───────────────────────────────────────────────────────────

async def _fetch_btc_markets(client: httpx.AsyncClient) -> list[dict]:
    """Récupère tous les marchés BTC actifs depuis la Gamma API."""
    results = []
    keywords = ["bitcoin", "btc"]

    for kw in keywords:
        try:
            resp = await client.get(
                f"{GAMMA_API_URL}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "tag": kw,
                    "limit": 200,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            results.extend(markets)
        except Exception as e:
            log.warning(f"Gamma API fetch error (kw={kw}): {e}")

    # Déduplique par id
    seen = {}
    for m in results:
        mid = m.get("id") or m.get("conditionId", "")
        if mid and mid not in seen:
            seen[mid] = m

    return list(seen.values())


def _is_btc_5min(market: dict) -> bool:
    """Filtre : uniquement les marchés BTC 5-min."""
    q = market.get("question", "") or market.get("title", "")
    q_lower = q.lower()

    # Doit mentionner BTC/Bitcoin
    if "btc" not in q_lower and "bitcoin" not in q_lower:
        return False

    # Doit être un marché de prix (above/below)
    if "above" not in q_lower and "below" not in q_lower:
        return False

    # Durée ≤ 10 min (filtre les marchés horaires/quotidiens)
    end = market.get("endDate") or market.get("endDateIso", "")
    start = market.get("startDate") or market.get("startDateIso", "")
    if end and start:
        try:
            ts_end   = datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp()
            ts_start = datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp()
            duration_min = (ts_end - ts_start) / 60
            if duration_min > 15:
                return False
        except Exception:
            pass

    return True


# ── Discoverer principal ───────────────────────────────────────────────────────

async def discovery_loop(btc_spot_fn) -> None:
    """
    Boucle principale de découverte.
    btc_spot_fn : fonction callable qui retourne le prix BTC spot actuel (float)
    """
    log.info(f"Discoverer démarré (poll toutes les {DISCOVER_INTERVAL_SEC}s)")

    async with httpx.AsyncClient() as client:
        while True:
            try:
                await _discover_once(client, btc_spot_fn)
            except Exception as e:
                log.error(f"Discovery error: {e}")
            await asyncio.sleep(DISCOVER_INTERVAL_SEC)


async def _discover_once(client: httpx.AsyncClient, btc_spot_fn) -> None:
    now_ms = int(time.time() * 1000)
    raw_markets = await _fetch_btc_markets(client)

    new_count = 0
    for raw in raw_markets:
        if not _is_btc_5min(raw):
            continue

        market_id = raw.get("id") or raw.get("conditionId", "")
        if not market_id or market_id in _active_markets:
            continue

        # Parse
        question  = raw.get("question") or raw.get("title", "")
        strike, above = _parse_strike(question)
        yes_id, no_id = _parse_token_ids(raw)

        end_iso   = raw.get("endDate") or raw.get("endDateIso", "")
        start_iso = raw.get("startDate") or raw.get("startDateIso", "")

        try:
            expiry_ts_ms = int(datetime.fromisoformat(
                end_iso.replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            expiry_ts_ms = 0

        try:
            open_ts_ms = int(datetime.fromisoformat(
                start_iso.replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            open_ts_ms = now_ms

        btc_spot = btc_spot_fn()
        sec_to_expiry = max(0, (expiry_ts_ms - now_ms) // 1000)

        market = BtcMarket(
            market_id=market_id,
            condition_id=raw.get("conditionId", market_id),
            token_id_yes=yes_id,
            token_id_no=no_id,
            question=question,
            strike_price=strike,
            outcome_above=above,
            expiry_ts_ms=expiry_ts_ms,
            expiry_iso=end_iso,
            window_minutes=5,
            open_ts_ms=open_ts_ms,
            initial_volume=float(raw.get("volume", 0) or 0),
            initial_liquidity=float(raw.get("liquidity", 0) or 0),
            initial_price_yes=_extract_price(raw, "YES"),
            initial_price_no=_extract_price(raw, "NO"),
            maker_fee=float(raw.get("makerBaseFee", 0) or 0),
            taker_fee=float(raw.get("takerBaseFee", 0) or 0),
            min_order_size=float(raw.get("minimumOrderSize", 1) or 1),
            min_tick_size=float(raw.get("minimumTickSize", 0.01) or 0.01),
            btc_spot_at_open=btc_spot,
            moneyness_at_open=btc_spot - strike,
            moneyness_pct_at_open=((btc_spot - strike) / strike * 100) if strike else 0,
            seconds_to_expiry_at_open=sec_to_expiry,
        )

        _active_markets[market_id] = market

        # Persiste en DuckDB
        await push("btc_markets", _market_to_dict(market))

        log.info(
            f"Nouveau marché BTC: {question[:60]} | "
            f"strike={strike} | expiry={end_iso} | moneyness={market.moneyness_at_open:+.0f}"
        )
        new_count += 1

        # Notifie les listeners (WS subscribe, etc.)
        for cb in _on_new_market_callbacks:
            try:
                await cb(market)
            except Exception as e:
                log.warning(f"on_new_market callback error: {e}")

    # Purge les marchés expirés
    expired = [mid for mid, m in _active_markets.items()
               if m.expiry_ts_ms > 0 and m.expiry_ts_ms < now_ms]
    for mid in expired:
        log.info(f"Marché expiré retiré: {_active_markets[mid].question[:50]}")
        del _active_markets[mid]

    if new_count:
        log.info(f"Découverte: {new_count} nouveaux marchés | {len(_active_markets)} actifs")


def _market_to_dict(m: BtcMarket) -> dict:
    return {
        "market_id": m.market_id,
        "condition_id": m.condition_id,
        "token_id_yes": m.token_id_yes,
        "token_id_no": m.token_id_no,
        "question": m.question,
        "strike_price": m.strike_price,
        "outcome_above": m.outcome_above,
        "expiry_ts_ms": m.expiry_ts_ms,
        "expiry_iso": m.expiry_iso,
        "window_minutes": m.window_minutes,
        "open_ts_ms": m.open_ts_ms,
        "initial_volume": m.initial_volume,
        "initial_liquidity": m.initial_liquidity,
        "initial_price_yes": m.initial_price_yes,
        "initial_price_no": m.initial_price_no,
        "maker_fee": m.maker_fee,
        "taker_fee": m.taker_fee,
        "min_order_size": m.min_order_size,
        "min_tick_size": m.min_tick_size,
        "btc_spot_at_open": m.btc_spot_at_open,
        "moneyness_at_open": m.moneyness_at_open,
        "moneyness_pct_at_open": m.moneyness_pct_at_open,
        "seconds_to_expiry_at_open": m.seconds_to_expiry_at_open,
        "resolved": False,
        "winning_outcome": None,
        "final_btc_price": None,
        "closed_ts_ms": None,
    }
