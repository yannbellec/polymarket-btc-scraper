"""
Snapshot complet de l'order book via CLOB REST API — toutes les TICK_INTERVAL_SEC secondes.
C'est la source principale de la table orderbook_ticks.
Le WebSocket complète avec les events trade, mais le REST donne le book complet.
"""
import asyncio
import time

import httpx

from config.settings import CLOB_API_URL, TICK_INTERVAL_SEC
from monitoring.logger import log
from storage.writer import push, next_tick_id
from scraper.ws_rtds import get_btc_spot
from scraper.discoverer import get_active_markets
from scraper.ws_polymarket import get_btc_horizon_fields, get_inter_market_fields, get_ofi_fields

# ── Historique de prix par marché pour les deltas ─────────────────────────────
# market_id → liste des 10 derniers mid YES
_price_history: dict[str, list[float]] = {}
# market_id → volume total cumulé depuis l'ouverture
_volume_cumul: dict[str, float] = {}
# market_id → nombre de trades depuis l'ouverture
_trade_count: dict[str, int] = {}


async def _fetch_book(client: httpx.AsyncClient, token_id: str) -> dict | None:
    """
    GET /book?token_id=<token_id>
    Retourne le carnet d'ordres complet avec tous les niveaux.
    """
    try:
        resp = await client.get(
            f"{CLOB_API_URL}/book",
            params={"token_id": token_id},
            timeout=3,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException:
        log.debug(f"Book timeout: {token_id[:20]}")
        return None
    except Exception as e:
        log.warning(f"Book fetch error [{token_id[:20]}]: {e}")
        return None


def _parse_book(book: dict) -> dict:
    """
    Extrait les métriques utiles depuis le book CLOB.
    Format Polymarket : {"bids": [{"price": "0.65", "size": "100"}, ...], "asks": [...]}
    """
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    if not bids and not asks:
        return {}

    # Trie bids décroissant, asks croissant
    bids_sorted = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
    asks_sorted = sorted(asks, key=lambda x: float(x.get("price", 0)))

    best_bid      = float(bids_sorted[0]["price"]) if bids_sorted else 0.0
    best_ask      = float(asks_sorted[0]["price"]) if asks_sorted else 0.0
    bid_size      = float(bids_sorted[0].get("size", 0)) if bids_sorted else 0.0
    ask_size      = float(asks_sorted[0].get("size", 0)) if asks_sorted else 0.0
    total_bid_liq = sum(float(b.get("size", 0)) for b in bids_sorted)
    total_ask_liq = sum(float(a.get("size", 0)) for a in asks_sorted)
    book_depth    = len(bids_sorted) + len(asks_sorted)

    return {
        "best_bid":      best_bid,
        "best_ask":      best_ask,
        "bid_size":      bid_size,
        "ask_size":      ask_size,
        "total_bid_liq": total_bid_liq,
        "total_ask_liq": total_ask_liq,
        "book_depth":    book_depth,
    }


async def _snapshot_market(client: httpx.AsyncClient, market) -> None:
    """Capture un tick complet pour un marché (YES + NO)."""
    now_ms   = int(time.time() * 1000)
    tte_ms   = max(0, market.expiry_ts_ms - now_ms)
    btc_spot = get_btc_spot()
    mid      = market.market_id

    # Fetch books YES et NO en parallèle
    yes_book_raw, no_book_raw = await asyncio.gather(
        _fetch_book(client, market.token_id_yes),
        _fetch_book(client, market.token_id_no),
        return_exceptions=True,
    )

    yes_data = _parse_book(yes_book_raw) if isinstance(yes_book_raw, dict) else {}
    no_data  = _parse_book(no_book_raw)  if isinstance(no_book_raw, dict)  else {}

    if not yes_data:
        return  # Book vide ou erreur — on skip ce tick

    # Calcul du mid et spread YES
    yes_bid = yes_data.get("best_bid", 0.0)
    yes_ask = yes_data.get("best_ask", 0.0)
    yes_mid = (yes_bid + yes_ask) / 2 if (yes_bid + yes_ask) > 0 else 0.0
    yes_spread = yes_ask - yes_bid if yes_ask > yes_bid else 0.0
    yes_spread_pct = (yes_spread / yes_mid * 100) if yes_mid > 0 else 0.0

    # Book imbalance : +1 = tout côté bid, -1 = tout côté ask
    bid_liq = yes_data.get("total_bid_liq", 0.0)
    ask_liq = yes_data.get("total_ask_liq", 0.0)
    total_liq = bid_liq + ask_liq
    imbalance = (bid_liq - ask_liq) / total_liq if total_liq > 0 else 0.0

    # Deltas de prix (poll ~1s : delta_tick = inter-poll ; delta_10s ≈ sur ~10 s)
    history = _price_history.setdefault(mid, [])
    delta_tick = yes_mid - history[-1]  if len(history) >= 1  else 0.0
    delta_10s  = yes_mid - history[-10] if len(history) >= 10 else 0.0
    history.append(yes_mid)
    if len(history) > 300:  # garde les 5 dernières minutes
        history.pop(0)

    btc_hz = get_btc_horizon_fields(mid, now_ms)
    im     = get_inter_market_fields(mid)
    ofi    = get_ofi_fields(mid, now_ms)

    row = {
        "tick_id":                next_tick_id(),
        "market_id":              mid,
        "captured_ts_ms":         now_ms,
        "time_to_expiry_ms":      tte_ms,

        # YES
        "yes_best_bid":           yes_bid,
        "yes_best_ask":           yes_ask,
        "yes_bid_size":           yes_data.get("bid_size", 0.0),
        "yes_ask_size":           yes_data.get("ask_size", 0.0),
        "yes_total_bid_liq":      bid_liq,
        "yes_total_ask_liq":      ask_liq,
        "yes_book_depth":         yes_data.get("book_depth", 0),

        # NO
        "no_best_bid":            no_data.get("best_bid", 0.0),
        "no_best_ask":            no_data.get("best_ask", 0.0),
        "no_bid_size":            no_data.get("bid_size", 0.0),
        "no_ask_size":            no_data.get("ask_size", 0.0),
        "no_total_bid_liq":       no_data.get("total_bid_liq", 0.0),
        "no_total_ask_liq":       no_data.get("total_ask_liq", 0.0),

        # Dérivés
        "yes_mid":                yes_mid,
        "yes_spread":             yes_spread,
        "yes_spread_pct":         yes_spread_pct,
        "book_imbalance":         imbalance,
        "yes_price_delta_tick":   delta_tick,
        "yes_price_delta_10s":    delta_10s,
        "volume_since_open":      _volume_cumul.get(mid, 0.0),
        "trade_count_since_open": _trade_count.get(mid, 0),
        "btc_spot":               btc_spot,
        "moneyness": btc_spot - market.btc_spot_at_open if market.btc_spot_at_open else 0.0,
        **btc_hz,
        **im,
        **ofi,
    }

    await push("orderbook_ticks", row)


async def book_poll_loop() -> None:
    """
    Boucle principale — snapshot tous les marchés actifs toutes les 1s.
    Utilise httpx AsyncClient avec connection pooling.
    """
    log.info(f"Book poller démarré (toutes les {TICK_INTERVAL_SEC}s)")

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        timeout=httpx.Timeout(3.0),
    ) as client:
        while True:
            start = time.monotonic()
            markets = get_active_markets()

            if markets:
                # Snapshot de tous les marchés actifs en parallèle
                tasks = [_snapshot_market(client, m) for m in markets.values()]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    log.debug(f"Book poller: {len(errors)} erreurs sur {len(tasks)} marchés")

            # Respecte le TICK_INTERVAL_SEC même si les requêtes prennent du temps
            elapsed = time.monotonic() - start
            sleep_time = max(0, TICK_INTERVAL_SEC - elapsed)
            await asyncio.sleep(sleep_time)
