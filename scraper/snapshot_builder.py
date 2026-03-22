"""
Construit les market_snapshots quand un marché expire.
"""
import asyncio
import json
import time

import httpx

from monitoring.logger import log
from storage.writer import push
from storage.schema import get_connection as _get_con
from scraper.ws_rtds import get_btc_spot


async def _fetch_winning_outcome(market_id: str, max_retries: int = 20) -> str | None:
    """
    Attend que le marché soit closed=True sur la Gamma API.
    Le settlement on-chain est instantané mais la Gamma API met 2-5 min à se mettre à jour.
    Retry toutes les 30s — max 10 minutes d'attente.
    """
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                resp = await client.get(
                    f"https://gamma-api.polymarket.com/markets/{market_id}"
                )
                if resp.status_code != 200:
                    await asyncio.sleep(30)
                    continue

                data = resp.json()
                if isinstance(data, list):
                    data = data[0] if data else {}
                if not data:
                    await asyncio.sleep(30)
                    continue

                closed   = data.get("closed", False)
                resolved = data.get("resolved", False)

                outcomes_raw = data.get("outcomes", '["Up","Down"]')
                outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw

                prices_raw = data.get("outcomePrices", "[0.5,0.5]")
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw

                # Cherche un outcome à 0.99 ou 1.0
                for i, price in enumerate(prices):
                    if float(price) >= 0.99:
                        outcome = str(outcomes[i]).lower() if i < len(outcomes) else None
                        if outcome in ("up", "yes"):
                            return "YES"
                        elif outcome in ("down", "no"):
                            return "NO"
                        return str(outcomes[i]) if outcome else None

                log.debug(
                    f"Marché {market_id} pas encore résolu "
                    f"(tentative {attempt+1}/{max_retries} | closed={closed} | prices={prices})"
                )

        except Exception as e:
            log.warning(f"fetch_winning_outcome error [{market_id}]: {e}")

        await asyncio.sleep(30)

    log.warning(f"Marché {market_id} non résolu après {max_retries * 30}s")
    return None


async def build_snapshot(market_id: str, market_obj=None, winning_outcome: str | None = None) -> None:
    try:
        # Attend 10s pour que le marché soit résolu sur la Gamma API
        if winning_outcome is None:
            await asyncio.sleep(30)
            winning_outcome = await _fetch_winning_outcome(market_id)
            log.info(f"Outcome résolu: {market_id} → {winning_outcome}")

        con = _get_con()
        ts_ms     = int(time.time() * 1000)
        btc_close = get_btc_spot()

        if market_obj:
            m = {
                "market_id":         market_obj.market_id,
                "expiry_ts_ms":      market_obj.expiry_ts_ms,
                "open_ts_ms":        market_obj.open_ts_ms,
                "btc_spot_at_open":  market_obj.btc_spot_at_open,
            }
        else:
            market_row = con.execute(
                "SELECT * FROM btc_markets WHERE market_id = ?", [market_id]
            ).fetchone()
            if not market_row:
                log.warning(f"Snapshot: marché {market_id} introuvable")
                return
            cols = [d[0] for d in con.description]
            m = dict(zip(cols, market_row))

        expiry_ms    = m.get("expiry_ts_ms", 0)
        open_ms      = m.get("open_ts_ms", ts_ms)
        duration_sec = max(0, (expiry_ms - open_ms) // 1000) if expiry_ms else 0

        # BTC open — depuis btc_spot_ticks en priorité, sinon depuis le marché
        btc_open_row = con.execute("""
            SELECT price FROM btc_spot_ticks
            WHERE ts_ms >= ?
            ORDER BY ts_ms ASC LIMIT 1
        """, [open_ms]).fetchone()

        btc_open = float(btc_open_row[0]) if btc_open_row else m.get("btc_spot_at_open", 0.0)
        if not btc_open:
            btc_open = get_btc_spot()

        # Agrégats ticks
        tick_agg = con.execute("""
            SELECT
                COUNT(*)                                                  AS total_ticks,
                COALESCE(MIN(yes_mid), 0)                                 AS min_price,
                COALESCE(MAX(yes_mid), 0)                                 AS max_price,
                COALESCE(STDDEV(yes_mid), 0)                              AS std_price,
                COALESCE(FIRST(yes_mid ORDER BY captured_ts_ms ASC), 0)  AS open_price,
                COALESCE(LAST(yes_mid  ORDER BY captured_ts_ms ASC), 0)  AS close_price
            FROM orderbook_ticks
            WHERE market_id = ?
        """, [market_id]).fetchone()

        total_ticks = int(tick_agg[0])   if tick_agg else 0
        min_price   = float(tick_agg[1]) if tick_agg else 0.0
        max_price   = float(tick_agg[2]) if tick_agg else 0.0
        std_price   = float(tick_agg[3]) if tick_agg else 0.0
        open_price  = float(tick_agg[4]) if tick_agg else 0.0
        close_price = float(tick_agg[5]) if tick_agg else 0.0

        # Agrégats trades — depuis la table trades directement
        trades_agg = con.execute("""
            SELECT
                COUNT(*)      AS total_trades,
                COALESCE(SUM(size), 0) AS total_volume
            FROM trades
            WHERE market_id = ?
        """, [market_id]).fetchone()

        total_trades_n = int(trades_agg[0])   if trades_agg and trades_agg[0] else 0
        total_volume   = float(trades_agg[1]) if trades_agg and trades_agg[1] else 0.0

        # Prix à T-60s et T-30s
        price_at_1min_row = con.execute("""
            SELECT yes_mid FROM orderbook_ticks
            WHERE market_id = ? AND captured_ts_ms <= ?
            ORDER BY captured_ts_ms DESC LIMIT 1
        """, [market_id, expiry_ms - 60_000]).fetchone()

        price_at_30s_row = con.execute("""
            SELECT yes_mid FROM orderbook_ticks
            WHERE market_id = ? AND captured_ts_ms <= ?
            ORDER BY captured_ts_ms DESC LIMIT 1
        """, [market_id, expiry_ms - 30_000]).fetchone()

        # Volatilité BTC sur la fenêtre
        btc_vol_row = con.execute("""
            SELECT COALESCE(STDDEV(price_delta_1s), 0)
            FROM btc_spot_ticks
            WHERE ts_ms BETWEEN ? AND ?
        """, [open_ms, expiry_ms]).fetchone()

        btc_move = ((btc_close - btc_open) / btc_open * 100) if btc_open else 0.0

        snap = {
            "market_id":          market_id,
            "total_duration_sec": duration_sec,
            "total_ticks":        total_ticks,
            "total_trades":       total_trades_n,
            "total_volume_usdc":  total_volume,
            "winning_outcome":    winning_outcome,
            "open_price_yes":     open_price,
            "close_price_yes":    close_price,
            "min_price_yes":      min_price,
            "max_price_yes":      max_price,
            "price_std_yes":      std_price,
            "price_at_1min":      float(price_at_1min_row[0]) if price_at_1min_row else 0.0,
            "price_at_30s":       float(price_at_30s_row[0])  if price_at_30s_row  else 0.0,
            "btc_open":           btc_open,
            "btc_close":          btc_close,
            "btc_move_pct":       btc_move,
            "btc_volatility":     float(btc_vol_row[0]) if btc_vol_row and btc_vol_row[0] else 0.0,
            "final_moneyness":    btc_close - btc_open,
            "snapshot_ts_ms":     ts_ms,
        }

        await push("market_snapshots", snap)
        log.info(
            f"Snapshot créé: {market_id} | outcome={winning_outcome} | "
            f"ticks={total_ticks} | trades={total_trades_n} | "
            f"vol={total_volume:.0f} USDC | BTC move={btc_move:+.2f}%"
        )

    except Exception as e:
        log.error(f"build_snapshot error [{market_id}]: {e}")