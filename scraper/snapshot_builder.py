"""
Construit les market_snapshots quand un marché expire.
C'est la table la plus précieuse pour l'entraînement ML — résumé complet de la vie d'un marché.
"""
import asyncio
import time

from monitoring.logger import log
from storage.writer import push, _get_con
from scraper.ws_binance import get_btc_spot


async def build_snapshot(market_id: str, winning_outcome: str | None = None) -> None:
    """
    Calcule et persiste le snapshot final d'un marché expiré.
    Requête DuckDB pour agréger les ticks et trades.
    """
    try:
        con = _get_con()
        ts_ms = int(time.time() * 1000)
        btc_close = get_btc_spot()

        # Infos de base depuis btc_markets
        market_row = con.execute(
            "SELECT * FROM btc_markets WHERE market_id = ?", [market_id]
        ).fetchone()

        if not market_row:
            log.warning(f"Snapshot: marché {market_id} introuvable")
            return

        cols = [d[0] for d in con.description]
        m = dict(zip(cols, market_row))

        # Agrégats ticks
        tick_agg = con.execute("""
            SELECT
                COUNT(*)                  AS total_ticks,
                MIN(yes_mid)              AS min_price,
                MAX(yes_mid)              AS max_price,
                STDDEV(yes_mid)           AS std_price,
                MAX(volume_since_open)    AS total_volume,
                MAX(trade_count_since_open) AS total_trades,
                FIRST(yes_mid ORDER BY captured_ts_ms ASC)  AS open_price,
                LAST(yes_mid  ORDER BY captured_ts_ms ASC)  AS close_price
            FROM orderbook_ticks
            WHERE market_id = ?
        """, [market_id]).fetchone()

        # Prix à T-60s et T-30s avant expiry
        expiry_ms = m.get("expiry_ts_ms", 0)

        price_at_1min_row = con.execute("""
            SELECT yes_mid FROM orderbook_ticks
            WHERE market_id = ?
              AND captured_ts_ms <= ?
            ORDER BY captured_ts_ms DESC LIMIT 1
        """, [market_id, expiry_ms - 60_000]).fetchone()

        price_at_30s_row = con.execute("""
            SELECT yes_mid FROM orderbook_ticks
            WHERE market_id = ?
              AND captured_ts_ms <= ?
            ORDER BY captured_ts_ms DESC LIMIT 1
        """, [market_id, expiry_ms - 30_000]).fetchone()

        # BTC sur la fenêtre
        btc_open_row = con.execute("""
            SELECT price FROM btc_spot_ticks
            WHERE ts_ms >= ?
            ORDER BY ts_ms ASC LIMIT 1
        """, [m.get("open_ts_ms", 0)]).fetchone()

        btc_vol_row = con.execute("""
            SELECT STDDEV(price_delta_1s)
            FROM btc_spot_ticks
            WHERE ts_ms BETWEEN ? AND ?
        """, [m.get("open_ts_ms", 0), expiry_ms]).fetchone()

        # Durée réelle
        open_ms = m.get("open_ts_ms", ts_ms)
        duration_sec = max(0, (expiry_ms - open_ms) // 1000) if expiry_ms else 0

        btc_open  = btc_open_row[0] if btc_open_row else get_btc_spot()
        btc_move  = ((btc_close - btc_open) / btc_open * 100) if btc_open else 0.0
        strike    = m.get("strike_price", 0.0)

        snap = {
            "market_id":         market_id,
            "total_duration_sec": duration_sec,
            "total_ticks":       tick_agg[0] if tick_agg else 0,
            "total_trades":      int(tick_agg[7] if tick_agg else 0),
            "total_volume_usdc": float(tick_agg[6] if tick_agg else 0),
            "winning_outcome":   winning_outcome,

            "open_price_yes":    float(tick_agg[8]) if tick_agg and tick_agg[8] else 0.0,
            "close_price_yes":   float(tick_agg[9]) if tick_agg and tick_agg[9] else 0.0,
            "min_price_yes":     float(tick_agg[1]) if tick_agg and tick_agg[1] else 0.0,
            "max_price_yes":     float(tick_agg[2]) if tick_agg and tick_agg[2] else 0.0,
            "price_std_yes":     float(tick_agg[3]) if tick_agg and tick_agg[3] else 0.0,
            "price_at_1min":     float(price_at_1min_row[0]) if price_at_1min_row else 0.0,
            "price_at_30s":      float(price_at_30s_row[0])  if price_at_30s_row  else 0.0,

            "btc_open":          btc_open,
            "btc_close":         btc_close,
            "btc_move_pct":      btc_move,
            "btc_volatility":    float(btc_vol_row[0]) if btc_vol_row and btc_vol_row[0] else 0.0,
            "final_moneyness":   btc_close - strike if strike else 0.0,

            "snapshot_ts_ms":    ts_ms,
        }

        await push("market_snapshots", snap)
        log.info(
            f"Snapshot créé: {market_id[:20]} | "
            f"outcome={winning_outcome} | ticks={snap['total_ticks']} | "
            f"vol={snap['total_volume_usdc']:.0f} USDC"
        )

    except Exception as e:
        log.error(f"build_snapshot error [{market_id}]: {e}")
