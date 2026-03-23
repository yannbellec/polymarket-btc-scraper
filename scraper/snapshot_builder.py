"""
Construit les market_snapshots quand un marché expire.

Les agrégats lisent DuckDB : tout ce qui est encore dans les buffers writer
donne ticks=0 / trades=0. On force donc plusieurs flush avant les SELECT,
notamment après l’attente Gamma (longue) où des derniers événements WS peuvent
encore arriver.
"""
import asyncio
import json
import time

import httpx

from monitoring.logger import log
from storage.writer import push, flush_all
from storage.schema import get_connection as _get_con
from scraper.ws_rtds import get_btc_spot, btc_price_for_horizon
from scraper.inter_market_context import record_completion

# Après expiry, derniers price_change / trades peuvent encore arriver ; pause courte + flush répété.
SNAPSHOT_DRAIN_INITIAL_SEC = 0.5
SNAPSHOT_DRAIN_SETTLE_SEC = 2.0
SNAPSHOT_DRAIN_FINAL_SEC = 0.5


async def _drain_buffers_to_duckdb() -> None:
    """Mémoire writer → DuckDB (toutes tables), en deux passes pour limiter la course."""
    await flush_all()
    await asyncio.sleep(SNAPSHOT_DRAIN_INITIAL_SEC)
    await flush_all()


def _parse_iso_to_ms(iso: str | None) -> int | None:
    if not iso or not isinstance(iso, str):
        return None
    try:
        s = iso.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        from datetime import datetime

        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


async def _drain_after_gamma_wait() -> None:
    """
    Après sleep(30) + fetch Gamma (potentiellement long), les buffers peuvent
    contenir des ticks/trades tardifs. On vide avant les agrégats SQL.
    """
    await flush_all()
    await asyncio.sleep(SNAPSHOT_DRAIN_SETTLE_SEC)
    await flush_all()
    await asyncio.sleep(SNAPSHOT_DRAIN_FINAL_SEC)
    await flush_all()


async def _fetch_winning_outcome(
    market_id: str, max_retries: int = 20
) -> tuple[str | None, int | None]:
    """
    Poll Gamma API jusqu'à ce qu'un outcome soit à ~1.0.
    Retourne (outcome YES/NO/None, closed_ts_ms depuis endDate API si présent).
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

                end_hint = _parse_iso_to_ms(data.get("endDate")) or _parse_iso_to_ms(
                    data.get("endDateIso")
                )

                # Cherche un outcome à 0.99 ou 1.0
                for i, price in enumerate(prices):
                    if float(price) >= 0.99:
                        outcome = str(outcomes[i]).lower() if i < len(outcomes) else None
                        out: str | None
                        if outcome in ("up", "yes"):
                            out = "YES"
                        elif outcome in ("down", "no"):
                            out = "NO"
                        else:
                            out = str(outcomes[i]) if outcome else None
                        return out, end_hint

                log.debug(
                    f"Marché {market_id} pas encore résolu "
                    f"(tentative {attempt+1}/{max_retries} | closed={closed} | prices={prices})"
                )

        except Exception as e:
            log.warning(f"fetch_winning_outcome error [{market_id}]: {e}")

        await asyncio.sleep(30)

    log.warning(f"Marché {market_id} non résolu après {max_retries * 30}s")
    return None, None


def _apply_btc_markets_resolution(
    con,
    market_id: str,
    m: dict,
    winning_outcome: str | None,
    gamma_closed_ts_ms: int | None,
    btc_close_snapshot: float,
) -> None:
    """
    Met à jour btc_markets après résolution (API Gamma + prix BTC de référence).
    resolution_ts_ms sert au watermark export Parquet (ligne ré-émise après UPDATE).
    """
    expiry_ms = int(m.get("expiry_ts_ms") or 0)
    final_btc = btc_price_for_horizon(expiry_ms) if expiry_ms else None
    if final_btc is None or final_btc <= 0:
        final_btc = float(btc_close_snapshot)

    closed_ts = gamma_closed_ts_ms if gamma_closed_ts_ms else expiry_ms
    if not closed_ts:
        closed_ts = int(time.time() * 1000)

    now_ms = int(time.time() * 1000)
    resolved = winning_outcome is not None

    con.execute(
        """
        UPDATE btc_markets SET
            resolved = ?,
            winning_outcome = ?,
            final_btc_price = ?,
            closed_ts_ms = ?,
            resolution_ts_ms = ?
        WHERE market_id = ?
        """,
        [resolved, winning_outcome, final_btc, closed_ts, now_ms, market_id],
    )
    log.info(
        f"btc_markets résolution persistée: {market_id} | resolved={resolved} | "
        f"outcome={winning_outcome} | final_btc={final_btc:.2f} | "
        f"closed_ts_ms={closed_ts} | resolution_ts_ms={now_ms}"
    )


async def build_snapshot(market_id: str, market_obj=None, winning_outcome: str | None = None) -> None:
    try:
        # Tout ce qui est déjà en RAM (flush intervalle 60s) doit être en DuckDB avant agrégats
        await _drain_buffers_to_duckdb()

        # Attente côté Gamma (résolution API) — pendant ce temps le WS peut encore pousser des lignes
        gamma_closed_ms: int | None = None
        if winning_outcome is None:
            await asyncio.sleep(30)
            winning_outcome, gamma_closed_ms = await _fetch_winning_outcome(market_id)
            log.info(f"Outcome résolu: {market_id} → {winning_outcome}")
        else:
            log.info(f"Outcome fourni pour snapshot: {market_id} → {winning_outcome}")

        # Avant COUNT/MIN/MAX : vider à nouveau (sinon snapshot « vide » alors que les données existent en buffer)
        await _drain_after_gamma_wait()

        con = _get_con()
        ts_ms     = int(time.time() * 1000)
        btc_close = get_btc_spot()

        if market_obj:
            m = {
                "market_id":         market_obj.market_id,
                "window_ts":         market_obj.window_ts,
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

        # Mise à jour explicite btc_markets (résolution Gamma + BTC à l’expiry)
        _apply_btc_markets_resolution(
            con, market_id, m, winning_outcome, gamma_closed_ms, btc_close
        )

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
        wts = m.get("window_ts")
        if wts is not None:
            record_completion(
                int(wts),
                market_id,
                winning_outcome,
                float(btc_move),
                float(close_price),
            )
        log.info(
            f"Snapshot créé: {market_id} | outcome={winning_outcome} | "
            f"ticks={total_ticks} | trades={total_trades_n} | "
            f"vol={total_volume:.0f} USDC | BTC move={btc_move:+.2f}%"
        )

        if total_ticks == 0 and total_trades_n == 0:
            log.warning(
                f"Snapshot sans activité orderbook/trades pour {market_id} — "
                "souvent causé par WS Polymarket coupé avant la fin de marché ou absence de flush "
                "avant agrégation ; vérifier les logs WS et les drains flush."
            )

    except Exception as e:
        log.error(f"build_snapshot error [{market_id}]: {e}")