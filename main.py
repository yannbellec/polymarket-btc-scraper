"""
Point d'entrée principal — BTC Polymarket Scraper.
Process indépendant du bot météo.

Architecture :
  rtds_loop()           → prix BTC spot en temps réel (Binance + Chainlink via Polymarket)
  discovery_loop()      → détecte les marchés BTC 5-min actifs
  polymarket_ws_loop()  → events trades et prix en temps réel
  flush_loop()          → buffer mémoire → DuckDB toutes les 60s
  upload_loop()         → DuckDB → Parquet → R2 toutes les 5 min
  expiry_watcher_loop() → déclenche les snapshots à l'expiry
  status_loop()         → log du statut toutes les 60s
"""
import asyncio
import sys
import time

from monitoring.logger import log
from monitoring.telegram_alert import alert_start, alert_stop, alert_error

from scraper.ws_rtds import binance_ws_loop, get_btc_spot
from scraper.ws_polymarket import polymarket_ws_loop, register_market, subscribe_tokens
from scraper.discoverer import discovery_loop, register_on_new_market, get_active_markets
from scraper.snapshot_builder import build_snapshot
from storage.writer import flush_loop, flush_all, get_buffer_stats
from storage.r2_uploader import upload_loop


async def on_new_market(market) -> None:
    """Appelé par le discoverer quand un nouveau marché BTC est trouvé."""
    register_market(market)
    await subscribe_tokens([market.token_id_yes, market.token_id_no])
    log.info(f"Marché enregistré pour WS: {market.question[:60]}")


async def expiry_watcher_loop() -> None:
    """Surveille les expiries et déclenche les snapshots."""
    triggered = set()
    while True:
        await asyncio.sleep(5)
        now_ms = int(time.time() * 1000)
        for market_id, market in list(get_active_markets().items()):
            if 0 < now_ms - market.expiry_ts_ms < 15_000 and market_id not in triggered:
                triggered.add(market_id)
                log.info(f"Expiry détectée: {market.question[:50]}")
                await asyncio.sleep(5)
                await flush_all()
                await build_snapshot(market_id, market_obj=market, winning_outcome=None)


async def status_loop() -> None:
    """Log le statut toutes les 60s pour monitoring Railway."""
    while True:
        await asyncio.sleep(60)
        stats = get_buffer_stats()
        total_pending = sum(stats.values())
        log.info(f"Status — buffers: {stats} | total pending: {total_pending}")


async def run() -> None:
    log.info("=" * 56)
    log.info("  BTC Polymarket Scraper — démarrage")
    log.info("=" * 56)

    alert_start()
    register_on_new_market(on_new_market)

    tasks = [
        asyncio.create_task(binance_ws_loop(),             name="rtds"),
        asyncio.create_task(polymarket_ws_loop(),          name="polymarket_ws"),
        asyncio.create_task(discovery_loop(get_btc_spot),  name="discoverer"),
        asyncio.create_task(flush_loop(),                  name="writer_flush"),
        asyncio.create_task(upload_loop(),                 name="r2_upload"),
        asyncio.create_task(expiry_watcher_loop(),         name="expiry_watcher"),
        asyncio.create_task(status_loop(),                 name="status"),
    ]

    log.info(f"  {len(tasks)} coroutines démarrées")
    log.info("=" * 56)

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            exc = task.exception()
            if exc:
                log.error(f"Tâche {task.get_name()} crashée: {exc}")
                alert_error(f"Task {task.get_name()}: {exc}")

    except KeyboardInterrupt:
        log.info("Arrêt manuel (Ctrl+C)")

    except Exception as e:
        log.exception(f"Erreur critique: {e}")
        alert_error(str(e))

    finally:
        log.info("Flush final avant arrêt...")
        n = await flush_all()
        log.info(f"Flush final: {n} lignes écrites")
        alert_stop("Arrêt propre")
        for task in tasks:
            task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(0)