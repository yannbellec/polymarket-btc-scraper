"""
Point d'entrée principal — BTC Polymarket Scraper.
Process indépendant du bot météo.
"""
import asyncio
import os
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


async def restore_from_r2() -> None:
    """
    Au démarrage, télécharge les Parquet du jour depuis R2
    et les importe dans DuckDB pour continuité.
    """
    from storage.r2_uploader import _get_client
    from storage.schema import get_connection
    from datetime import datetime, timezone
    import duckdb

    client = _get_client()
    if client is None:
        log.info("R2 non configuré — démarrage sans restauration")
        return

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    tables   = ["btc_markets", "orderbook_ticks", "trades",
                "btc_spot_ticks", "market_snapshots"]
    restored = 0

    os.makedirs(f"data/parquet/{date_str}", exist_ok=True)

    bucket = os.getenv("R2_BUCKET_NAME", "polymarket-btc-data")

    for table in tables:
        r2_key     = f"parquet/{date_str}/{table}.parquet"
        local_path = f"data/parquet/{date_str}/{table}.parquet"
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda k=r2_key, p=local_path: client.download_file(bucket, k, p)
            )
            con = get_connection()
            con.execute(
                f"INSERT OR IGNORE INTO {table} SELECT * FROM read_parquet('{local_path}')"
            )
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info(f"R2 restore: {table} — {count} lignes importées")
            restored += 1
        except Exception as e:
            log.debug(f"R2 restore [{table}]: pas de fichier existant — démarrage propre")

    if restored:
        log.info(f"Restauration R2 terminée — {restored} tables rechargées")
    else:
        log.info("Aucune donnée R2 à restaurer — démarrage propre")


async def on_new_market(market) -> None:
    register_market(market)
    await subscribe_tokens([market.token_id_yes, market.token_id_no])
    log.info(f"Marché enregistré pour WS: {market.question[:60]}")


async def expiry_watcher_loop() -> None:
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

    # Restaure depuis R2 si disponible
    await restore_from_r2()

    tasks = [
        asyncio.create_task(binance_ws_loop(),            name="rtds"),
        asyncio.create_task(polymarket_ws_loop(),         name="polymarket_ws"),
        asyncio.create_task(discovery_loop(get_btc_spot), name="discoverer"),
        asyncio.create_task(flush_loop(),                 name="writer_flush"),
        asyncio.create_task(upload_loop(),                name="r2_upload"),
        asyncio.create_task(expiry_watcher_loop(),        name="expiry_watcher"),
        asyncio.create_task(status_loop(),                name="status"),
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