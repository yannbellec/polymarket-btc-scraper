"""
Point d'entrée principal — BTC Polymarket Scraper.
"""
import asyncio
import sys
import time

from monitoring.logger import log
from monitoring.telegram_alert import alert_start, alert_stop, alert_error

from scraper.ws_rtds import binance_ws_loop, get_btc_spot
from scraper.ws_polymarket import polymarket_ws_loop, register_market, subscribe_tokens
from scraper.discoverer import (
    discovery_loop,
    register_on_new_market,
    get_active_markets,
    expiry_snapshot_triggered,
)
from scraper.snapshot_builder import build_snapshot
from storage.writer import flush_loop, flush_all, get_buffer_stats, export_incremental
from storage.r2_uploader import upload_loop, upload_file


async def restart_on_crash(name: str, coro_func, *args, **kwargs) -> None:
    """
    Exécute une boucle async (typiquement while True) ; en cas d'exception,
    log + alerte Telegram + backoff exponentiel puis relance. N'arrête pas les autres tâches.
    """
    backoff = 1.0
    while True:
        try:
            await coro_func(*args, **kwargs)
            log.warning(f"[{name}] coroutine terminée — relance")
            backoff = 1.0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception(f"[{name}] crash — redémarrage dans {backoff:.0f}s: {e}")
            try:
                alert_error(f"{name}: {e}")
            except Exception:
                pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


async def on_new_market(market) -> None:
    register_market(market)
    await subscribe_tokens([market.token_id_yes, market.token_id_no])
    log.info(f"Marché enregistré: {market.question[:60]}")


async def expiry_watcher_loop() -> None:
    """Déclenche snapshot + export immédiat à chaque expiry."""
    while True:
        await asyncio.sleep(5)
        now_ms = int(time.time() * 1000)
        for market_id, market in list(get_active_markets().items()):
            if 0 < now_ms - market.expiry_ts_ms < 15_000 and market_id not in expiry_snapshot_triggered:
                expiry_snapshot_triggered.add(market_id)
                log.info(f"Expiry détectée: {market.question[:50]}")
                # Double flush pour garantir que tous les buffers sont en DuckDB
                await asyncio.sleep(3)
                await flush_all()
                await asyncio.sleep(1)
                await flush_all()
                # Build snapshot depuis DuckDB
                await build_snapshot(market_id, market_obj=market, winning_outcome=None)
                # Export immédiat vers R2
                await flush_all()
                paths = await export_incremental()
                for path in paths:
                    await upload_file(path)


async def status_loop() -> None:
    while True:
        await asyncio.sleep(60)
        stats         = get_buffer_stats()
        total_pending = sum(stats.values())
        log.info(f"Status — buffers: {stats} | pending: {total_pending}")


async def run() -> None:
    log.info("=" * 56)
    log.info("  BTC Polymarket Scraper — démarrage")
    log.info("=" * 56)

    alert_start()
    register_on_new_market(on_new_market)

    tasks = [
        asyncio.create_task(restart_on_crash("rtds", binance_ws_loop), name="rtds"),
        asyncio.create_task(restart_on_crash("polymarket_ws", polymarket_ws_loop), name="polymarket_ws"),
        asyncio.create_task(restart_on_crash("discoverer", discovery_loop, get_btc_spot), name="discoverer"),
        asyncio.create_task(restart_on_crash("writer_flush", flush_loop), name="writer_flush"),
        asyncio.create_task(restart_on_crash("r2_upload", upload_loop), name="r2_upload"),
        asyncio.create_task(restart_on_crash("expiry_watcher", expiry_watcher_loop), name="expiry_watcher"),
        asyncio.create_task(restart_on_crash("status", status_loop), name="status"),
    ]

    log.info(f"  {len(tasks)} coroutines démarrées (restart_on_crash)")
    log.info("=" * 56)

    try:
        await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        log.info("Arrêt manuel (Ctrl+C)")

    except asyncio.CancelledError:
        log.info("Annulation")

    except Exception as e:
        log.exception(f"Erreur critique: {e}")
        alert_error(str(e))

    finally:
        log.info("Flush final avant arrêt...")
        await flush_all()
        paths = await export_incremental()
        for path in paths:
            await upload_file(path)
        log.info("Export final terminé")
        alert_stop("Arrêt propre")
        for task in tasks:
            task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(0)
