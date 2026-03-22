"""
Point d'entrée principal — BTC Polymarket Scraper.
Process indépendant du bot météo.

Architecture :
  binance_ws_loop()     → prix BTC spot en temps réel
  discovery_loop()      → détecte les marchés BTC 5-min actifs
  polymarket_ws_loop()  → events trades et prix en temps réel
  book_poll_loop()      → snapshot order book complet toutes les 1s
  flush_loop()          → buffer mémoire → DuckDB toutes les 60s
  upload_loop()         → DuckDB → Parquet → R2 toutes les 5 min
"""
import asyncio
import sys

from monitoring.logger import log
from monitoring.telegram_alert import alert_start, alert_stop, alert_error
from config.settings import FLUSH_INTERVAL_SEC, UPLOAD_INTERVAL_SEC

from scraper.ws_binance import binance_ws_loop, get_btc_spot
from scraper.ws_polymarket import polymarket_ws_loop, register_market, subscribe_tokens
from scraper.discoverer import discovery_loop, register_on_new_market
from scraper.book_poller import book_poll_loop
from storage.writer import flush_loop, flush_all, get_buffer_stats
from storage.r2_uploader import upload_loop


# ── Callback : nouveau marché découvert ───────────────────────────────────────

async def on_new_market(market) -> None:
    """Appelé par le discoverer quand un nouveau marché BTC est trouvé."""
    register_market(market)
    await subscribe_tokens([market.token_id_yes, market.token_id_no])
    log.info(f"Marché enregistré pour WS: {market.question[:60]}")


# ── Boucle de monitoring console ─────────────────────────────────────────────

async def status_loop() -> None:
    """Log le statut toutes les 60s pour monitoring Railway."""
    while True:
        await asyncio.sleep(60)
        stats = get_buffer_stats()
        total_pending = sum(stats.values())
        log.info(
            f"Status — buffers: {stats} | total pending: {total_pending}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

async def run() -> None:
    log.info("=" * 56)
    log.info("  BTC Polymarket Scraper — démarrage")
    log.info("=" * 56)

    alert_start()

    # Enregistre le callback de découverte
    register_on_new_market(on_new_market)

    # Lance toutes les coroutines en parallèle
    tasks = [
        asyncio.create_task(binance_ws_loop(),    name="binance_ws"),
        asyncio.create_task(polymarket_ws_loop(), name="polymarket_ws"),
        asyncio.create_task(discovery_loop(get_btc_spot), name="discoverer"),
        asyncio.create_task(book_poll_loop(),     name="book_poller"),
        asyncio.create_task(flush_loop(),         name="writer_flush"),
        asyncio.create_task(upload_loop(),        name="r2_upload"),
        asyncio.create_task(status_loop(),        name="status"),
    ]

    log.info(f"  {len(tasks)} coroutines démarrées")
    log.info("=" * 56)

    try:
        # Attend que toutes les tâches tournent (loop infinie)
        # Si une tâche crash, on la voit et on log
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
        # Flush final avant arrêt
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
