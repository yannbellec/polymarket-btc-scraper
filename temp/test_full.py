import asyncio, sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scraper.ws_rtds import rtds_loop, get_btc_spot
from scraper.discoverer import discovery_loop, get_active_markets
from scraper.book_poller import book_poll_loop
from scraper.ws_polymarket import polymarket_ws_loop, register_market, subscribe_tokens
from storage.writer import flush_loop, flush_all, get_buffer_stats
import duckdb

async def on_new_market(market):
    register_market(market)
    await subscribe_tokens([market.token_id_yes, market.token_id_no])

async def main():
    from scraper.discoverer import register_on_new_market
    register_on_new_market(on_new_market)

    tasks = [
        asyncio.create_task(rtds_loop()),
        asyncio.create_task(discovery_loop(get_btc_spot)),
        asyncio.create_task(book_poll_loop()),
        asyncio.create_task(polymarket_ws_loop()),
        asyncio.create_task(flush_loop()),
    ]

    print("Scraper complet — 60s de collecte...")
    await asyncio.sleep(60)

    # Flush final
    n = await flush_all()
    print(f"\nFlush: {n} lignes écrites")

    # Stats
    stats = get_buffer_stats()
    print(f"Buffers restants: {stats}")

    # Vérifie DuckDB
    con = duckdb.connect("data/btc_scraper.duckdb")
    for table in ["btc_markets", "orderbook_ticks", "trades", "btc_spot_ticks"]:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:<25} {count:>6} lignes")
        except:
            print(f"  {table:<25} absente")
    con.close()

    for t in tasks:
        t.cancel()

asyncio.run(main())