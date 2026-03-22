import asyncio, sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scraper.ws_rtds import rtds_loop, get_btc_spot
from scraper.discoverer import discovery_loop, get_active_markets, register_on_new_market
from scraper.ws_polymarket import polymarket_ws_loop, register_market, subscribe_tokens
from scraper.snapshot_builder import build_snapshot
from storage.writer import flush_loop, flush_all
import duckdb


async def on_new_market(market):
    register_market(market)
    await subscribe_tokens([market.token_id_yes, market.token_id_no])


async def expiry_watcher():
    triggered = set()
    while True:
        await asyncio.sleep(5)
        now_ms = int(time.time() * 1000)
        for market_id, market in list(get_active_markets().items()):
            tte = market.expiry_ts_ms - now_ms
            print(f"  [{market.question[20:50]}] tte={tte//1000}s")
            if -15_000 < now_ms - market.expiry_ts_ms < 15_000 and market_id not in triggered:
                triggered.add(market_id)
                print(f"EXPIRY DETECTEE: {market.question[:60]}")
                await asyncio.sleep(5)   # laisse le temps au flush auto
                await flush_all()
                await build_snapshot(market_id, market_obj=market, winning_outcome=None)
                await flush_all()
                con = duckdb.connect("data/btc_scraper.duckdb")
                n = con.execute("SELECT COUNT(*) FROM market_snapshots").fetchone()[0]
                print(f"market_snapshots: {n} lignes")
                row = con.execute("SELECT * FROM market_snapshots ORDER BY snapshot_ts_ms DESC LIMIT 1").fetchone()
                if row:
                    cols = [d[0] for d in con.description]
                    for c, v in zip(cols, row):
                        print(f"  {c:<25} {v}")
                con.close()


register_on_new_market(on_new_market)


async def main():
    print("En attente d'une expiry... (max 5 min)")
    tasks = [
        asyncio.create_task(rtds_loop()),
        asyncio.create_task(discovery_loop(get_btc_spot)),
        asyncio.create_task(polymarket_ws_loop()),
        asyncio.create_task(flush_loop()),
        asyncio.create_task(expiry_watcher()),
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())