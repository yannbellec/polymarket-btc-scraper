import asyncio, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scraper.ws_rtds import rtds_loop, get_btc_spot
from scraper.discoverer import discovery_loop, get_active_markets, register_on_new_market
from scraper.ws_polymarket import register_market, subscribe_tokens
from storage.writer import push, _buffers

async def on_new_market(market):
    register_market(market)
    await subscribe_tokens([market.token_id_yes, market.token_id_no])

async def main():
    register_on_new_market(on_new_market)

    # Lance RTDS + discoverer
    asyncio.create_task(rtds_loop())
    asyncio.create_task(discovery_loop(get_btc_spot))
    await asyncio.sleep(5)

    markets = get_active_markets()
    print(f"\n{len(markets)} marchés actifs")
    if not markets:
        print("ERREUR : aucun marché découvert")
        return

    # Test manuel du book poller sur 1 marché
    import httpx
    from scraper.book_poller import _snapshot_market

    market = list(markets.values())[0]
    print(f"Marché : {market.question[:60]}")
    print(f"YES token : {market.token_id_yes[:20]}...")
    print(f"NO  token : {market.token_id_no[:20]}...")

    async with httpx.AsyncClient(timeout=5.0) as client:
        print("\nSnapshot manuel...")
        await _snapshot_market(client, market)

    print(f"\nBuffer orderbook_ticks après snapshot : {len(_buffers['orderbook_ticks'])} lignes")
    if _buffers['orderbook_ticks']:
        row = _buffers['orderbook_ticks'][0]
        print("Première ligne :")
        for k, v in row.items():
            print(f"  {k:<30} {v}")
    else:
        print("AUCUNE LIGNE — le snapshot ne push rien")

asyncio.run(main())