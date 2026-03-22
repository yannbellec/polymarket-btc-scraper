"""Test live — vérifie RTDS + discoverer en 60s."""
import asyncio
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scraper.ws_rtds import rtds_loop, get_btc_spot
from scraper.discoverer import discovery_loop, get_active_markets, make_slug, current_window_ts

async def main():
    print(f"Window courante : {make_slug(current_window_ts())}")

    # Lance RTDS 5s pour avoir un prix
    print("Connexion RTDS...")
    rtds_task = asyncio.create_task(rtds_loop())
    await asyncio.sleep(5)

    print(f"BTC spot (Chainlink/Binance) : ${get_btc_spot():,.2f}")

    if get_btc_spot() == 0:
        print("ERREUR : aucun prix reçu du RTDS")
        rtds_task.cancel()
        return

    # Lance le discoverer 20s
    print("Lancement discoverer...")
    disc_task = asyncio.create_task(discovery_loop(get_btc_spot))
    await asyncio.sleep(20)

    markets = get_active_markets()
    print(f"\n{len(markets)} marchés actifs:")
    for m in markets.values():
        print(f"  {m.question[:60]}")
        print(f"    YES token : {m.token_id_yes[:20]}...")
        print(f"    Prix YES  : {m.initial_price_yes:.3f}")

    rtds_task.cancel()
    disc_task.cancel()
    print("\nTest terminé")

asyncio.run(main())