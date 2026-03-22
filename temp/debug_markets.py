import sys, os, httpx, asyncio
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import duckdb, time

con = duckdb.connect("data/btc_scraper.duckdb")

print("=== Marchés capturés en DuckDB ===")
rows = con.execute("""
    SELECT market_id, question, window_minutes, initial_volume, initial_price_yes
    FROM btc_markets ORDER BY open_ts_ms
""").fetchall()
for r in rows:
    print(f"  [{r[2]}min] {r[1][:65]}")
    print(f"         vol={r[3]:.0f} USDC | prix_yes={r[4]:.3f} | id={r[0]}")

print("\n=== Test slugs connus ===")
now = int(time.time())
slugs_to_test = []
for prefix, window in [("btc-updown-5m", 300), ("btc-updown-1m", 60), ("btc-updown-1h", 3600)]:
    wts = now - (now % window)
    slugs_to_test.append((f"{prefix}-{wts}", prefix))

async def test_slugs():
    async with httpx.AsyncClient(timeout=8) as client:
        for slug, prefix in slugs_to_test:
            for endpoint in [
                f"https://gamma-api.polymarket.com/events?slug={slug}",
                f"https://gamma-api.polymarket.com/markets?slug={slug}",
            ]:
                resp = await client.get(endpoint)
                data = resp.json()
                found = bool(data and (isinstance(data, list) and data or
                             isinstance(data, dict) and (data.get("events") or data.get("markets"))))
                print(f"  {'OK' if found else 'NOT FOUND'} | {slug} via {endpoint.split('?')[0].split('/')[-1]}")
                if found:
                    break

asyncio.run(test_slugs())