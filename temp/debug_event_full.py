import httpx, json, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

client = httpx.Client(timeout=8)

# Fetch l'event complet par slug
slug = "btc-updown-5m-1774148100"
resp = client.get(f"https://gamma-api.polymarket.com/events?slug={slug}")
data = resp.json()

if data:
    event = data[0]
    print("=== EVENT TOP LEVEL ===")
    for k, v in event.items():
        if k != "markets":
            print(f"  {k}: {str(v)[:80]}")

    print("\n=== MARKETS[0] ALL FIELDS ===")
    markets = event.get("markets", [])
    if markets:
        m = markets[0]
        for k, v in m.items():
            print(f"  {k}: {str(v)[:100]}")