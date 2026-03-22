import httpx, json

client = httpx.Client(timeout=10)

# Test 1 — endpoint /events avec slug crypto
print("=== /events slug=crypto ===")
resp = client.get("https://gamma-api.polymarket.com/events",
    params={"active": "true", "limit": 10, "slug": "crypto"})
print(resp.status_code, resp.text[:500])

# Test 2 — endpoint /series
print("\n=== /series ===")
resp = client.get("https://gamma-api.polymarket.com/series",
    params={"active": "true", "limit": 20})
print(resp.status_code, resp.text[:1000])

# Test 3 — markets sans filtre tag, juste actifs
print("\n=== /markets sans tag, 5 premiers ===")
resp = client.get("https://gamma-api.polymarket.com/markets",
    params={"active": "true", "closed": "false", "limit": 5})
data = resp.json()
if isinstance(data, list):
    markets = data
else:
    markets = data.get("markets", [])
for m in markets:
    print(json.dumps({
        "question": m.get("question","")[:60],
        "tags": m.get("tags", []),
        "slug": m.get("slug",""),
        "groupItemTitle": m.get("groupItemTitle",""),
    }, indent=2))