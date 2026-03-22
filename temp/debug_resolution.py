import httpx, json, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

market_id = "1669769"
client = httpx.Client(timeout=8)

resp = client.get(f"https://gamma-api.polymarket.com/markets/{market_id}")
print(f"Status: {resp.status_code}")
data = resp.json()
if isinstance(data, list):
    data = data[0]
print(f"outcomes      : {data.get('outcomes')}")
print(f"outcomePrices : {data.get('outcomePrices')}")
print(f"closed        : {data.get('closed')}")
print(f"resolved      : {data.get('resolved')}")
print(f"winner        : {data.get('winner')}")
print(f"resolutionPrice : {data.get('resolutionPrice')}")
print(f"lastTradePrice  : {data.get('lastTradePrice')}")
print()
print("Tous les champs:")
for k, v in data.items():
    if v not in (None, "", [], {}):
        print(f"  {k}: {str(v)[:80]}")