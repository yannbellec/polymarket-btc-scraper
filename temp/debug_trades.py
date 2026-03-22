import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import duckdb
from datetime import datetime, timezone

con = duckdb.connect("data/btc_scraper.duckdb")

rows = con.execute("""
    SELECT trade_ts_ms, price, size, side
    FROM trades
    ORDER BY trade_ts_ms ASC
    LIMIT 10
""").fetchall()

print("=== 10 premiers trades ===")
for r in rows:
    dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
    print(f"  {dt.strftime('%H:%M:%S.%f')}  price={r[1]:.3f}  size={r[2]:.1f}  side={r[3]}")

rows2 = con.execute("""
    SELECT trade_ts_ms, price, size, side
    FROM trades
    ORDER BY trade_ts_ms DESC
    LIMIT 10
""").fetchall()

print("\n=== 10 derniers trades ===")
for r in rows2:
    dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
    print(f"  {dt.strftime('%H:%M:%S.%f')}  price={r[1]:.3f}  size={r[2]:.1f}  side={r[3]}")

span = con.execute("SELECT MIN(trade_ts_ms), MAX(trade_ts_ms) FROM trades").fetchone()
duration = (span[1] - span[0]) / 1000
print(f"\nSpan total : {duration:.1f}s")
print(f"Trades/seconde : {749/duration:.1f}")