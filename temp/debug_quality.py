import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import duckdb
from datetime import datetime, timezone

con = duckdb.connect("data/btc_scraper.duckdb")

print("=== Fréquence des ticks ===")
rows = con.execute("""
    SELECT
        COUNT(*) as total,
        MIN(yes_spread) as min_spread,
        MAX(yes_spread) as max_spread,
        AVG(yes_spread) as avg_spread,
        MIN(yes_book_depth) as min_depth,
        MAX(yes_book_depth) as max_depth,
        AVG(book_imbalance) as avg_imbalance
    FROM orderbook_ticks
""").fetchone()
print(f"  Total ticks    : {rows[0]}")
print(f"  Spread min/max : {rows[1]:.4f} / {rows[2]:.4f}")
print(f"  Spread moyen   : {rows[3]:.4f}")
print(f"  Depth min/max  : {rows[4]} / {rows[5]}")
print(f"  Imbalance moy  : {rows[6]:.4f}")

print("\n=== Ticks par seconde (sample 10s) ===")
rows2 = con.execute("""
    SELECT
        captured_ts_ms / 1000 as sec,
        COUNT(*) as ticks
    FROM orderbook_ticks
    GROUP BY sec
    ORDER BY sec DESC
    LIMIT 10
""").fetchall()
for r in rows2:
    dt = datetime.fromtimestamp(r[0], tz=timezone.utc)
    print(f"  {dt.strftime('%H:%M:%S')}  →  {r[1]} ticks")

print("\n=== Trades — check timestamps ===")
rows3 = con.execute("""
    SELECT trade_ts_ms, price, size, side,
           time_to_expiry_at_trade_ms / 1000 as tte_sec
    FROM trades
    ORDER BY trade_ts_ms DESC
    LIMIT 5
""").fetchall()
for r in rows3:
    dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
    print(f"  {dt.strftime('%H:%M:%S.%f')}  p={r[1]:.3f}  size={r[2]:.1f}  {r[3]}  tte={r[4]:.0f}s")