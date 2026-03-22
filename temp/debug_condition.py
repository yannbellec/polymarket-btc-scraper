import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import duckdb

con = duckdb.connect("data/btc_scraper.duckdb")
rows = con.execute("SELECT market_id, condition_id, question FROM btc_markets").fetchall()
for r in rows:
    print(f"market_id={r[0]} | condition_id={r[1][:30]}... | {r[2][:50]}")