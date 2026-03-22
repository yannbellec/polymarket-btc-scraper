import sys, os, boto3, duckdb
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
from datetime import datetime, timezone
load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("R2_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    region_name="auto",
)
bucket   = os.getenv("R2_BUCKET_NAME")
date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
os.makedirs("temp/r2_inspect", exist_ok=True)

# Liste tous les fichiers
resp    = s3.list_objects_v2(Bucket=bucket)
objects = resp.get("Contents", [])
print(f"=== {len(objects)} fichiers dans R2 ===")
for obj in objects:
    print(f"  {obj['Key']:<55} {obj['Size']/1024:.1f} KB  {obj['LastModified'].strftime('%H:%M:%S')}")

# Télécharge et inspecte chaque table
tables = ["btc_markets", "orderbook_ticks", "trades", "btc_spot_ticks", "market_snapshots"]
con    = duckdb.connect()

for table in tables:
    key        = f"parquet/{date_str}/{table}.parquet"
    local_path = f"temp/r2_inspect/{table}.parquet"
    try:
        s3.download_file(bucket, key, local_path)
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{local_path}')").fetchone()[0]
        print(f"\n{'='*50}")
        print(f"TABLE: {table} — {count} lignes")
        print(f"{'='*50}")

        # Schéma
        cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{local_path}')").fetchall()
        print("Colonnes:")
        for c in cols:
            print(f"  {c[0]:<35} {c[1]}")

        # Aperçu
        print(f"\nDernières lignes:")
        if table == "btc_markets":
            rows = con.execute(f"""
                SELECT market_id, question, initial_price_yes, btc_spot_at_open, window_ts
                FROM read_parquet('{local_path}')
                ORDER BY open_ts_ms DESC LIMIT 5
            """).fetchall()
            for r in rows:
                print(f"  id={r[0]} | {r[1][:50]} | yes={r[2]:.3f} | btc_open={r[3]:.0f}")

        elif table == "orderbook_ticks":
            rows = con.execute(f"""
                SELECT captured_ts_ms, yes_mid, yes_spread, book_imbalance,
                       yes_book_depth, btc_spot, time_to_expiry_ms
                FROM read_parquet('{local_path}')
                ORDER BY captured_ts_ms DESC LIMIT 5
            """).fetchall()
            for r in rows:
                dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
                print(f"  {dt.strftime('%H:%M:%S')} | mid={r[1]:.3f} | spread={r[2]:.3f} | imbal={r[3]:.3f} | depth={r[4]} | btc={r[5]:.0f} | tte={r[6]//1000}s")

        elif table == "trades":
            rows = con.execute(f"""
                SELECT trade_ts_ms, price, size, side, slippage_vs_mid,
                       time_to_expiry_at_trade_ms
                FROM read_parquet('{local_path}')
                ORDER BY trade_ts_ms DESC LIMIT 5
            """).fetchall()
            for r in rows:
                dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
                print(f"  {dt.strftime('%H:%M:%S.%f')} | p={r[1]:.3f} | size={r[2]:.1f} | {r[3]} | slip={r[4]:.4f} | tte={r[5]//1000}s")

        elif table == "btc_spot_ticks":
            rows = con.execute(f"""
                SELECT ts_ms, price, price_delta_1s, price_delta_30s, volatility_1min
                FROM read_parquet('{local_path}')
                ORDER BY ts_ms DESC LIMIT 5
            """).fetchall()
            for r in rows:
                dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
                print(f"  {dt.strftime('%H:%M:%S')} | btc={r[1]:.2f} | d1s={r[2]:+.2f} | d30s={r[3]:+.2f} | vol={r[4]:.2f}")

        elif table == "market_snapshots":
            rows = con.execute(f"""
                SELECT market_id, winning_outcome, total_ticks, total_trades,
                       total_volume_usdc, btc_move_pct, open_price_yes,
                       close_price_yes, price_at_30s, btc_open, btc_close
                FROM read_parquet('{local_path}')
                ORDER BY snapshot_ts_ms DESC LIMIT 5
            """).fetchall()
            for r in rows:
                print(f"  id={r[0]} | {r[1]} | ticks={r[2]} | trades={r[3]} | vol={r[4]:.0f} USDC")
                print(f"    btc_move={r[5]:+.3f}% | yes: {r[6]:.3f}→{r[7]:.3f} | at30s={r[8]:.3f}")
                print(f"    btc_open={r[9]:.2f} | btc_close={r[10]:.2f}")

    except Exception as e:
        print(f"\n{table}: non disponible ({e})")