import sys, os, boto3, duckdb
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from storage.r2_list_utils import list_objects_v2_all
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
os.makedirs("temp/r2_check", exist_ok=True)

# Liste tous les fichiers (pagination si >1000 clés)
objects = list_objects_v2_all(s3, bucket)
print(f"=== {len(objects)} fichiers dans R2 ===")
for obj in objects:
    print(f"  {obj['Key']:<65} {obj['Size']/1024:.1f} KB  {obj['LastModified'].strftime('%H:%M:%S')}")

# Télécharge et concatène tous les fichiers par table (nouvelle structure)
tables = ["btc_markets", "orderbook_ticks", "trades", "btc_spot_ticks", "market_snapshots"]
con    = duckdb.connect()

for table in tables:
    # Récupère tous les fichiers de cette table
    prefix  = f"parquet/{table}/"
    keys    = [o["Key"] for o in objects if o["Key"].startswith(prefix) and o["Key"].endswith(".parquet")]

    if not keys:
        print(f"\n{table}: aucun fichier (nouvelle structure)")
        continue

    local_files = []
    for key in sorted(keys):
        local = f"temp/r2_check/{key.replace('/', '_')}"
        try:
            s3.download_file(bucket, key, local)
            local_files.append(local)
        except Exception as e:
            print(f"  Erreur download {key}: {e}")

    if not local_files:
        continue

    # Concatène tous les fichiers
    files_list = "', '".join(local_files)
    try:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet(['{files_list}'])").fetchone()[0]
        print(f"\n{'='*55}")
        print(f"TABLE: {table} — {count} lignes totales ({len(local_files)} fichiers)")
        print(f"{'='*55}")

        if table == "market_snapshots":
            rows = con.execute(f"""
                SELECT market_id, winning_outcome, total_ticks, total_trades,
                       total_volume_usdc, btc_move_pct, btc_open, btc_close,
                       open_price_yes, close_price_yes, snapshot_ts_ms
                FROM read_parquet(['{files_list}'])
                ORDER BY snapshot_ts_ms DESC
                LIMIT 5
            """).fetchall()
            print("5 derniers snapshots:")
            for r in rows:
                dt = datetime.fromtimestamp(r[10]/1000, tz=timezone.utc)
                print(f"  {dt.strftime('%H:%M')} | {r[1]:<4} | ticks={r[2]:<5} | trades={r[3]:<5} | vol={r[4]:.0f} USDC | btc={r[5]:+.3f}%")
                print(f"         yes: {r[8]:.3f}→{r[9]:.3f} | btc_open={r[6]:.2f} | btc_close={r[7]:.2f}")

        elif table == "orderbook_ticks":
            rows = con.execute(f"""
                SELECT captured_ts_ms, yes_mid, yes_spread, book_imbalance, btc_spot, time_to_expiry_ms
                FROM read_parquet(['{files_list}'])
                ORDER BY captured_ts_ms DESC
                LIMIT 3
            """).fetchall()
            print("3 derniers ticks:")
            for r in rows:
                dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
                print(f"  {dt.strftime('%H:%M:%S')} | mid={r[1]:.3f} | spread={r[2]:.3f} | imbal={r[3]:.3f} | btc={r[4]:.0f} | tte={r[5]//1000}s")

        elif table == "btc_spot_ticks":
            rows = con.execute(f"""
                SELECT ts_ms, price, price_delta_1s, price_delta_30s, volatility_1min
                FROM read_parquet(['{files_list}'])
                ORDER BY ts_ms DESC
                LIMIT 3
            """).fetchall()
            print("3 derniers prix BTC:")
            for r in rows:
                dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
                print(f"  {dt.strftime('%H:%M:%S')} | btc={r[1]:.2f} | d1s={r[2]:+.2f} | d30s={r[3]:+.2f} | vol={r[4]:.2f}")

        elif table == "trades":
            rows = con.execute(f"""
                SELECT trade_ts_ms, price, size, side, slippage_vs_mid
                FROM read_parquet(['{files_list}'])
                ORDER BY trade_ts_ms DESC
                LIMIT 3
            """).fetchall()
            print("3 derniers trades:")
            for r in rows:
                dt = datetime.fromtimestamp(r[0]/1000, tz=timezone.utc)
                print(f"  {dt.strftime('%H:%M:%S.%f')} | p={r[1]:.3f} | size={r[2]:.1f} | {r[3]} | slip={r[4]:.4f}")

    except Exception as e:
        print(f"  Erreur lecture {table}: {e}")
