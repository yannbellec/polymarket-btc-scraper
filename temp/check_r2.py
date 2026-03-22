import sys, os, boto3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("R2_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    region_name="auto",
)

bucket = os.getenv("R2_BUCKET_NAME")

# Liste tous les fichiers
resp = s3.list_objects_v2(Bucket=bucket)
objects = resp.get("Contents", [])

if not objects:
    print("R2 vide — upload pas encore déclenché")
else:
    print(f"{len(objects)} fichiers dans R2 :\n")
    for obj in objects:
        size_kb = obj["Size"] / 1024
        print(f"  {obj['Key']:<55} {size_kb:.1f} KB  {obj['LastModified'].strftime('%H:%M:%S')}")

# Télécharge et inspecte market_snapshots
from datetime import datetime, timezone
date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
key = f"parquet/{date_str}/market_snapshots.parquet"

try:
    s3.download_file(bucket, key, "temp/market_snapshots_check.parquet")
    import duckdb
    con = duckdb.connect()
    rows = con.execute("""
        SELECT market_id, winning_outcome, total_ticks, total_trades,
               total_volume_usdc, btc_move_pct, snapshot_ts_ms
        FROM read_parquet('temp/market_snapshots_check.parquet')
        ORDER BY snapshot_ts_ms DESC
        LIMIT 10
    """).fetchall()
    print(f"\n=== {len(rows)} derniers snapshots dans R2 ===")
    for r in rows:
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(r[6]/1000, tz=timezone.utc)
        print(f"  {dt.strftime('%H:%M')}  outcome={r[1]:<4}  ticks={r[2]:<5}  trades={r[3]:<5}  vol={r[4]:.0f} USDC  btc={r[5]:+.2f}%")
except Exception as e:
    print(f"\nmarket_snapshots pas encore uploadé: {e}")