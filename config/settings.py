"""Chargement de la config depuis .env / variables d'environnement."""
import os
from dotenv import load_dotenv

load_dotenv()

# ── APIs ──────────────────────────────────────────────────────────────────────
GAMMA_API_URL      = os.getenv("GAMMA_API_URL",      "https://gamma-api.polymarket.com")
CLOB_API_URL       = os.getenv("CLOB_API_URL",       "https://clob.polymarket.com")
POLYMARKET_WS_URL  = os.getenv("POLYMARKET_WS_URL",  "wss://ws-subscriptions-clob.polymarket.com/ws/market")
BINANCE_WS_URL     = os.getenv("BINANCE_WS_URL",     "wss://stream.binance.com:9443/ws")

# ── Cloudflare R2 ─────────────────────────────────────────────────────────────
R2_ACCOUNT_ID         = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID      = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY  = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME        = os.getenv("R2_BUCKET_NAME", "polymarket-btc-data")
R2_ENDPOINT_URL       = os.getenv("R2_ENDPOINT_URL", f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com")

# ── Timing ────────────────────────────────────────────────────────────────────
DISCOVER_INTERVAL_SEC = int(os.getenv("DISCOVER_INTERVAL_SEC", "5"))
TICK_INTERVAL_SEC     = int(os.getenv("TICK_INTERVAL_SEC",     "1"))
FLUSH_INTERVAL_SEC    = int(os.getenv("FLUSH_INTERVAL_SEC",    "60"))
UPLOAD_INTERVAL_SEC   = int(os.getenv("UPLOAD_INTERVAL_SEC",   "300"))

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "")

# ── Logs ──────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
