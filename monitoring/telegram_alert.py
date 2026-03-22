"""Alertes Telegram — démarrage, arrêt, erreurs critiques."""
import httpx
from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from monitoring.logger import log

BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def _send(msg: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        httpx.post(BASE, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")


def alert_start() -> None:
    _send("BTC Scraper démarré")


def alert_stop(reason: str = "") -> None:
    _send(f"BTC Scraper arrêté — {reason}")


def alert_error(error: str) -> None:
    _send(f"BTC Scraper ERREUR CRITIQUE:\n{error[:400]}")


def alert_info(msg: str) -> None:
    _send(f"BTC Scraper: {msg}")
