"""
Upload incrémental vers Cloudflare R2.
Chaque fichier Parquet est uploadé avec sa clé complète.
Structure R2 : parquet/{table}/{date}/{hhmmss}.parquet
Jamais d'écrasement — append-only.
"""
import asyncio
import os
import time

import boto3
from botocore.exceptions import ClientError

from config.settings import (
    R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY,
    R2_ENDPOINT_URL,
    R2_BUCKET_NAME,
    R2_ACCOUNT_ID,
    UPLOAD_INTERVAL_SEC,
    FLUSH_INTERVAL_SEC,
)
from monitoring.logger import log

_s3_client = None

# Observabilité ops (dernier cycle upload_loop)
_last_cycle_wall_ts: float = 0.0
_last_flush_rows: int = 0
_last_export_file_count: int = 0
_last_upload_ok: int = 0
_last_upload_fail: int = 0
_r2_disabled_logged: bool = False


def r2_credentials_present() -> bool:
    return bool(R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_BUCKET_NAME)


def get_r2_ops_snapshot() -> dict:
    """Instantané pour logs status (persist R2 + dernier cycle)."""
    return {
        "r2_credentials_present": r2_credentials_present(),
        "last_cycle_wall_ts": _last_cycle_wall_ts,
        "last_flush_rows": _last_flush_rows,
        "last_export_files": _last_export_file_count,
        "last_upload_ok": _last_upload_ok,
        "last_upload_fail": _last_upload_fail,
        "upload_interval_sec": UPLOAD_INTERVAL_SEC,
    }


def probe_r2_bucket() -> tuple[bool, str]:
    """
    Vérifie clés + endpoint + accès bucket (HeadBucket).
    À appeler une fois au démarrage ; synchrone, court.
    """
    if not r2_credentials_present():
        return False, "variables R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET_NAME incomplètes"
    if not R2_ENDPOINT_URL or not str(R2_ENDPOINT_URL).startswith("http"):
        return False, f"R2_ENDPOINT_URL invalide: {R2_ENDPOINT_URL!r}"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
        )
        client.head_bucket(Bucket=R2_BUCKET_NAME)
        return True, f"bucket {R2_BUCKET_NAME!r} joignable"
    except ClientError as e:
        code = (e.response or {}).get("Error", {}).get("Code", "")
        return False, f"ClientError [{code}]: {e}"
    except Exception as e:
        return False, str(e)


def log_ops_startup_report() -> None:
    """
    Rapport infra : intervalles, R2, continuité de processus.
    Sans flush/export régulier et sans upload, R2 ne reçoit pas les Parquet.
    """
    log.info("--- Infra / persistance ---")
    log.info(
        f"  FLUSH_INTERVAL_SEC={FLUSH_INTERVAL_SEC} (RAM → DuckDB) | "
        f"UPLOAD_INTERVAL_SEC={UPLOAD_INTERVAL_SEC} (export Parquet + R2)"
    )
    if UPLOAD_INTERVAL_SEC > 900:
        log.warning(
            f"  UPLOAD_INTERVAL_SEC={UPLOAD_INTERVAL_SEC}s est élevé — "
            "données visibles sur R2 avec forte latence ; 300s est un bon défaut"
        )
    if UPLOAD_INTERVAL_SEC < FLUSH_INTERVAL_SEC:
        log.warning(
            f"  UPLOAD_INTERVAL_SEC ({UPLOAD_INTERVAL_SEC}) < FLUSH_INTERVAL_SEC ({FLUSH_INTERVAL_SEC}) : "
            "chaque cycle upload refait quand même un flush avant export"
        )
    if not R2_ACCOUNT_ID:
        log.warning(
            "  R2_ACCOUNT_ID vide — vérifiez R2_ENDPOINT_URL explicite dans .env "
            "(ex. https://<accountid>.r2.cloudflarestorage.com)"
        )

    if r2_credentials_present():
        ok, msg = probe_r2_bucket()
        if ok:
            log.info(f"  R2: OK — {msg}")
        else:
            log.error(
                f"  R2: échec sonde bucket — {msg} | "
                "Les Parquet restent en local (data/parquet/) tant que l’upload échoue"
            )
    else:
        log.error(
            "  R2: désactivé (identifiants manquants) — "
            "aucun upload ; configurez R2_* dans l’environnement pour la copie cloud"
        )

    log.info(
        "  Processus : doit tourner en continu (systemd/PM2/Docker). "
        "Si le scraper s’arrête, RTDS + WS Polymarket + flush/export s’arrêtent avec lui."
    )
    log.info("---")


def _get_client():
    global _s3_client
    if _s3_client is None:
        if not r2_credentials_present():
            log.warning("R2 credentials manquants — uploads désactivés")
            return None
        _s3_client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
        )
    return _s3_client


async def upload_file(local_path: str) -> bool:
    """
    Upload un fichier Parquet vers R2.
    Clé R2 = chemin local sans le préfixe 'data/'
    ex: data/parquet/orderbook_ticks/2026-03-22/143500.parquet
     → parquet/orderbook_ticks/2026-03-22/143500.parquet
    """
    global _r2_disabled_logged
    client = _get_client()
    if client is None:
        if not _r2_disabled_logged:
            log.warning(
                "R2: upload ignoré (pas de client S3) — fichiers Parquet uniquement en local"
            )
            _r2_disabled_logged = True
        return False

    r2_key = local_path.replace("data/", "", 1)

    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: client.upload_file(local_path, R2_BUCKET_NAME, r2_key)
        )
        log.info(f"R2 upload OK: {r2_key}")
        return True
    except ClientError as e:
        log.error(f"R2 upload erreur [{local_path}]: {e}")
        return False


async def upload_loop() -> None:
    """
    Toutes les UPLOAD_INTERVAL_SEC secondes :
    1. Flush les buffers vers DuckDB
    2. Export incrémental vers Parquet horodaté
    3. Upload chaque nouveau fichier vers R2
    """
    global _last_cycle_wall_ts, _last_flush_rows, _last_export_file_count
    global _last_upload_ok, _last_upload_fail

    from storage.writer import flush_all, export_incremental

    log.info(
        f"R2 upload loop démarré (toutes les {UPLOAD_INTERVAL_SEC}s) — "
        f"même rythme que l’export Parquet vers data/ puis copie R2"
    )

    while True:
        await asyncio.sleep(UPLOAD_INTERVAL_SEC)
        try:
            _last_cycle_wall_ts = time.time()

            n = await flush_all()
            _last_flush_rows = n
            if n > 0:
                log.info(f"Pre-upload flush: {n} lignes")

            paths = await export_incremental()
            _last_export_file_count = len(paths)

            uploaded = 0
            failed = 0
            for path in paths:
                if await upload_file(path):
                    uploaded += 1
                else:
                    failed += 1
            _last_upload_ok = uploaded
            _last_upload_fail = failed

            log.info(
                f"R2 cycle — flush={n} lignes | parquet={len(paths)} fichiers | "
                f"upload OK={uploaded} échecs={failed}"
            )

        except Exception as e:
            log.error(f"Upload loop error: {e}")