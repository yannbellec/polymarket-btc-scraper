"""
Upload incrémental vers Cloudflare R2.
Chaque fichier Parquet est uploadé avec sa clé complète.
Structure R2 : parquet/{table}/{date}/{hhmmss}.parquet
Jamais d'écrasement — append-only.
"""
import asyncio
import os

import boto3
from botocore.exceptions import ClientError

from config.settings import (
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
    R2_ENDPOINT_URL, R2_BUCKET_NAME, UPLOAD_INTERVAL_SEC,
)
from monitoring.logger import log

_s3_client = None


def _get_client():
    global _s3_client
    if _s3_client is None:
        if not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
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
    client = _get_client()
    if client is None:
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
    from storage.writer import flush_all, export_incremental

    log.info(f"R2 upload loop démarré (toutes les {UPLOAD_INTERVAL_SEC}s)")

    while True:
        await asyncio.sleep(UPLOAD_INTERVAL_SEC)
        try:
            # Flush d'abord
            n = await flush_all()
            if n > 0:
                log.info(f"Pre-upload flush: {n} lignes")

            # Export incrémental
            paths = await export_incremental()

            # Upload chaque nouveau fichier
            uploaded = 0
            for path in paths:
                if await upload_file(path):
                    uploaded += 1

            if uploaded:
                log.info(f"R2 sync: {uploaded}/{len(paths)} fichiers uploadés")

        except Exception as e:
            log.error(f"Upload loop error: {e}")