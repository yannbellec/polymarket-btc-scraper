"""
Upload des fichiers Parquet vers Cloudflare R2.
R2 est compatible S3 — on utilise boto3.
"""
import asyncio
import os
from datetime import datetime, timezone

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
    La clé R2 conserve la structure de dossiers : data/parquet/YYYY-MM-DD/table.parquet
    """
    client = _get_client()
    if client is None:
        return False

    # Clé R2 : retire le préfixe "data/" pour garder parquet/date/table.parquet
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


async def upload_all_parquet(date_str: str | None = None) -> int:
    """Upload tous les fichiers Parquet du jour vers R2."""
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    parquet_dir = f"data/parquet/{date_str}"
    if not os.path.exists(parquet_dir):
        return 0

    files = [
        os.path.join(parquet_dir, f)
        for f in os.listdir(parquet_dir)
        if f.endswith(".parquet")
    ]

    uploaded = 0
    for path in files:
        if await upload_file(path):
            uploaded += 1

    if uploaded:
        log.info(f"R2 sync: {uploaded}/{len(files)} fichiers uploadés")

    return uploaded


async def upload_loop() -> None:
    """Boucle d'upload automatique toutes les UPLOAD_INTERVAL_SEC secondes."""
    log.info(f"R2 upload loop démarré (toutes les {UPLOAD_INTERVAL_SEC}s)")
    while True:
        await asyncio.sleep(UPLOAD_INTERVAL_SEC)
        try:
            from storage.writer import export_parquet
            paths = await export_parquet()
            for path in paths:
                await upload_file(path)
        except Exception as e:
            log.error(f"Upload loop error: {e}")
