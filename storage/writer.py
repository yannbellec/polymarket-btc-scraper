"""
Buffer mémoire thread-safe → DuckDB → Parquet.
Flush automatique toutes les FLUSH_INTERVAL_SEC secondes.
"""
import asyncio
import time
import os
from collections import deque
from datetime import datetime, timezone
from typing import Any
import duckdb

from monitoring.logger import log
from config.settings import FLUSH_INTERVAL_SEC
from storage.schema import get_connection

# ── Compteur auto-incrémentiel pour tick_id ───────────────────────────────────
_tick_counter = 0

# ── Buffers en mémoire (deque non-borné, vidé à chaque flush) ─────────────────
_buffers: dict[str, list[dict]] = {
    "btc_markets":      [],
    "orderbook_ticks":  [],
    "trades":           [],
    "btc_spot_ticks":   [],
    "market_snapshots": [],
}

_lock = asyncio.Lock()
_con: duckdb.DuckDBPyConnection | None = None


def _get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        _con = get_connection()
    return _con


def next_tick_id() -> int:
    global _tick_counter
    _tick_counter += 1
    return _tick_counter


async def push(table: str, row: dict) -> None:
    """Ajoute une ligne au buffer en mémoire (non-bloquant)."""
    async with _lock:
        _buffers[table].append(row)


async def flush_all() -> int:
    """
    Vide tous les buffers vers DuckDB.
    Retourne le nombre total de lignes écrites.
    """
    async with _lock:
        total = 0
        con = _get_con()

        for table, rows in _buffers.items():
            if not rows:
                continue
            try:
                _insert_rows(con, table, rows)
                total += len(rows)
                log.debug(f"Flush {table}: {len(rows)} lignes")
                rows.clear()
            except Exception as e:
                log.error(f"Flush error [{table}]: {e}")

        return total


def _insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Insert batch via duckdb (rapide, évite les INSERT 1 par 1)."""
    import pandas as pd
    df = pd.DataFrame(rows)
    # DuckDB peut insérer depuis un DataFrame directement
    con.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM df")


async def export_parquet(date_str: str | None = None) -> list[str]:
    """
    Exporte chaque table en fichier Parquet partitionné par date.
    Retourne la liste des chemins créés.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    os.makedirs(f"data/parquet/{date_str}", exist_ok=True)
    paths = []
    con = _get_con()

    for table in _buffers.keys():
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if count == 0:
                continue
            path = f"data/parquet/{date_str}/{table}.parquet"
            con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            paths.append(path)
            log.info(f"Parquet exporté: {path} ({count} lignes)")
        except Exception as e:
            log.error(f"Export Parquet error [{table}]: {e}")

    return paths


async def flush_loop() -> None:
    """Boucle de flush automatique — tourne en arrière-plan."""
    log.info(f"Writer flush loop démarré (toutes les {FLUSH_INTERVAL_SEC}s)")
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SEC)
        try:
            n = await flush_all()
            if n > 0:
                log.info(f"Flush automatique: {n} lignes écrites en DuckDB")
        except Exception as e:
            log.error(f"Flush loop error: {e}")


def get_buffer_stats() -> dict:
    """Retourne le nombre de lignes en attente dans chaque buffer."""
    return {table: len(rows) for table, rows in _buffers.items()}
