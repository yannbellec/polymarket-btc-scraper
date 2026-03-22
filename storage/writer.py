"""
Buffer mémoire → DuckDB → export incrémental Parquet.
- DuckDB = buffer de travail depuis le dernier restart
- Export incrémental = seulement les nouvelles lignes depuis le dernier export
- Parquet horodaté = jamais écrasé, append-only sur R2
"""
import asyncio
import os
import time
from datetime import datetime, timezone

import duckdb
import pandas as pd

from monitoring.logger import log
from config.settings import FLUSH_INTERVAL_SEC
from storage.schema import get_connection, DB_PATH

# ── Tables et colonnes de timestamp pour l'export incrémental ────────────────
TABLES = ["btc_markets", "orderbook_ticks", "trades", "btc_spot_ticks", "market_snapshots"]

TIMESTAMP_COLS = {
    "btc_markets":      "open_ts_ms",
    "orderbook_ticks":  "captured_ts_ms",
    "trades":           "trade_ts_ms",
    "btc_spot_ticks":   "ts_ms",
    "market_snapshots": "snapshot_ts_ms",
}

# ── Buffers mémoire ───────────────────────────────────────────────────────────
_buffers: dict[str, list[dict]] = {t: [] for t in TABLES}
_lock = asyncio.Lock()
_con: duckdb.DuckDBPyConnection | None = None

# ── Watermarks — dernière valeur de timestamp exportée par table ──────────────
_watermarks: dict[str, int] = {t: 0 for t in TABLES}

# ── Compteur tick_id ──────────────────────────────────────────────────────────
_tick_counter = 0


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
    async with _lock:
        _buffers[table].append(row)


async def flush_all() -> int:
    """Vide les buffers vers DuckDB. Retourne le nombre de lignes écrites."""
    async with _lock:
        total = 0
        con   = _get_con()
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
    df       = pd.DataFrame(rows)
    pk_tables = {"btc_markets", "trades", "btc_spot_ticks", "market_snapshots"}

    # Réordonne les colonnes selon le schéma DuckDB
    try:
        schema_cols = [c[0] for c in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{table}' ORDER BY ordinal_position"
        ).fetchall()]
        cols = [c for c in schema_cols if c in df.columns]
        df   = df[cols]
    except Exception:
        pass

    if table in pk_tables:
        try:
            con.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM df")
        except Exception:
            for _, row in df.iterrows():
                try:
                    rdf = pd.DataFrame([row])
                    con.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM rdf")
                except Exception:
                    pass
    else:
        con.execute(f"INSERT INTO {table} SELECT * FROM df")


async def export_incremental() -> list[str]:
    """
    Exporte UNIQUEMENT les nouvelles lignes depuis le dernier export.
    Chaque appel crée un nouveau fichier Parquet horodaté.
    Ne modifie jamais les fichiers existants.
    """
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%H%M%S")
    con      = _get_con()
    paths    = []

    for table in TABLES:
        ts_col    = TIMESTAMP_COLS[table]
        watermark = _watermarks[table]

        try:
            # Seulement les nouvelles lignes
            count = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {ts_col} > {watermark}"
            ).fetchone()[0]

            if count == 0:
                continue

            folder = f"data/parquet/{table}/{date_str}"
            os.makedirs(folder, exist_ok=True)
            path = f"{folder}/{ts_str}.parquet"

            con.execute(f"""
                COPY (
                    SELECT * FROM {table}
                    WHERE {ts_col} > {watermark}
                    ORDER BY {ts_col}
                ) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)

            # Met à jour le watermark
            new_watermark = con.execute(
                f"SELECT MAX({ts_col}) FROM {table} WHERE {ts_col} > {watermark}"
            ).fetchone()[0]

            if new_watermark:
                _watermarks[table] = int(new_watermark)

            paths.append(path)
            log.info(f"Parquet exporté: {table}/{date_str}/{ts_str}.parquet ({count} lignes)")

        except Exception as e:
            log.error(f"Export incrémental [{table}]: {e}")

    return paths


async def flush_loop() -> None:
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
    return {table: len(rows) for table, rows in _buffers.items()}