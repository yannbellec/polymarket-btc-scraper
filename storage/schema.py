"""
Schéma DuckDB complet — 5 tables.
Appelé une seule fois au démarrage du scraper.
"""
import duckdb
from monitoring.logger import log

DB_PATH = "data/btc_scraper.duckdb"


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    log.info("Initialisation du schéma DuckDB...")

    # ── 1. btc_markets ─────────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS btc_markets (
            -- Identifiants
            market_id               TEXT PRIMARY KEY,
            condition_id            TEXT,
            token_id_yes            TEXT,
            token_id_no             TEXT,

            -- Caractéristiques du contrat
            question                TEXT,
            window_ts               BIGINT,
            expiry_ts_ms            BIGINT,
            expiry_iso              TEXT,
            window_minutes          INTEGER,
            open_ts_ms              BIGINT,

            -- Volume et liquidité à l'ouverture
            initial_volume          DOUBLE,
            initial_liquidity       DOUBLE,
            initial_price_yes       DOUBLE,
            initial_price_no        DOUBLE,
            maker_fee               DOUBLE,
            taker_fee               DOUBLE,
            min_order_size          DOUBLE,
            min_tick_size           DOUBLE,

            -- Dérivés calculés à la découverte
            btc_spot_at_open        DOUBLE,
            price_to_beat           DOUBLE,
            moneyness_at_open       DOUBLE,
            moneyness_pct_at_open   DOUBLE,
            seconds_to_expiry_at_open INTEGER,

            -- Statut final (mis à jour post-expiry)
            resolved                BOOLEAN DEFAULT FALSE,
            winning_outcome         TEXT,
            final_btc_price         DOUBLE,
            closed_ts_ms            BIGINT
        )
    """)

    # ── 2. orderbook_ticks ─────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_ticks (
            -- Clés
            tick_id                 BIGINT,
            market_id               TEXT,
            captured_ts_ms          BIGINT,
            time_to_expiry_ms       BIGINT,

            -- Order book YES
            yes_best_bid            DOUBLE,
            yes_best_ask            DOUBLE,
            yes_bid_size            DOUBLE,
            yes_ask_size            DOUBLE,
            yes_total_bid_liq       DOUBLE,
            yes_total_ask_liq       DOUBLE,
            yes_book_depth          INTEGER,

            -- Order book NO
            no_best_bid             DOUBLE,
            no_best_ask             DOUBLE,
            no_bid_size             DOUBLE,
            no_ask_size             DOUBLE,
            no_total_bid_liq        DOUBLE,
            no_total_ask_liq        DOUBLE,

            -- Dérivés calculés au tick
            yes_mid                 DOUBLE,
            yes_spread              DOUBLE,
            yes_spread_pct          DOUBLE,
            book_imbalance          DOUBLE,
            yes_price_delta_1s      DOUBLE,
            yes_price_delta_10s     DOUBLE,
            volume_since_open       DOUBLE,
            trade_count_since_open  INTEGER,
            btc_spot                DOUBLE,
            moneyness               DOUBLE
        )
    """)

    # ── 3. trades ──────────────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            -- Identifiants
            trade_id                TEXT PRIMARY KEY,
            market_id               TEXT,
            token_id                TEXT,
            outcome                 TEXT,

            -- Données du trade
            price                   DOUBLE,
            size                    DOUBLE,
            side                    TEXT,
            trade_ts_ms             BIGINT,
            fee_rate_bps            DOUBLE,
            trade_type              TEXT,

            -- Dérivés calculés au trade
            time_to_expiry_at_trade_ms BIGINT,
            btc_spot_at_trade       DOUBLE,
            moneyness_at_trade      DOUBLE,
            slippage_vs_mid         DOUBLE
        )
    """)

    # ── 4. btc_spot_ticks ──────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS btc_spot_ticks (
            ts_ms               BIGINT PRIMARY KEY,
            price               DOUBLE,
            bid                 DOUBLE,
            ask                 DOUBLE,
            volume_24h          DOUBLE,
            binance_trade_id    BIGINT,
            qty                 DOUBLE,
            buyer_maker         BOOLEAN,

            -- Dérivés
            price_delta_1s      DOUBLE,
            price_delta_30s     DOUBLE,
            volatility_1min     DOUBLE
        )
    """)

    # ── 5. market_snapshots ────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            -- Résumé de vie du marché
            market_id               TEXT PRIMARY KEY,
            total_duration_sec      INTEGER,
            total_ticks             INTEGER,
            total_trades            INTEGER,
            total_volume_usdc       DOUBLE,
            winning_outcome         TEXT,

            -- Trajectoire du prix YES
            open_price_yes          DOUBLE,
            close_price_yes         DOUBLE,
            min_price_yes           DOUBLE,
            max_price_yes           DOUBLE,
            price_std_yes           DOUBLE,
            price_at_1min           DOUBLE,
            price_at_30s            DOUBLE,

            -- Contexte BTC sur la fenêtre
            btc_open                DOUBLE,
            btc_close               DOUBLE,
            btc_move_pct            DOUBLE,
            btc_volatility          DOUBLE,
            final_moneyness         DOUBLE,

            -- Timestamp du snapshot
            snapshot_ts_ms          BIGINT
        )
    """)

    log.info("Schéma DuckDB initialisé — 5 tables prêtes")


def get_connection() -> duckdb.DuckDBPyConnection:
    import os
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect(DB_PATH)
    init_schema(con)
    return con
