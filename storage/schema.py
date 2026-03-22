"""
Schema DuckDB complet — 6 tables.
btc_spot_ticks         = Chainlink uniquement (reference officielle Polymarket)
btc_spot_ticks_binance = Binance uniquement (bonus, source secondaire)
"""
import duckdb
from monitoring.logger import log

DB_PATH = "data/btc_scraper.duckdb"


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    log.info("Initialisation du schema DuckDB...")

    con.execute("""
        CREATE TABLE IF NOT EXISTS btc_markets (
            market_id               TEXT PRIMARY KEY,
            condition_id            TEXT,
            token_id_yes            TEXT,
            token_id_no             TEXT,
            question                TEXT,
            window_ts               BIGINT,
            expiry_ts_ms            BIGINT,
            expiry_iso              TEXT,
            window_minutes          INTEGER,
            open_ts_ms              BIGINT,
            initial_volume          DOUBLE,
            initial_liquidity       DOUBLE,
            initial_price_yes       DOUBLE,
            initial_price_no        DOUBLE,
            maker_fee               DOUBLE,
            taker_fee               DOUBLE,
            min_order_size          DOUBLE,
            min_tick_size           DOUBLE,
            btc_spot_at_open        DOUBLE,
            price_to_beat           DOUBLE,
            moneyness_at_open       DOUBLE,
            moneyness_pct_at_open   DOUBLE,
            seconds_to_expiry_at_open INTEGER,
            resolved                BOOLEAN DEFAULT FALSE,
            winning_outcome         TEXT,
            final_btc_price         DOUBLE,
            closed_ts_ms            BIGINT,
            prev1_market_id         TEXT,
            prev1_outcome           TEXT,
            prev1_btc_move_pct      DOUBLE,
            prev1_close_price_yes   DOUBLE,
            prev2_market_id         TEXT,
            prev2_outcome           TEXT,
            prev2_btc_move_pct      DOUBLE,
            prev2_close_price_yes   DOUBLE,
            prev3_market_id         TEXT,
            prev3_outcome           TEXT,
            prev3_btc_move_pct      DOUBLE,
            prev3_close_price_yes   DOUBLE,
            prev_no_streak                  INTEGER,
            prev_btc_down_streak            INTEGER,
            prev_no_count                   INTEGER,
            prev_btc_down_count             INTEGER,
            prev_signal_all3_no_btc_down    DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_ticks (
            tick_id                 BIGINT,
            market_id               TEXT,
            captured_ts_ms          BIGINT,
            time_to_expiry_ms       BIGINT,
            yes_best_bid            DOUBLE,
            yes_best_ask            DOUBLE,
            yes_bid_size            DOUBLE,
            yes_ask_size            DOUBLE,
            yes_total_bid_liq       DOUBLE,
            yes_total_ask_liq       DOUBLE,
            yes_book_depth          INTEGER,
            no_best_bid             DOUBLE,
            no_best_ask             DOUBLE,
            no_bid_size             DOUBLE,
            no_ask_size             DOUBLE,
            no_total_bid_liq        DOUBLE,
            no_total_ask_liq        DOUBLE,
            yes_mid                 DOUBLE,
            yes_spread              DOUBLE,
            yes_spread_pct          DOUBLE,
            book_imbalance          DOUBLE,
            yes_price_delta_tick    DOUBLE,
            yes_price_delta_10s     DOUBLE,
            volume_since_open       DOUBLE,
            trade_count_since_open  INTEGER,
            btc_spot                DOUBLE,
            moneyness               DOUBLE,
            yes_spread_ma_60        DOUBLE,
            yes_liq_ma_60           DOUBLE,
            btc_price_at_open       DOUBLE,
            btc_delta_1min          DOUBLE,
            btc_delta_2min          DOUBLE,
            btc_delta_3min          DOUBLE,
            btc_vol_1m              DOUBLE,
            btc_vol_2m              DOUBLE,
            btc_vol_5m              DOUBLE,
            btc_vol_since_open      DOUBLE,
            prev1_market_id         TEXT,
            prev1_outcome           TEXT,
            prev1_btc_move_pct      DOUBLE,
            prev1_close_price_yes   DOUBLE,
            prev2_market_id         TEXT,
            prev2_outcome           TEXT,
            prev2_btc_move_pct      DOUBLE,
            prev2_close_price_yes   DOUBLE,
            prev3_market_id         TEXT,
            prev3_outcome           TEXT,
            prev3_btc_move_pct      DOUBLE,
            prev3_close_price_yes   DOUBLE,
            prev_no_streak                  INTEGER,
            prev_btc_down_streak            INTEGER,
            prev_no_count                   INTEGER,
            prev_btc_down_count             INTEGER,
            prev_signal_all3_no_btc_down    DOUBLE,
            ofi_since_open          DOUBLE,
            ofi_last_60s            DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id                    TEXT PRIMARY KEY,
            market_id                   TEXT,
            token_id                    TEXT,
            outcome                     TEXT,
            price                       DOUBLE,
            size                        DOUBLE,
            side                        TEXT,
            trade_ts_ms                 BIGINT,
            fee_rate_bps                DOUBLE,
            trade_type                  TEXT,
            time_to_expiry_at_trade_ms  BIGINT,
            btc_spot_at_trade           DOUBLE,
            moneyness_at_trade          DOUBLE,
            slippage_vs_mid             DOUBLE,
            yes_best_bid_at_trade       DOUBLE,
            yes_best_ask_at_trade       DOUBLE,
            btc_price_at_open           DOUBLE,
            btc_delta_1min              DOUBLE,
            btc_delta_2min              DOUBLE,
            btc_delta_3min              DOUBLE,
            btc_vol_1m                  DOUBLE,
            btc_vol_2m                  DOUBLE,
            btc_vol_5m                  DOUBLE,
            btc_vol_since_open          DOUBLE,
            prev1_market_id             TEXT,
            prev1_outcome               TEXT,
            prev1_btc_move_pct          DOUBLE,
            prev1_close_price_yes       DOUBLE,
            prev2_market_id             TEXT,
            prev2_outcome               TEXT,
            prev2_btc_move_pct          DOUBLE,
            prev2_close_price_yes       DOUBLE,
            prev3_market_id             TEXT,
            prev3_outcome               TEXT,
            prev3_btc_move_pct          DOUBLE,
            prev3_close_price_yes       DOUBLE,
            prev_no_streak                  INTEGER,
            prev_btc_down_streak            INTEGER,
            prev_no_count                   INTEGER,
            prev_btc_down_count             INTEGER,
            prev_signal_all3_no_btc_down    DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS btc_spot_ticks (
            ts_ms               BIGINT PRIMARY KEY,
            price               DOUBLE,
            price_delta_1s      DOUBLE,
            price_delta_30s     DOUBLE,
            volatility_1min     DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS btc_spot_ticks_binance (
            ts_ms               BIGINT PRIMARY KEY,
            price               DOUBLE,
            bid                 DOUBLE,
            ask                 DOUBLE,
            volume_24h          DOUBLE,
            binance_trade_id    BIGINT,
            qty                 DOUBLE,
            buyer_maker         BOOLEAN,
            price_delta_1s      DOUBLE,
            price_delta_30s     DOUBLE,
            volatility_1min     DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            market_id               TEXT PRIMARY KEY,
            total_duration_sec      INTEGER,
            total_ticks             INTEGER,
            total_trades            INTEGER,
            total_volume_usdc       DOUBLE,
            winning_outcome         TEXT,
            open_price_yes          DOUBLE,
            close_price_yes         DOUBLE,
            min_price_yes           DOUBLE,
            max_price_yes           DOUBLE,
            price_std_yes           DOUBLE,
            price_at_1min           DOUBLE,
            price_at_30s            DOUBLE,
            btc_open                DOUBLE,
            btc_close               DOUBLE,
            btc_move_pct            DOUBLE,
            btc_volatility          DOUBLE,
            final_moneyness         DOUBLE,
            snapshot_ts_ms          BIGINT
        )
    """)

    # Migrations idempotentes pour DuckDB existant
    _migrations = [
        ("orderbook_ticks", "yes_price_delta_1s",    None,     "yes_price_delta_tick"),
        ("orderbook_ticks", "yes_spread_ma_60",       "DOUBLE", None),
        ("orderbook_ticks", "yes_liq_ma_60",          "DOUBLE", None),
        ("orderbook_ticks", "btc_price_at_open",      "DOUBLE", None),
        ("orderbook_ticks", "btc_delta_1min",         "DOUBLE", None),
        ("orderbook_ticks", "btc_delta_2min",         "DOUBLE", None),
        ("orderbook_ticks", "btc_delta_3min",         "DOUBLE", None),
        ("orderbook_ticks", "prev1_market_id",        "TEXT",   None),
        ("orderbook_ticks", "prev1_outcome",          "TEXT",   None),
        ("orderbook_ticks", "prev1_btc_move_pct",     "DOUBLE", None),
        ("orderbook_ticks", "prev1_close_price_yes",  "DOUBLE", None),
        ("orderbook_ticks", "prev2_market_id",        "TEXT",   None),
        ("orderbook_ticks", "prev2_outcome",          "TEXT",   None),
        ("orderbook_ticks", "prev2_btc_move_pct",     "DOUBLE", None),
        ("orderbook_ticks", "prev2_close_price_yes",  "DOUBLE", None),
        ("orderbook_ticks", "prev3_market_id",        "TEXT",   None),
        ("orderbook_ticks", "prev3_outcome",          "TEXT",   None),
        ("orderbook_ticks", "prev3_btc_move_pct",     "DOUBLE", None),
        ("orderbook_ticks", "prev3_close_price_yes",  "DOUBLE", None),
        ("orderbook_ticks", "ofi_since_open",         "DOUBLE", None),
        ("orderbook_ticks", "ofi_last_60s",           "DOUBLE", None),
        ("orderbook_ticks", "btc_vol_1m",             "DOUBLE", None),
        ("orderbook_ticks", "btc_vol_2m",             "DOUBLE", None),
        ("orderbook_ticks", "btc_vol_5m",             "DOUBLE", None),
        ("orderbook_ticks", "btc_vol_since_open",     "DOUBLE", None),
        ("trades",          "yes_best_bid_at_trade",  "DOUBLE", None),
        ("trades",          "yes_best_ask_at_trade",  "DOUBLE", None),
        ("trades",          "btc_price_at_open",      "DOUBLE", None),
        ("trades",          "btc_delta_1min",         "DOUBLE", None),
        ("trades",          "btc_delta_2min",         "DOUBLE", None),
        ("trades",          "btc_delta_3min",         "DOUBLE", None),
        ("trades",          "btc_vol_1m",             "DOUBLE", None),
        ("trades",          "btc_vol_2m",             "DOUBLE", None),
        ("trades",          "btc_vol_5m",             "DOUBLE", None),
        ("trades",          "btc_vol_since_open",     "DOUBLE", None),
        ("trades",          "prev1_market_id",        "TEXT",   None),
        ("trades",          "prev1_outcome",          "TEXT",   None),
        ("trades",          "prev1_btc_move_pct",     "DOUBLE", None),
        ("trades",          "prev1_close_price_yes",  "DOUBLE", None),
        ("trades",          "prev2_market_id",        "TEXT",   None),
        ("trades",          "prev2_outcome",          "TEXT",   None),
        ("trades",          "prev2_btc_move_pct",     "DOUBLE", None),
        ("trades",          "prev2_close_price_yes",  "DOUBLE", None),
        ("trades",          "prev3_market_id",        "TEXT",   None),
        ("trades",          "prev3_outcome",          "TEXT",   None),
        ("trades",          "prev3_btc_move_pct",     "DOUBLE", None),
        ("trades",          "prev3_close_price_yes",  "DOUBLE", None),
        ("btc_markets",     "prev1_market_id",        "TEXT",   None),
        ("btc_markets",     "prev1_outcome",          "TEXT",   None),
        ("btc_markets",     "prev1_btc_move_pct",     "DOUBLE", None),
        ("btc_markets",     "prev1_close_price_yes",  "DOUBLE", None),
        ("btc_markets",     "prev2_market_id",        "TEXT",   None),
        ("btc_markets",     "prev2_outcome",          "TEXT",   None),
        ("btc_markets",     "prev2_btc_move_pct",     "DOUBLE", None),
        ("btc_markets",     "prev2_close_price_yes",  "DOUBLE", None),
        ("btc_markets",     "prev3_market_id",        "TEXT",   None),
        ("btc_markets",     "prev3_outcome",          "TEXT",   None),
        ("btc_markets",     "prev3_btc_move_pct",     "DOUBLE", None),
        ("btc_markets",     "prev3_close_price_yes",  "DOUBLE", None),
        ("btc_markets",     "prev_no_streak",                  "INTEGER", None),
        ("btc_markets",     "prev_btc_down_streak",            "INTEGER", None),
        ("btc_markets",     "prev_no_count",                   "INTEGER", None),
        ("btc_markets",     "prev_btc_down_count",             "INTEGER", None),
        ("btc_markets",     "prev_signal_all3_no_btc_down",    "DOUBLE", None),
        ("orderbook_ticks", "prev_no_streak",                  "INTEGER", None),
        ("orderbook_ticks", "prev_btc_down_streak",            "INTEGER", None),
        ("orderbook_ticks", "prev_no_count",                   "INTEGER", None),
        ("orderbook_ticks", "prev_btc_down_count",             "INTEGER", None),
        ("orderbook_ticks", "prev_signal_all3_no_btc_down",    "DOUBLE", None),
        ("trades",          "prev_no_streak",                  "INTEGER", None),
        ("trades",          "prev_btc_down_streak",            "INTEGER", None),
        ("trades",          "prev_no_count",                   "INTEGER", None),
        ("trades",          "prev_btc_down_count",             "INTEGER", None),
        ("trades",          "prev_signal_all3_no_btc_down",    "DOUBLE", None),
    ]

    for table, col, typ, rename_to in _migrations:
        if rename_to:
            try:
                con.execute(f"ALTER TABLE {table} RENAME COLUMN {col} TO {rename_to}")
            except Exception:
                pass
        else:
            try:
                con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
            except Exception:
                pass

    try:
        from scraper.inter_market_context import bootstrap_from_duckdb

        bootstrap_from_duckdb(con)
    except Exception as e:
        log.warning(f"inter_market_context bootstrap: {e}")

    log.info("Schema DuckDB initialise — 6 tables pretes")


def get_connection() -> duckdb.DuckDBPyConnection:
    import os
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect(DB_PATH)
    init_schema(con)
    return con