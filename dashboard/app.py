"""
Dashboard Streamlit — inspection des données R2 du scraper BTC Polymarket.
Lecture seule depuis R2, aucun impact sur le scraper.
"""
import os
import io
from datetime import datetime, timezone, timedelta

import boto3
import duckdb
import pandas as pd
import streamlit as st

# ─── Config page ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BTC Polymarket — Data Inspector",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@400;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'JetBrains Mono', monospace;
    background: #0a0a0f;
    color: #e2e8f0;
}

.stApp { background: #0a0a0f; }

h1, h2, h3 { font-family: 'Syne', sans-serif; color: #f0f4ff; }

.metric-card {
    background: linear-gradient(135deg, #111827 0%, #1a1f2e 100%);
    border: 1px solid #1e293b;
    border-radius: 8px;
    padding: 16px 20px;
    margin: 4px 0;
}

.metric-value {
    font-size: 2rem;
    font-weight: 700;
    color: #38bdf8;
    font-family: 'Syne', sans-serif;
}

.metric-label {
    font-size: 0.7rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

.status-ok { color: #22c55e; }
.status-warn { color: #f59e0b; }
.status-error { color: #ef4444; }

.stTabs [data-baseweb="tab-list"] {
    background: #111827;
    border-bottom: 1px solid #1e293b;
    gap: 0;
}

.stTabs [data-baseweb="tab"] {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    color: #64748b;
    padding: 10px 20px;
    border-bottom: 2px solid transparent;
}

.stTabs [aria-selected="true"] {
    color: #38bdf8;
    border-bottom: 2px solid #38bdf8;
    background: transparent;
}

.stDataFrame { font-size: 0.75rem; }

[data-testid="stSidebar"] {
    background: #0d1117;
    border-right: 1px solid #1e293b;
}

.sidebar-header {
    font-family: 'Syne', sans-serif;
    font-size: 1.2rem;
    font-weight: 800;
    color: #38bdf8;
    margin-bottom: 4px;
}

.sidebar-sub {
    font-size: 0.65rem;
    color: #475569;
    margin-bottom: 20px;
}

div[data-testid="stSelectbox"] label,
div[data-testid="stDateInput"] label {
    font-size: 0.7rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

.corrupt-badge {
    background: #7f1d1d;
    color: #fca5a5;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.65rem;
}

.ok-badge {
    background: #14532d;
    color: #86efac;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.65rem;
}
</style>
""", unsafe_allow_html=True)

# ─── R2 Client ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )

BUCKET = os.getenv("R2_BUCKET_NAME", "polymarket-btc-data")
TABLES = ["btc_markets", "orderbook_ticks", "trades", "btc_spot_ticks", "btc_spot_ticks_binance", "market_snapshots"]

# Tables légères : chargées entièrement sans restriction
LIGHT_TABLES = {"btc_markets", "market_snapshots"}
# Tables lourdes : limitées par défaut à N fichiers parquet + bouton pour tout charger
HEAVY_TABLES = {"orderbook_ticks", "trades", "btc_spot_ticks", "btc_spot_ticks_binance"}
HEAVY_DEFAULT_FILES = 2  # nb de fichiers parquet chargés par défaut pour les tables lourdes

# ─── Helpers ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def list_dates():
    s3 = get_s3()
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="parquet/btc_spot_ticks/", Delimiter="/")
    dates = []
    for p in resp.get("CommonPrefixes", []):
        parts = p["Prefix"].rstrip("/").split("/")
        if len(parts) >= 3:
            dates.append(parts[2])
    return sorted(dates, reverse=True)


def _load_parquet_keys(table: str, date: str) -> list:
    """Retourne la liste triée des clés parquet pour une table/date."""
    s3 = get_s3()
    prefix = f"parquet/{table}/{date}/"
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return sorted([o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")])


def _read_parquet_keys(keys: list) -> pd.DataFrame:
    """Télécharge et concatène une liste de clés parquet depuis R2."""
    s3 = get_s3()
    con = duckdb.connect()
    dfs = []
    for key in keys:
        buf = io.BytesIO()
        s3.download_fileobj(BUCKET, key, buf)
        buf.seek(0)
        tmp = f"/tmp/{key.replace('/', '_')}"
        with open(tmp, "wb") as f:
            f.write(buf.read())
        try:
            df = con.execute(f"SELECT * FROM read_parquet('{tmp}')").df()
            dfs.append(df)
        except Exception:
            pass
    con.close()
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


@st.cache_data(ttl=30)
def load_table(table: str, date: str) -> pd.DataFrame:
    """Charge une table depuis R2.
    - Tables légères (btc_markets, market_snapshots) : tous les fichiers.
    - Tables lourdes : seulement les HEAVY_DEFAULT_FILES premiers fichiers par défaut.
    """
    keys = _load_parquet_keys(table, date)
    if not keys:
        return pd.DataFrame()
    if table in HEAVY_TABLES:
        keys = keys[:HEAVY_DEFAULT_FILES]
    return _read_parquet_keys(keys)


@st.cache_data(ttl=30)
def load_table_full(table: str, date: str) -> pd.DataFrame:
    """Charge tous les fichiers parquet d'une table lourde (sur demande explicite)."""
    keys = _load_parquet_keys(table, date)
    if not keys:
        return pd.DataFrame()
    return _read_parquet_keys(keys)


def _heavy_table_header(table: str, date: str, df: pd.DataFrame, session_key: str) -> pd.DataFrame:
    """Affiche un bandeau preview + bouton 'Charger tout' pour les tables lourdes.
    Retourne le df complet si l'utilisateur clique, sinon df inchangé."""
    keys = _load_parquet_keys(table, date)
    nb_total = len(keys)
    nb_loaded = min(HEAVY_DEFAULT_FILES, nb_total)
    if nb_total > nb_loaded:
        c_info, c_btn = st.columns([4, 1])
        with c_info:
            st.caption(
                f"⚡ Preview : {nb_loaded}/{nb_total} fichier(s) chargé(s). "
                f"Les données affichées couvrent une fraction de la journée."
            )
        with c_btn:
            if st.button("📥 Tout charger", key=session_key):
                with st.spinner(f"Chargement complet de {table}..."):
                    df = load_table_full(table, date)
    return df


@st.cache_data(ttl=30)
def get_r2_file_list(date: str):
    s3 = get_s3()
    # Utilise un prefix filtré + paginator pour éviter de lister tout le bucket
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix="parquet/"):
        for o in page.get("Contents", []):
            if date in o["Key"]:
                files.append({
                    "path": o["Key"],
                    "size_kb": round(o["Size"] / 1024, 1),
                    "modified": o["LastModified"].strftime("%H:%M:%S"),
                })
    return pd.DataFrame(files)

def fmt_ts(ts_ms):
    if pd.isna(ts_ms):
        return ""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")

# ─── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="sidebar-header">⚡ BTC Inspector</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-sub">Polymarket BTC 5-min — Read-only R2</div>', unsafe_allow_html=True)

    dates = list_dates()
    if not dates:
        st.error("Aucune donnée dans R2")
        st.stop()

    selected_date = st.selectbox("Date", dates, index=0)

    st.markdown("---")
    if st.button("🔄 Rafraîchir", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.markdown('<div style="font-size:0.65rem;color:#475569;">Lecture seule — aucun impact sur le scraper</div>', unsafe_allow_html=True)

# ─── Header ─────────────────────────────────────────────────────────────────
st.markdown(f"## Data Inspector &nbsp;·&nbsp; <span style='color:#38bdf8'>{selected_date}</span>", unsafe_allow_html=True)

# ─── Métriques globales ──────────────────────────────────────────────────────
with st.spinner("Chargement des métriques..."):
    metrics = {}
    for t in TABLES:
        df = load_table(t, selected_date)
        metrics[t] = len(df)

col1, col2, col3, col4, col5, col6 = st.columns(6)
cols = [col1, col2, col3, col4, col5, col6]
labels = {
    "btc_markets": "Marchés",
    "orderbook_ticks": "OB Ticks",
    "trades": "Trades",
    "btc_spot_ticks": "BTC (CL)",
    "btc_spot_ticks_binance": "BTC (BN)",
    "market_snapshots": "Snapshots",
}
for i, t in enumerate(TABLES):
    with cols[i]:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{metrics[t]:,}</div>
            <div class="metric-label">{labels[t]}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─── Onglets ────────────────────────────────────────────────────────────────
tabs = st.tabs(["📋 Marchés", "📊 Orderbook", "💸 Trades", "₿ BTC Chainlink", "₿ BTC Binance", "📸 Snapshots", "🗂 Fichiers R2"])

# ── Tab: btc_markets ────────────────────────────────────────────────────────
with tabs[0]:
    df = load_table("btc_markets", selected_date)
    if df.empty:
        st.info("Aucune donnée")
    else:
        # Dédoublonne
        if "market_id" in df.columns:
            df = df.drop_duplicates(subset=["market_id"], keep="last")

        st.markdown(f"**{len(df)} marchés** &nbsp;·&nbsp; colonnes: `{len(df.columns)}`")

        # Formatage colonnes ts
        for col in ["open_ts_ms", "expiry_ts_ms", "closed_ts_ms"]:
            if col in df.columns:
                df[col + "_fmt"] = df[col].apply(fmt_ts)

        # Colonnes clés en premier
        priority = ["market_id", "question", "open_ts_ms_fmt", "expiry_ts_ms_fmt",
                    "initial_price_yes", "btc_spot_at_open", "price_to_beat",
                    "winning_outcome", "resolved",
                    "prev1_outcome", "prev2_outcome", "prev3_outcome",
                    "prev_no_streak", "prev_btc_down_streak"]
        cols_show = [c for c in priority if c in df.columns] + \
                    [c for c in df.columns if c not in priority and not c.endswith("_ts_ms")]
        st.dataframe(df[cols_show], use_container_width=True, height=400)

        st.markdown("**Toutes les colonnes**")
        st.dataframe(df, use_container_width=True, height=300)

# ── Tab: orderbook_ticks ────────────────────────────────────────────────────
with tabs[1]:
    df = load_table("orderbook_ticks", selected_date)
    df = _heavy_table_header("orderbook_ticks", selected_date, df, "load_full_ob")
    if df.empty:
        st.info("Aucune donnée")
    else:
        st.markdown(f"**{len(df):,} ticks** &nbsp;·&nbsp; colonnes: `{len(df.columns)}`")

        # Filtres
        c1, c2, c3 = st.columns(3)
        markets = ["Tous"] + (sorted(df["market_id"].dropna().unique().tolist()) if "market_id" in df.columns else [])
        sel_mkt = c1.selectbox("Marché", markets, key="ob_mkt")
        n_rows = c2.slider("Nb lignes", 100, min(10000, len(df)), 1000, key="ob_rows")

        df_show = df.copy()
        if sel_mkt != "Tous" and "market_id" in df.columns:
            df_show = df_show[df_show["market_id"] == sel_mkt]

        # Formatage ts
        if "captured_ts_ms" in df_show.columns:
            df_show.insert(0, "heure", df_show["captured_ts_ms"].apply(fmt_ts))

        priority = ["heure", "market_id", "yes_mid", "yes_spread", "book_imbalance",
                    "yes_best_bid", "yes_best_ask", "yes_total_bid_liq", "yes_total_ask_liq",
                    "yes_book_depth", "btc_spot", "moneyness", "time_to_expiry_ms",
                    "ofi_since_open", "ofi_last_60s", "yes_spread_ma_60", "yes_liq_ma_60",
                    "volume_since_open", "trade_count_since_open",
                    "btc_delta_1min", "btc_delta_2min", "btc_delta_3min"]
        cols_show = [c for c in priority if c in df_show.columns] + \
                    [c for c in df_show.columns if c not in priority and "ts_ms" not in c]

        st.dataframe(df_show[cols_show].tail(n_rows), use_container_width=True, height=500)

        # Stats rapides
        st.markdown("**Stats**")
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Spread moyen", f"{df['yes_spread'].mean():.4f}" if "yes_spread" in df.columns else "—")
        s2.metric("Imbalance moyen", f"{df['book_imbalance'].mean():.4f}" if "book_imbalance" in df.columns else "—")
        s3.metric("Mid min/max", f"{df['yes_mid'].min():.3f} / {df['yes_mid'].max():.3f}" if "yes_mid" in df.columns else "—")
        s4.metric("Marchés uniques", df["market_id"].nunique() if "market_id" in df.columns else "—")

# ── Tab: trades ─────────────────────────────────────────────────────────────
with tabs[2]:
    df = load_table("trades", selected_date)
    df = _heavy_table_header("trades", selected_date, df, "load_full_trades")
    if df.empty:
        st.info("Aucune donnée")
    else:
        st.markdown(f"**{len(df):,} trades** &nbsp;·&nbsp; colonnes: `{len(df.columns)}`")

        c1, c2, c3 = st.columns(3)
        markets = ["Tous"] + (sorted(df["market_id"].dropna().unique().tolist()) if "market_id" in df.columns else [])
        sel_mkt = c1.selectbox("Marché", markets, key="tr_mkt")
        sel_side = c2.selectbox("Side", ["Tous", "BUY", "SELL"], key="tr_side")
        n_rows = c3.slider("Nb lignes", 100, min(10000, len(df)), 1000, key="tr_rows")

        df_show = df.copy()
        if sel_mkt != "Tous":
            df_show = df_show[df_show["market_id"] == sel_mkt]
        if sel_side != "Tous":
            df_show = df_show[df_show["side"] == sel_side]

        if "trade_ts_ms" in df_show.columns:
            df_show.insert(0, "heure", df_show["trade_ts_ms"].apply(fmt_ts))

        priority = ["heure", "market_id", "outcome", "price", "size", "side",
                    "slippage_vs_mid", "yes_best_bid_at_trade", "yes_best_ask_at_trade",
                    "btc_spot_at_trade", "moneyness_at_trade", "time_to_expiry_at_trade_ms",
                    "btc_delta_1min", "btc_delta_2min", "btc_delta_3min"]
        cols_show = [c for c in priority if c in df_show.columns] + \
                    [c for c in df_show.columns if c not in priority and "ts_ms" not in c]

        st.dataframe(df_show[cols_show].tail(n_rows), use_container_width=True, height=500)

        # Stats
        st.markdown("**Stats**")
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Volume total", f"{df['size'].sum():,.0f} USDC" if "size" in df.columns else "—")
        s2.metric("Taille moyenne", f"{df['size'].mean():.2f}" if "size" in df.columns else "—")
        s3.metric("BUY / SELL", f"{(df['side']=='BUY').sum()} / {(df['side']=='SELL').sum()}" if "side" in df.columns else "—")
        s4.metric("Slip moyen", f"{df['slippage_vs_mid'].mean():.4f}" if "slippage_vs_mid" in df.columns else "—")

# ── Tab: btc_spot_ticks (Chainlink) ─────────────────────────────────────────
with tabs[3]:
    df = load_table("btc_spot_ticks", selected_date)
    df = _heavy_table_header("btc_spot_ticks", selected_date, df, "load_full_cl")
    if df.empty:
        st.info("Aucune donnée")
    else:
        st.markdown(f"**{len(df):,} ticks Chainlink** &nbsp;·&nbsp; colonnes: `{len(df.columns)}`")

        if "ts_ms" in df.columns:
            first = fmt_ts(df["ts_ms"].min())
            last  = fmt_ts(df["ts_ms"].max())
            expected = (df["ts_ms"].max() - df["ts_ms"].min()) / 1000
            coverage = 100 * len(df) / expected if expected > 0 else 100
            s1, s2, s3 = st.columns(3)
            s1.metric("Premier tick", first)
            s2.metric("Dernier tick", last)
            s3.metric("Coverage 1/s", f"{coverage:.1f}%",
                      delta="✅ OK" if coverage > 95 else "⚠️ Gap",
                      delta_color="normal" if coverage > 95 else "inverse")

        n_rows = st.slider("Nb lignes", 100, min(5000, len(df)), 500, key="btc_rows")
        df_show = df.copy()
        if "ts_ms" in df_show.columns:
            df_show.insert(0, "heure", df_show["ts_ms"].apply(fmt_ts))
            df_show = df_show.drop(columns=["ts_ms"])
        st.dataframe(df_show.tail(n_rows), use_container_width=True, height=500)

# ── Tab: btc_spot_ticks_binance ──────────────────────────────────────────────
with tabs[4]:
    df = load_table("btc_spot_ticks_binance", selected_date)
    df = _heavy_table_header("btc_spot_ticks_binance", selected_date, df, "load_full_bn")
    if df.empty:
        st.info("Aucune donnée (source secondaire)")
    else:
        st.markdown(f"**{len(df):,} ticks Binance** &nbsp;·&nbsp; colonnes: `{len(df.columns)}`")
        n_rows = st.slider("Nb lignes", 100, min(5000, len(df)), 500, key="bn_rows")
        df_show = df.copy()
        if "ts_ms" in df_show.columns:
            df_show.insert(0, "heure", df_show["ts_ms"].apply(fmt_ts))
            df_show = df_show.drop(columns=["ts_ms"])
        st.dataframe(df_show.tail(n_rows), use_container_width=True, height=500)

# ── Tab: market_snapshots ────────────────────────────────────────────────────
with tabs[5]:
    df = load_table("market_snapshots", selected_date)
    if df.empty:
        st.info("Aucune donnée")
    else:
        st.markdown(f"**{len(df)} snapshots**")

        # Détecte corrompus
        if all(c in df.columns for c in ["total_ticks", "total_trades", "total_volume_usdc"]):
            df["status"] = df.apply(
                lambda r: "❌ CORROMPU" if (r["total_ticks"] == 0 and r["total_trades"] == 0) else "✅ OK",
                axis=1
            )

        if "snapshot_ts_ms" in df.columns:
            df.insert(0, "heure", df["snapshot_ts_ms"].apply(fmt_ts))

        priority = ["heure", "market_id", "status", "winning_outcome",
                    "total_ticks", "total_trades", "total_volume_usdc",
                    "open_price_yes", "close_price_yes", "min_price_yes", "max_price_yes",
                    "price_at_1min", "price_at_30s",
                    "btc_open", "btc_close", "btc_move_pct", "btc_volatility",
                    "final_moneyness", "total_duration_sec"]
        cols_show = [c for c in priority if c in df.columns] + \
                    [c for c in df.columns if c not in priority and "ts_ms" not in c]

        st.dataframe(df[cols_show], use_container_width=True, height=500)

        # Résumé outcome
        if "winning_outcome" in df.columns:
            st.markdown("**Distribution outcomes**")
            vc = df["winning_outcome"].value_counts()
            c1, c2, c3 = st.columns(3)
            c1.metric("YES", int(vc.get("YES", 0)))
            c2.metric("NO", int(vc.get("NO", 0)))
            c3.metric("Non résolu", int(df["winning_outcome"].isna().sum()))

# ── Tab: fichiers R2 ─────────────────────────────────────────────────────────
with tabs[6]:
    st.markdown("**Fichiers R2 pour cette date**")
    files_df = get_r2_file_list(selected_date)
    if files_df.empty:
        st.info("Aucun fichier")
    else:
        st.markdown(f"**{len(files_df)} fichiers**")
        st.dataframe(files_df, use_container_width=True, height=600)
        total_mb = files_df["size_kb"].sum() / 1024
        st.metric("Taille totale", f"{total_mb:.2f} MB")