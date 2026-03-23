"""
Dashboard Streamlit — inspection des données R2 du scraper BTC Polymarket.
Lecture seule depuis R2, aucun impact sur le scraper.

Optimisé : chargement lazy par défaut (preview ~500 lignes par fichier parquet),
avec possibilité de tout charger si nécessaire.
"""
import os
import io
from datetime import datetime, timezone

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

.preview-banner {
    background: #1c1f2e;
    border: 1px solid #2d3748;
    border-left: 3px solid #f59e0b;
    border-radius: 6px;
    padding: 8px 14px;
    font-size: 0.75rem;
    color: #f59e0b;
    margin-bottom: 10px;
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
</style>
""", unsafe_allow_html=True)

# ─── Constantes ─────────────────────────────────────────────────────────────
BUCKET = os.getenv("R2_BUCKET_NAME", "polymarket-btc-data")
TABLES = ["btc_markets", "orderbook_ticks", "trades", "btc_spot_ticks", "btc_spot_ticks_binance", "market_snapshots"]

# Nb de lignes max par défaut pour les grandes tables (preview)
PREVIEW_ROWS = 500
# Nb de fichiers parquet chargés par défaut en mode preview
PREVIEW_FILES = 1

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

# ─── Helpers ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=120)
def list_dates():
    s3 = get_s3()
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="parquet/btc_spot_ticks/", Delimiter="/")
    dates = []
    for p in resp.get("CommonPrefixes", []):
        parts = p["Prefix"].rstrip("/").split("/")
        if len(parts) >= 3:
            dates.append(parts[2])
    return sorted(dates, reverse=True)


@st.cache_data(ttl=60)
def list_parquet_keys(table: str, date: str) -> list[str]:
    """Liste les clés parquet disponibles pour une table/date — léger, pas de téléchargement."""
    s3 = get_s3()
    prefix = f"parquet/{table}/{date}/"
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = sorted([o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")])
    return keys


def _download_parquet(key: str, row_limit: int | None = None) -> pd.DataFrame:
    """Télécharge un fichier parquet depuis R2 et le lit avec DuckDB.
    Si row_limit est défini, ne retourne que les N premières lignes (via LIMIT SQL).
    """
    s3 = get_s3()
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET, key, buf)
    buf.seek(0)
    tmp = f"/tmp/{key.replace('/', '_')}"
    with open(tmp, "wb") as f:
        f.write(buf.read())
    try:
        con = duckdb.connect()
        limit_clause = f"LIMIT {row_limit}" if row_limit else ""
        df = con.execute(f"SELECT * FROM read_parquet('{tmp}') {limit_clause}").df()
        con.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_table_preview(table: str, date: str) -> tuple[pd.DataFrame, int, int]:
    """Charge un extrait léger : premier fichier parquet, limité à PREVIEW_ROWS lignes.
    Retourne (df, nb_fichiers_total, nb_lignes_estimé).
    """
    keys = list_parquet_keys(table, date)
    if not keys:
        return pd.DataFrame(), 0, 0

    # On charge seulement le premier fichier pour le preview
    df = _download_parquet(keys[0], row_limit=PREVIEW_ROWS)
    return df, len(keys), -1  # -1 = total inconnu en mode preview


@st.cache_data(ttl=60, show_spinner=False)
def load_table_full(table: str, date: str) -> pd.DataFrame:
    """Charge TOUS les fichiers parquet d'une table pour une date.
    À n'appeler que sur demande explicite de l'utilisateur.
    """
    keys = list_parquet_keys(table, date)
    if not keys:
        return pd.DataFrame()

    dfs = []
    for key in keys:
        df = _download_parquet(key)
        if not df.empty:
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


@st.cache_data(ttl=120, show_spinner=False)
def get_table_row_counts(date: str) -> dict:
    """Compte les lignes en téléchargeant uniquement un fichier par table (rapide)."""
    counts = {}
    for t in TABLES:
        keys = list_parquet_keys(t, date)
        counts[t] = {"files": len(keys), "sample_rows": 0}
        if keys:
            # On ne télécharge que le 1er fichier pour estimer
            df = _download_parquet(keys[0])
            counts[t]["sample_rows"] = len(df)
    return counts


@st.cache_data(ttl=120, show_spinner=False)
def get_r2_file_list(date: str, max_files: int = 200):
    """Liste les fichiers R2 avec pagination (max_files par appel)."""
    s3 = get_s3()
    prefix = f"parquet/"
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            if date in o["Key"]:
                files.append({
                    "path": o["Key"],
                    "size_kb": round(o["Size"] / 1024, 1),
                    "modified": o["LastModified"].strftime("%H:%M:%S"),
                })
                if len(files) >= max_files:
                    return pd.DataFrame(files)
    return pd.DataFrame(files)


def fmt_ts(ts_ms):
    if pd.isna(ts_ms):
        return ""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def preview_banner(nb_files: int, nb_loaded: int = PREVIEW_FILES):
    """Affiche un bandeau avertissant que c'est un extrait."""
    if nb_files > nb_loaded:
        st.markdown(
            f'<div class="preview-banner">⚡ Mode preview — {nb_loaded}/{nb_files} fichier(s) chargé(s), '
            f'≤{PREVIEW_ROWS} lignes. Cliquez <b>Charger tout</b> pour voir l\'ensemble des données.</div>',
            unsafe_allow_html=True,
        )


def load_full_button(key: str) -> bool:
    """Affiche le bouton 'Charger tout' et retourne True si cliqué."""
    return st.button("📥 Charger tout (attention : peut être volumineux)", key=key, type="secondary")


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
    st.markdown('<div style="font-size:0.65rem;color:#475569;margin-top:6px;">⚡ Mode preview actif par défaut — charge 1 fichier / table</div>', unsafe_allow_html=True)

# ─── Header ─────────────────────────────────────────────────────────────────
st.markdown(f"## Data Inspector &nbsp;·&nbsp; <span style='color:#38bdf8'>{selected_date}</span>", unsafe_allow_html=True)

# ─── Métriques globales (légères) ───────────────────────────────────────────
with st.spinner("Chargement des métriques (preview)..."):
    counts = get_table_row_counts(selected_date)

labels = {
    "btc_markets": "Marchés",
    "orderbook_ticks": "OB Ticks",
    "trades": "Trades",
    "btc_spot_ticks": "BTC (CL)",
    "btc_spot_ticks_binance": "BTC (BN)",
    "market_snapshots": "Snapshots",
}

col1, col2, col3, col4, col5, col6 = st.columns(6)
cols_metric = [col1, col2, col3, col4, col5, col6]
for i, t in enumerate(TABLES):
    info = counts.get(t, {})
    nb_files = info.get("files", 0)
    sample = info.get("sample_rows", 0)
    label_val = f"{nb_files} fichier{'s' if nb_files != 1 else ''}"
    with cols_metric[i]:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{label_val}</div>
            <div class="metric-label">{labels[t]}</div>
            <div style="font-size:0.65rem;color:#475569;margin-top:4px;">~{sample:,} lignes/fichier</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─── Onglets ────────────────────────────────────────────────────────────────
tabs = st.tabs(["📋 Marchés", "📊 Orderbook", "💸 Trades", "₿ BTC Chainlink", "₿ BTC Binance", "📸 Snapshots", "🗂 Fichiers R2"])

# ── Tab: btc_markets ────────────────────────────────────────────────────────
with tabs[0]:
    df_m, nb_files_m, _ = load_table_preview("btc_markets", selected_date)
    preview_banner(nb_files_m)

    load_all_m = load_full_button("load_all_markets")
    if load_all_m:
        with st.spinner("Chargement complet..."):
            df_m = load_table_full("btc_markets", selected_date)

    if df_m.empty:
        st.info("Aucune donnée")
    else:
        if "market_id" in df_m.columns:
            df_m = df_m.drop_duplicates(subset=["market_id"], keep="last")

        st.markdown(f"**{len(df_m)} marchés affichés** &nbsp;·&nbsp; colonnes: `{len(df_m.columns)}`")

        for col in ["open_ts_ms", "expiry_ts_ms", "closed_ts_ms"]:
            if col in df_m.columns:
                df_m[col + "_fmt"] = df_m[col].apply(fmt_ts)

        priority = ["market_id", "question", "open_ts_ms_fmt", "expiry_ts_ms_fmt",
                    "initial_price_yes", "btc_spot_at_open", "price_to_beat",
                    "winning_outcome", "resolved",
                    "prev1_outcome", "prev2_outcome", "prev3_outcome",
                    "prev_no_streak", "prev_btc_down_streak"]
        cols_show = [c for c in priority if c in df_m.columns] + \
                    [c for c in df_m.columns if c not in priority and not c.endswith("_ts_ms")]
        st.dataframe(df_m[cols_show], use_container_width=True, height=400)

# ── Tab: orderbook_ticks ────────────────────────────────────────────────────
with tabs[1]:
    df_ob, nb_files_ob, _ = load_table_preview("orderbook_ticks", selected_date)
    preview_banner(nb_files_ob)

    load_all_ob = load_full_button("load_all_ob")
    if load_all_ob:
        with st.spinner("Chargement complet orderbook (peut être volumineux)..."):
            df_ob = load_table_full("orderbook_ticks", selected_date)

    if df_ob.empty:
        st.info("Aucune donnée")
    else:
        st.markdown(f"**{len(df_ob):,} ticks affichés** &nbsp;·&nbsp; colonnes: `{len(df_ob.columns)}`")

        c1, c2, c3 = st.columns(3)
        markets = ["Tous"] + (sorted(df_ob["market_id"].dropna().unique().tolist()) if "market_id" in df_ob.columns else [])
        sel_mkt = c1.selectbox("Marché", markets, key="ob_mkt")
        n_rows = c2.slider("Nb lignes affichées", 50, min(2000, len(df_ob)), min(500, len(df_ob)), key="ob_rows")

        df_show = df_ob.copy()
        if sel_mkt != "Tous" and "market_id" in df_show.columns:
            df_show = df_show[df_show["market_id"] == sel_mkt]

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

        st.markdown("**Stats**")
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Spread moyen", f"{df_ob['yes_spread'].mean():.4f}" if "yes_spread" in df_ob.columns else "—")
        s2.metric("Imbalance moyen", f"{df_ob['book_imbalance'].mean():.4f}" if "book_imbalance" in df_ob.columns else "—")
        s3.metric("Mid min/max", f"{df_ob['yes_mid'].min():.3f} / {df_ob['yes_mid'].max():.3f}" if "yes_mid" in df_ob.columns else "—")
        s4.metric("Marchés uniques", df_ob["market_id"].nunique() if "market_id" in df_ob.columns else "—")

# ── Tab: trades ─────────────────────────────────────────────────────────────
with tabs[2]:
    df_tr, nb_files_tr, _ = load_table_preview("trades", selected_date)
    preview_banner(nb_files_tr)

    load_all_tr = load_full_button("load_all_trades")
    if load_all_tr:
        with st.spinner("Chargement complet trades..."):
            df_tr = load_table_full("trades", selected_date)

    if df_tr.empty:
        st.info("Aucune donnée")
    else:
        st.markdown(f"**{len(df_tr):,} trades affichés** &nbsp;·&nbsp; colonnes: `{len(df_tr.columns)}`")

        c1, c2, c3 = st.columns(3)
        markets = ["Tous"] + (sorted(df_tr["market_id"].dropna().unique().tolist()) if "market_id" in df_tr.columns else [])
        sel_mkt = c1.selectbox("Marché", markets, key="tr_mkt")
        sel_side = c2.selectbox("Side", ["Tous", "BUY", "SELL"], key="tr_side")
        n_rows = c3.slider("Nb lignes", 50, min(2000, len(df_tr)), min(500, len(df_tr)), key="tr_rows")

        df_show = df_tr.copy()
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

        st.markdown("**Stats**")
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Volume total", f"{df_tr['size'].sum():,.0f} USDC" if "size" in df_tr.columns else "—")
        s2.metric("Taille moyenne", f"{df_tr['size'].mean():.2f}" if "size" in df_tr.columns else "—")
        s3.metric("BUY / SELL", f"{(df_tr['side']=='BUY').sum()} / {(df_tr['side']=='SELL').sum()}" if "side" in df_tr.columns else "—")
        s4.metric("Slip moyen", f"{df_tr['slippage_vs_mid'].mean():.4f}" if "slippage_vs_mid" in df_tr.columns else "—")

# ── Tab: btc_spot_ticks (Chainlink) ─────────────────────────────────────────
with tabs[3]:
    df_cl, nb_files_cl, _ = load_table_preview("btc_spot_ticks", selected_date)
    preview_banner(nb_files_cl)

    load_all_cl = load_full_button("load_all_cl")
    if load_all_cl:
        with st.spinner("Chargement complet BTC Chainlink..."):
            df_cl = load_table_full("btc_spot_ticks", selected_date)

    if df_cl.empty:
        st.info("Aucune donnée")
    else:
        st.markdown(f"**{len(df_cl):,} ticks Chainlink affichés** &nbsp;·&nbsp; colonnes: `{len(df_cl.columns)}`")

        if "ts_ms" in df_cl.columns:
            first = fmt_ts(df_cl["ts_ms"].min())
            last  = fmt_ts(df_cl["ts_ms"].max())
            expected = (df_cl["ts_ms"].max() - df_cl["ts_ms"].min()) / 1000
            coverage = 100 * len(df_cl) / expected if expected > 0 else 100
            s1, s2, s3 = st.columns(3)
            s1.metric("Premier tick", first)
            s2.metric("Dernier tick", last)
            s3.metric("Coverage 1/s", f"{coverage:.1f}%",
                      delta="✅ OK" if coverage > 95 else "⚠️ Gap",
                      delta_color="normal" if coverage > 95 else "inverse")

        n_rows = st.slider("Nb lignes", 50, min(2000, len(df_cl)), min(500, len(df_cl)), key="btc_rows")
        df_show = df_cl.copy()
        if "ts_ms" in df_show.columns:
            df_show.insert(0, "heure", df_show["ts_ms"].apply(fmt_ts))
            df_show = df_show.drop(columns=["ts_ms"])
        st.dataframe(df_show.tail(n_rows), use_container_width=True, height=500)

# ── Tab: btc_spot_ticks_binance ──────────────────────────────────────────────
with tabs[4]:
    df_bn, nb_files_bn, _ = load_table_preview("btc_spot_ticks_binance", selected_date)
    preview_banner(nb_files_bn)

    load_all_bn = load_full_button("load_all_bn")
    if load_all_bn:
        with st.spinner("Chargement complet BTC Binance..."):
            df_bn = load_table_full("btc_spot_ticks_binance", selected_date)

    if df_bn.empty:
        st.info("Aucune donnée (source secondaire)")
    else:
        st.markdown(f"**{len(df_bn):,} ticks Binance affichés** &nbsp;·&nbsp; colonnes: `{len(df_bn.columns)}`")
        n_rows = st.slider("Nb lignes", 50, min(2000, len(df_bn)), min(500, len(df_bn)), key="bn_rows")
        df_show = df_bn.copy()
        if "ts_ms" in df_show.columns:
            df_show.insert(0, "heure", df_show["ts_ms"].apply(fmt_ts))
            df_show = df_show.drop(columns=["ts_ms"])
        st.dataframe(df_show.tail(n_rows), use_container_width=True, height=500)

# ── Tab: market_snapshots ────────────────────────────────────────────────────
with tabs[5]:
    df_sn, nb_files_sn, _ = load_table_preview("market_snapshots", selected_date)
    preview_banner(nb_files_sn)

    load_all_sn = load_full_button("load_all_snapshots")
    if load_all_sn:
        with st.spinner("Chargement complet snapshots..."):
            df_sn = load_table_full("market_snapshots", selected_date)

    if df_sn.empty:
        st.info("Aucune donnée")
    else:
        st.markdown(f"**{len(df_sn)} snapshots affichés**")

        if all(c in df_sn.columns for c in ["total_ticks", "total_trades", "total_volume_usdc"]):
            df_sn["status"] = df_sn.apply(
                lambda r: "❌ CORROMPU" if (r["total_ticks"] == 0 and r["total_trades"] == 0) else "✅ OK",
                axis=1
            )

        if "snapshot_ts_ms" in df_sn.columns:
            df_sn.insert(0, "heure", df_sn["snapshot_ts_ms"].apply(fmt_ts))

        priority = ["heure", "market_id", "status", "winning_outcome",
                    "total_ticks", "total_trades", "total_volume_usdc",
                    "open_price_yes", "close_price_yes", "min_price_yes", "max_price_yes",
                    "price_at_1min", "price_at_30s",
                    "btc_open", "btc_close", "btc_move_pct", "btc_volatility",
                    "final_moneyness", "total_duration_sec"]
        cols_show = [c for c in priority if c in df_sn.columns] + \
                    [c for c in df_sn.columns if c not in priority and "ts_ms" not in c]

        st.dataframe(df_sn[cols_show], use_container_width=True, height=500)

        if "winning_outcome" in df_sn.columns:
            st.markdown("**Distribution outcomes**")
            vc = df_sn["winning_outcome"].value_counts()
            c1, c2, c3 = st.columns(3)
            c1.metric("YES", int(vc.get("YES", 0)))
            c2.metric("NO", int(vc.get("NO", 0)))
            c3.metric("Non résolu", int(df_sn["winning_outcome"].isna().sum()))

# ── Tab: fichiers R2 ─────────────────────────────────────────────────────────
with tabs[6]:
    st.markdown("**Fichiers R2 pour cette date** (max 200)")
    with st.spinner("Listing R2..."):
        files_df = get_r2_file_list(selected_date, max_files=200)

    if files_df.empty:
        st.info("Aucun fichier")
    else:
        st.markdown(f"**{len(files_df)} fichier(s) listés**")
        st.dataframe(files_df, use_container_width=True, height=600)
        total_mb = files_df["size_kb"].sum() / 1024
        st.metric("Taille totale (sample)", f"{total_mb:.2f} MB")