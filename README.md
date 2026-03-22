# polymarket-btc-scraper

Scraper autonome pour les marchés BTC 5-min Polymarket.
Collecte tick data, order book, trades et prix BTC spot en continu.
Stocke en DuckDB local + Parquet → Cloudflare R2.

## Architecture

```
binance_ws     → btc_spot_ticks       (prix BTC spot, 1/s)
discoverer     → btc_markets          (nouveaux marchés BTC 5-min)
polymarket_ws  → trades               (chaque trade exécuté)
book_poller    → orderbook_ticks      (snapshot complet, 1/s par marché)
snapshot_builder → market_snapshots  (résumé à l'expiry, label ML)
```

## Déploiement Railway

1. Fork ce repo sur GitHub
2. Nouveau projet Railway → "Deploy from GitHub repo"
3. Variables d'environnement à renseigner (voir `.env.example`) :

```
R2_ACCOUNT_ID=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET_NAME=polymarket-btc-data
R2_ENDPOINT_URL=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
TELEGRAM_BOT_TOKEN=          # optionnel
TELEGRAM_CHAT_ID=            # optionnel
```

4. Railway détecte le Dockerfile automatiquement → Deploy

## Cloudflare R2

1. Dashboard Cloudflare → R2 → Create bucket `polymarket-btc-data`
2. Manage R2 API tokens → Create token (Object Read & Write)
3. Copie Account ID, Access Key, Secret Key dans Railway

## Structure des données R2

```
parquet/
  YYYY-MM-DD/
    btc_markets.parquet
    orderbook_ticks.parquet
    trades.parquet
    btc_spot_ticks.parquet
    market_snapshots.parquet
```

## Consommation Railway estimée

~$1.50–2.50/mois (process I/O léger, ~200MB RAM, 0.05 vCPU moyen)
