<div align="center">
  <h1>📈 Polymarket BTC Scraper</h1>
  <p><strong>Autonomous High-Frequency Data Pipeline for Polymarket & Binance</strong></p>
  
  [![Python](https://img.shields.io/badge/Python-3.10+-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
  [![DuckDB](https://img.shields.io/badge/Database-DuckDB-FFF000.svg)](https://duckdb.org/)
  [![Cloudflare R2](https://img.shields.io/badge/Storage-Cloudflare_R2-F38020.svg?logo=cloudflare)](https://www.cloudflare.com/developer-platform/r2/)
  [![Docker](https://img.shields.io/badge/Docker-Ready-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
  [![Deployment](https://img.shields.io/badge/Deploy_on-Railway-131415.svg)](https://railway.app/)
  
  *Collect, store, and build ML-ready datasets from Polymarket 5-minute Bitcoin prediction markets.*
</div>

<br/>

## 📖 Overview

This repository provides an autonomous, lightweight, and highly optimized scraper designed to track **Polymarket's 5-minute BTC markets** alongside **Binance BTC spot prices**. 

By gathering continuous tick data, order books, and executed trades, it automatically compiles synchronized snapshots perfect for quantitative analysis, algorithmic trading backtesting, and machine learning models. Data is stored locally via **DuckDB**, converted to highly-compressed **Parquet** files, and synchronized with **Cloudflare R2** storage.

## ✨ Key Features

- **⚡ Real-Time WebSockets:** Simultaneous connections to Binance and Polymarket.
- **📊 Granular Tick Data:** 1-second interval snapshots for order books and BTC spot prices.
- **🤖 ML-Ready Output:** Automatically builds market summaries at expiry (perfect labels for predictive models).
- **🪶 Highly Optimized:** Low memory footprint (~200MB RAM) and cheap to run (Estimated ~$2/month on Railway).
- **☁️ Cloud Sync:** Zero-maintenance Parquet exports directly to Cloudflare R2 object storage.
- **📱 Telegram Alerts (Optional):** Receive continuous monitoring logs straight to your phone.

## 🏗️ Architecture Pipeline

```mermaid
graph TD;
    A[Binance WS] -->|Spot Prices 1/s| E[(DuckDB Local)]
    B[Discoverer] -->|Finds 5-min Markets| E
    C[Polymarket WS] -->|Executed Trades| E
    D[Book Poller] -->|Full Snapshot 1/s| E
    
    E --> F{Snapshot Builder}
    F -->|ML Labels & Summary at Expiry| G[Parquet Files]
    
    G --> H((Cloudflare R2))
