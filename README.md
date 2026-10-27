# FinAiMine-Data

> **PySpark + Delta Lake Medallion Data Pipeline & ML Feature Engineering**

Part of the [Fin AI Mine](../plan.md) system. FinAiMine-Data ingests financial market data, cleans and scores sentiment in Silver tables, computes technical indicators in Gold tables, and trains ML models for next-day stock price predictions.

---

## 🏛 Medallion Architecture

```
delta_warehouse/
├── bronze/
│   ├── prices/          # Raw OHLCV from yfinance / Alpha Vantage
│   ├── news/            # Financial news articles (NewsAPI / Yahoo)
│   ├── fundamentals/    # Balance sheets, P/E, EPS, Margins (FMP / yfinance)
│   └── mutual_funds/    # NAV time series (mftool / ETFs)
├── silver/
│   ├── prices/          # Deduplicated, cleaned, partitioned by ticker
│   ├── news/            # Scored with VADER sentiment (compound, pos, neg)
│   ├── fundamentals/    # Normalized schemas & profit margins
│   └── mutual_funds/    # Return metrics calculated
└── gold/
    ├── stock_features/  # SMA(20/50/200), EMA(12/26), RSI, MACD, Bollinger Bands
    ├── sentiment_agg/   # Daily sentiment score & news volume per ticker
    ├── predictions/     # Next-day ML model predictions & confidence scores
    └── portfolio_summary/# Aggregated market stats & top movers
```

---

## 🚀 Getting Started

### 1. Setup Virtual Environment
```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate # Linux/Mac
pip install -r requirements.txt
```

### 2. Configure Environment
Copy `.env.example` to `.env` and configure keys if available:
```bash
cp .env.example .env
```
*(All pipelines have full free-tier fallbacks using `yfinance` when API keys are not supplied).*

### 3. Run Pipelines

Run the master pipeline:
```bash
python run_pipeline.py
```

Or run individual steps:
```bash
# Bronze Ingestion
python -m etl.ingest_prices
python -m etl.ingest_news
python -m etl.ingest_fundamentals
python -m etl.ingest_mutual_funds

# Silver Transformations
python -m etl.transform_silver

# Gold Transformations
python -m etl.transform_gold

# Model Training & Predictions
python -m models.train
```

---

## 🤖 Prediction Model

- **Algorithm:** Gradient Boosting Regressor / Random Forest Regressor (`sklearn`)
- **Features:** Price lags (1, 2, 3, 5 days), return momentum, SMA ratios (20, 50), RSI(14), MACD, Bollinger Band position, rolling 20d volatility.
- **Target:** Next trading day close price.
- **Output:** Stored in `gold_predictions` Delta table with confidence metric and direction (BULLISH / BEARISH).
