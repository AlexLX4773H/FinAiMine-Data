# FinAiMine-Data

> PySpark + Delta Lake — ETL Pipelines, Medallion Architecture, Feature Engineering

Part of the [Fin AI Mine](../plan.md) project.

## Tech Stack
- PySpark 3.5 + Delta Lake 3.2
- yfinance, NewsAPI, Financial Modeling Prep, mftool
- scikit-learn / XGBoost (prediction models)

## Prerequisites
- Java 11 or 17 (set `JAVA_HOME`)
- Python 3.10+

## Getting Started
```bash
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
python -m etl.ingest_prices
```
