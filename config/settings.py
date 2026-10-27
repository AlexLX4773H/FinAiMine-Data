"""
FinAiMine-Data — Settings & Configuration
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# Delta Lake Warehouse Path
warehouse_env = os.getenv("DELTA_WAREHOUSE_PATH", "./delta_warehouse")
if os.path.isabs(warehouse_env):
    DELTA_WAREHOUSE_PATH = Path(warehouse_env)
else:
    DELTA_WAREHOUSE_PATH = (BASE_DIR / warehouse_env).resolve()

# Tracked Tickers
default_tickers = "AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA,META,SPY"
TRACKED_TICKERS = [
    t.strip().upper()
    for t in os.getenv("TRACKED_TICKERS", default_tickers).split(",")
    if t.strip()
]

# API Keys
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
FMP_KEY = os.getenv("FMP_KEY", "")
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")

# Spark Config
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")

# Delta Subdirectories
BRONZE_DIR = DELTA_WAREHOUSE_PATH / "bronze"
SILVER_DIR = DELTA_WAREHOUSE_PATH / "silver"
GOLD_DIR = DELTA_WAREHOUSE_PATH / "gold"

# Table Paths
TABLE_PATHS = {
    # Bronze
    "bronze_prices": str(BRONZE_DIR / "prices"),
    "bronze_news": str(BRONZE_DIR / "news"),
    "bronze_fundamentals": str(BRONZE_DIR / "fundamentals"),
    "bronze_mutual_funds": str(BRONZE_DIR / "mutual_funds"),
    # Silver
    "silver_prices": str(SILVER_DIR / "prices"),
    "silver_news": str(SILVER_DIR / "news"),
    "silver_fundamentals": str(SILVER_DIR / "fundamentals"),
    "silver_mutual_funds": str(SILVER_DIR / "mutual_funds"),
    # Gold
    "gold_stock_features": str(GOLD_DIR / "stock_features"),
    "gold_sentiment_agg": str(GOLD_DIR / "sentiment_agg"),
    "gold_predictions": str(GOLD_DIR / "predictions"),
    "gold_portfolio_summary": str(GOLD_DIR / "portfolio_summary"),
}

# Ensure warehouse directories exist
for p in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    p.mkdir(parents=True, exist_ok=True)
