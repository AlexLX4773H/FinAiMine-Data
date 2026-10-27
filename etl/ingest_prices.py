"""
FinAiMine-Data — Bronze Layer: Ingest Stock Prices
Source: yfinance / Alpha Vantage
Target Table: bronze_prices
Schema: ticker, date, open, high, low, close, volume, adj_close, _ingested_at
"""

import datetime
import logging
from typing import List, Optional
import pandas as pd
import yfinance as yf

from config.settings import TRACKED_TICKERS, TABLE_PATHS
from config.spark_config import write_delta_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_yfinance_prices(tickers: List[str], period: str = "1y") -> pd.DataFrame:
    """
    Fetches historical OHLCV data from yfinance for given tickers.
    """
    records = []
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for ticker in tickers:
        logger.info("Fetching price data for ticker: %s (period: %s)", ticker, period)
        try:
            t = yf.Ticker(ticker)
            df = t.history(period=period)
            if df.empty:
                logger.warning("No data found for %s", ticker)
                continue

            df = df.reset_index()
            for _, row in df.iterrows():
                # Handle timezone-aware or naive datetime
                dt_val = row["Date"]
                if hasattr(dt_val, "strftime"):
                    date_str = dt_val.strftime("%Y-%m-%d")
                else:
                    date_str = str(dt_val)[:10]

                records.append({
                    "ticker": ticker.upper(),
                    "date": date_str,
                    "open": float(row.get("Open", 0.0)),
                    "high": float(row.get("High", 0.0)),
                    "low": float(row.get("Low", 0.0)),
                    "close": float(row.get("Close", 0.0)),
                    "volume": float(row.get("Volume", 0.0)),
                    "adj_close": float(row.get("Close", 0.0)),
                    "_ingested_at": now_iso,
                })
        except Exception as e:
            logger.error("Error fetching price data for %s: %s", ticker, e)

    return pd.DataFrame(records)


def run_ingest_prices(tickers: Optional[List[str]] = None, period: str = "1y"):
    """
    Ingests price data and writes to bronze_prices Delta table.
    """
    target_tickers = tickers or TRACKED_TICKERS
    logger.info("Starting Bronze Prices ingestion for %d tickers...", len(target_tickers))
    df = fetch_yfinance_prices(target_tickers, period=period)

    if df.empty:
        logger.warning("No price data ingested.")
        return df

    table_path = TABLE_PATHS["bronze_prices"]
    logger.info("Writing %d price rows to Delta table: %s", len(df), table_path)
    write_delta_table(df, table_path, mode="overwrite")
    logger.info("Bronze Prices ingestion complete.")
    return df


if __name__ == "__main__":
    run_ingest_prices()
