"""
FinAiMine-Data — Bronze Layer: Ingest Company Fundamentals
Source: Financial Modeling Prep (FMP) / yfinance
Target Table: bronze_fundamentals
Schema: ticker, period, revenue, net_income, eps, pe_ratio, market_cap, debt_to_equity, pb_ratio, dividend_yield, _ingested_at
"""

import datetime
import logging
from typing import List, Optional
import pandas as pd
import requests
import yfinance as yf

from config.settings import TRACKED_TICKERS, FMP_KEY, TABLE_PATHS
from config.spark_config import write_delta_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_fmp_fundamentals(ticker: str, api_key: str) -> List[dict]:
    """
    Fetches income statement and key metrics from Financial Modeling Prep.
    """
    if not api_key:
        return []
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=4&apikey={api_key}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            results = []
            for item in data:
                results.append({
                    "ticker": ticker.upper(),
                    "period": str(item.get("date", "TTM")),
                    "revenue": float(item.get("revenue", 0.0) or 0.0),
                    "net_income": float(item.get("netIncome", 0.0) or 0.0),
                    "eps": float(item.get("eps", 0.0) or 0.0),
                    "pe_ratio": float(item.get("peRatio", 0.0) or 0.0),
                    "market_cap": float(item.get("marketCap", 0.0) or 0.0),
                    "debt_to_equity": 0.0,
                    "pb_ratio": 0.0,
                    "dividend_yield": 0.0,
                })
            return results
    except Exception as e:
        logger.warning("Error fetching FMP fundamentals for %s: %s", ticker, e)
    return []


def fetch_yfinance_fundamentals(ticker: str) -> List[dict]:
    """
    Fetches key statistics and financials from yfinance.
    """
    results = []
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}

        # Latest overview / TTM
        rev = float(info.get("totalRevenue", 0.0) or 0.0)
        net_inc = float(info.get("netIncomeToCommon", 0.0) or 0.0)
        eps = float(info.get("trailingEps", 0.0) or 0.0)
        pe = float(info.get("trailingPE", 0.0) or 0.0)
        mcap = float(info.get("marketCap", 0.0) or 0.0)
        dte = float(info.get("debtToEquity", 0.0) or 0.0)
        pb = float(info.get("priceToBook", 0.0) or 0.0)
        div = float(info.get("dividendYield", 0.0) or 0.0)

        results.append({
            "ticker": ticker.upper(),
            "period": "TTM",
            "revenue": rev,
            "net_income": net_inc,
            "eps": eps,
            "pe_ratio": pe,
            "market_cap": mcap,
            "debt_to_equity": dte,
            "pb_ratio": pb,
            "dividend_yield": div,
        })
    except Exception as e:
        logger.warning("Error fetching yfinance fundamentals for %s: %s", ticker, e)
    return results


def run_ingest_fundamentals(tickers: Optional[List[str]] = None):
    """
    Ingests fundamentals and writes to bronze_fundamentals Delta table.
    """
    target_tickers = tickers or TRACKED_TICKERS
    logger.info("Starting Bronze Fundamentals ingestion for %d tickers...", len(target_tickers))
    all_records = []
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for ticker in target_tickers:
        records = []
        if FMP_KEY:
            records.extend(fetch_fmp_fundamentals(ticker, FMP_KEY))
        if not records:
            records.extend(fetch_yfinance_fundamentals(ticker))

        for r in records:
            r["_ingested_at"] = now_iso
            all_records.append(r)

    if not all_records:
        logger.warning("No fundamentals data fetched.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    table_path = TABLE_PATHS["bronze_fundamentals"]
    logger.info("Writing %d fundamentals records to Delta table: %s", len(df), table_path)
    write_delta_table(df, table_path, mode="overwrite")
    logger.info("Bronze Fundamentals ingestion complete.")
    return df


if __name__ == "__main__":
    run_ingest_fundamentals()
