"""
FinAiMine-Data — Bronze Layer: Ingest Mutual Funds & Index Funds
Source: mftool (Indian MFs) / yfinance (Global/US Mutual Funds & ETFs)
Target Table: bronze_mutual_funds
Schema: scheme_code, scheme_name, nav, date, _ingested_at
"""

import datetime
import logging
from typing import List, Optional
import pandas as pd
import yfinance as yf

from config.settings import TABLE_PATHS
from config.spark_config import write_delta_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MFS = [
    # Prominent US mutual funds & ETFs
    {"symbol": "VFIAX", "name": "Vanguard 500 Index Fund Admiral Shares"},
    {"symbol": "VTSAX", "name": "Vanguard Total Stock Market Index Fund"},
    {"symbol": "QQQ", "name": "Invesco QQQ Trust ETF"},
    {"symbol": "SPY", "name": "SPDR S&P 500 ETF Trust"},
]


def fetch_mf_via_yfinance(funds: List[dict]) -> List[dict]:
    """
    Fetches latest NAV/close for mutual funds and ETFs using yfinance.
    """
    records = []
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for item in funds:
        sym = item["symbol"]
        name = item["name"]
        logger.info("Fetching NAV for fund: %s (%s)", sym, name)
        try:
            t = yf.Ticker(sym)
            hist = t.history(period="1mo")
            if not hist.empty:
                hist = hist.reset_index()
                for _, row in hist.iterrows():
                    dt_val = row["Date"]
                    date_str = dt_val.strftime("%Y-%m-%d") if hasattr(dt_val, "strftime") else str(dt_val)[:10]
                    records.append({
                        "scheme_code": sym,
                        "scheme_name": name,
                        "nav": float(row.get("Close", 0.0)),
                        "date": date_str,
                        "_ingested_at": now_iso,
                    })
        except Exception as e:
            logger.warning("Error fetching data for %s: %s", sym, e)

    return records


def fetch_mftool_funds() -> List[dict]:
    """
    Attempts to fetch Indian Mutual Funds data if mftool is available.
    """
    records = []
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
        from mftool import Mftool
        obj = Mftool()
        # Fetch a few top fund codes (e.g. Mirae, SBI, HDFC)
        sample_codes = ["120716", "119551", "102885"]
        for code in sample_codes:
            data = obj.get_scheme_quote(code)
            if data and "nav" in data:
                records.append({
                    "scheme_code": str(code),
                    "scheme_name": data.get("scheme_name", f"Fund {code}"),
                    "nav": float(data.get("nav", 0.0)),
                    "date": data.get("last_updated", str(datetime.date.today())),
                    "_ingested_at": now_iso,
                })
    except Exception as e:
        logger.info("mftool fetch optional notice: %s", e)
    return records


def run_ingest_mutual_funds():
    """
    Ingests mutual fund NAVs and writes to bronze_mutual_funds Delta table.
    """
    logger.info("Starting Bronze Mutual Funds ingestion...")
    all_records = []

    # 1. Fetch yfinance mutual funds / ETFs
    all_records.extend(fetch_mf_via_yfinance(DEFAULT_MFS))

    # 2. Fetch mftool if possible
    all_records.extend(fetch_mftool_funds())

    if not all_records:
        logger.warning("No mutual fund data ingested.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    table_path = TABLE_PATHS["bronze_mutual_funds"]
    logger.info("Writing %d mutual fund rows to Delta table: %s", len(df), table_path)
    write_delta_table(df, table_path, mode="overwrite")
    logger.info("Bronze Mutual Funds ingestion complete.")
    return df


if __name__ == "__main__":
    run_ingest_mutual_funds()
