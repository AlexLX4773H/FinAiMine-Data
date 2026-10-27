"""
FinAiMine-Data — Bronze Layer: Ingest Stock News
Source: NewsAPI / yfinance News
Target Table: bronze_news
Schema: ticker, headline, source, url, published_at, content, _ingested_at
"""

import datetime
import logging
from typing import List, Optional
import pandas as pd
import requests
import yfinance as yf

from config.settings import TRACKED_TICKERS, NEWSAPI_KEY, TABLE_PATHS
from config.spark_config import write_delta_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_newsapi(ticker: str, api_key: str) -> List[dict]:
    """
    Fetches financial news using NewsAPI.
    """
    if not api_key:
        return []
    url = f"https://newsapi.org/v2/everything?q={ticker}+stock&sortBy=publishedAt&pageSize=15&apiKey={api_key}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            articles = []
            for a in data.get("articles", []):
                articles.append({
                    "ticker": ticker.upper(),
                    "headline": a.get("title") or "",
                    "source": a.get("source", {}).get("name") or "NewsAPI",
                    "url": a.get("url") or "",
                    "published_at": a.get("publishedAt") or "",
                    "content": a.get("description") or a.get("content") or "",
                })
            return articles
    except Exception as e:
        logger.warning("Error fetching NewsAPI for %s: %s", ticker, e)
    return []


def fetch_yfinance_news(ticker: str) -> List[dict]:
    """
    Fetches recent news items directly from yfinance Ticker object.
    Free, no API key required.
    """
    articles = []
    try:
        t = yf.Ticker(ticker)
        news_items = t.news or []
        for item in news_items:
            # yfinance returns content under nested dictionaries in recent versions
            content_dict = item.get("content", item)
            title = content_dict.get("title") or item.get("title") or ""
            publisher = ""
            provider = content_dict.get("provider") or item.get("publisher") or {}
            if isinstance(provider, dict):
                publisher = provider.get("displayName", "Yahoo Finance")
            elif isinstance(provider, str):
                publisher = provider

            link = ""
            canonical = content_dict.get("canonicalUrl") or item.get("link") or {}
            if isinstance(canonical, dict):
                link = canonical.get("url", "")
            elif isinstance(canonical, str):
                link = canonical

            pub_time = content_dict.get("pubDate") or item.get("providerPublishTime") or ""
            if isinstance(pub_time, (int, float)):
                pub_time = datetime.datetime.fromtimestamp(pub_time, datetime.timezone.utc).isoformat()

            summary = content_dict.get("summary") or item.get("summary") or title

            if title:
                articles.append({
                    "ticker": ticker.upper(),
                    "headline": title,
                    "source": publisher or "Yahoo Finance",
                    "url": link,
                    "published_at": str(pub_time),
                    "content": summary,
                })
    except Exception as e:
        logger.warning("Error fetching yfinance news for %s: %s", ticker, e)
    return articles


def run_ingest_news(tickers: Optional[List[str]] = None):
    """
    Ingests news for tracked tickers and writes to bronze_news Delta table.
    """
    target_tickers = tickers or TRACKED_TICKERS
    logger.info("Starting Bronze News ingestion for %d tickers...", len(target_tickers))
    all_articles = []
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for ticker in target_tickers:
        articles = []
        if NEWSAPI_KEY:
            articles.extend(fetch_newsapi(ticker, NEWSAPI_KEY))
        # Always supplement with yfinance news
        articles.extend(fetch_yfinance_news(ticker))

        for a in articles:
            a["_ingested_at"] = now_iso
            all_articles.append(a)

    if not all_articles:
        logger.warning("No news articles fetched.")
        return pd.DataFrame()

    df = pd.DataFrame(all_articles)
    # Deduplicate by url/headline
    df = df.drop_duplicates(subset=["ticker", "headline"])

    table_path = TABLE_PATHS["bronze_news"]
    logger.info("Writing %d news items to Delta table: %s", len(df), table_path)
    write_delta_table(df, table_path, mode="overwrite")
    logger.info("Bronze News ingestion complete.")
    return df


if __name__ == "__main__":
    run_ingest_news()
