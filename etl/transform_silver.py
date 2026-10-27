"""
FinAiMine-Data — Silver Layer Transformations
Bronze → Silver Transforms:
- silver_prices: Deduplicated, null-filled, sorted, typed
- silver_news: Deduplicated, VADER sentiment scored (compound, pos, neg, neu)
- silver_fundamentals: Normalized schemas, growth rates computed
- silver_mutual_funds: Cleaned NAVs, return metrics calculated
"""

import datetime
import logging
import pandas as pd
import numpy as np

from config.settings import TABLE_PATHS
from config.spark_config import read_delta_table, write_delta_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def score_sentiment(text: str) -> dict:
    """
    Computes VADER sentiment score for a given text.
    Fallback to simple lexicon if vaderSentiment is not installed.
    """
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        analyzer = SentimentIntensityAnalyzer()
        return analyzer.polarity_scores(text or "")
    except Exception:
        # Fallback heuristic
        t = (text or "").lower()
        pos = sum(1 for w in ["gain", "surge", "bull", "growth", "profit", "beat", "high", "upgrade", "record"] if w in t)
        neg = sum(1 for w in ["loss", "drop", "bear", "fall", "decline", "miss", "low", "downgrade", "crisis"] if w in t)
        total = pos + neg + 1e-6
        compound = (pos - neg) / total
        return {"compound": round(compound, 3), "pos": pos, "neg": neg, "neu": 1.0}


def transform_silver_prices():
    """
    Transforms bronze_prices → silver_prices.
    """
    logger.info("Transforming bronze_prices → silver_prices...")
    df = read_delta_table(TABLE_PATHS["bronze_prices"])
    if df.empty:
        logger.warning("No data in bronze_prices table.")
        return df

    # 1. Type casting and null filling
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    for col in ["open", "high", "low", "close", "volume", "adj_close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # 2. Deduplication and sort
    df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
    df = df.sort_values(by=["ticker", "date"]).reset_index(drop=True)

    # 3. Compute daily returns & price change
    df["daily_return"] = df.groupby("ticker")["close"].pct_change().fillna(0.0)
    df["price_change"] = df.groupby("ticker")["close"].diff().fillna(0.0)
    df["_transformed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    table_path = TABLE_PATHS["silver_prices"]
    logger.info("Writing %d cleaned rows to silver_prices...", len(df))
    write_delta_table(df, table_path, mode="overwrite", partition_by=["ticker"])
    return df


def transform_silver_news():
    """
    Transforms bronze_news → silver_news with sentiment scoring.
    """
    logger.info("Transforming bronze_news → silver_news...")
    df = read_delta_table(TABLE_PATHS["bronze_news"])
    if df.empty:
        logger.warning("No data in bronze_news table.")
        return df

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["headline"] = df["headline"].fillna("").astype(str)
    df["content"] = df["content"].fillna("").astype(str)

    # Deduplicate
    df = df.drop_duplicates(subset=["ticker", "headline"], keep="last").reset_index(drop=True)

    # Sentiment scoring
    compounds = []
    pos_scores = []
    neg_scores = []
    labels = []

    for _, row in df.iterrows():
        combined_text = f"{row['headline']} {row['content']}".strip()
        scores = score_sentiment(combined_text)
        comp = scores.get("compound", 0.0)
        compounds.append(comp)
        pos_scores.append(scores.get("pos", 0.0))
        neg_scores.append(scores.get("neg", 0.0))

        if comp >= 0.05:
            labels.append("Bullish")
        elif comp <= -0.05:
            labels.append("Bearish")
        else:
            labels.append("Neutral")

    df["sentiment_compound"] = compounds
    df["sentiment_positive"] = pos_scores
    df["sentiment_negative"] = neg_scores
    df["sentiment_label"] = labels
    df["_transformed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    table_path = TABLE_PATHS["silver_news"]
    logger.info("Writing %d scored news rows to silver_news...", len(df))
    write_delta_table(df, table_path, mode="overwrite", partition_by=["ticker"])
    return df


def transform_silver_fundamentals():
    """
    Transforms bronze_fundamentals → silver_fundamentals.
    """
    logger.info("Transforming bronze_fundamentals → silver_fundamentals...")
    df = read_delta_table(TABLE_PATHS["bronze_fundamentals"])
    if df.empty:
        logger.warning("No data in bronze_fundamentals table.")
        return df

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    for col in ["revenue", "net_income", "eps", "pe_ratio", "market_cap", "debt_to_equity", "pb_ratio", "dividend_yield"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Compute net profit margin
    df["profit_margin"] = np.where(
        df["revenue"] > 0,
        (df["net_income"] / df["revenue"]) * 100,
        0.0
    )
    df["_transformed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    table_path = TABLE_PATHS["silver_fundamentals"]
    logger.info("Writing %d rows to silver_fundamentals...", len(df))
    write_delta_table(df, table_path, mode="overwrite", partition_by=["ticker"])
    return df


def transform_silver_mutual_funds():
    """
    Transforms bronze_mutual_funds → silver_mutual_funds.
    """
    logger.info("Transforming bronze_mutual_funds → silver_mutual_funds...")
    df = read_delta_table(TABLE_PATHS["bronze_mutual_funds"])
    if df.empty:
        logger.warning("No data in bronze_mutual_funds table.")
        return df

    df["scheme_code"] = df["scheme_code"].astype(str)
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce").fillna(0.0)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    df = df.drop_duplicates(subset=["scheme_code", "date"]).sort_values(by=["scheme_code", "date"])
    df["nav_change"] = df.groupby("scheme_code")["nav"].pct_change().fillna(0.0) * 100
    df["_transformed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    table_path = TABLE_PATHS["silver_mutual_funds"]
    logger.info("Writing %d rows to silver_mutual_funds...", len(df))
    write_delta_table(df, table_path, mode="overwrite")
    return df


def run_silver_pipeline():
    """
    Runs all Silver layer transformations sequentially.
    """
    logger.info("=== Starting Silver Pipeline ===")
    transform_silver_prices()
    transform_silver_news()
    transform_silver_fundamentals()
    transform_silver_mutual_funds()
    logger.info("=== Silver Pipeline Finished ===")


if __name__ == "__main__":
    run_silver_pipeline()
