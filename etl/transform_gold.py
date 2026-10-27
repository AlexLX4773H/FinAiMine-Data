"""
FinAiMine-Data — Gold Layer Transformations
Silver → Gold Transforms:
- gold_stock_features: Technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands, ATR, 52w range)
- gold_sentiment_agg: Daily average sentiment per ticker & news volume
- gold_portfolio_summary: Aggregated market & watchlist summary
"""

import datetime
import logging
import pandas as pd
import numpy as np

from config.settings import TABLE_PATHS
from config.spark_config import read_delta_table, write_delta_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def compute_technical_indicators(df_ticker: pd.DataFrame) -> pd.DataFrame:
    """
    Computes standard technical indicators for a single ticker DataFrame sorted by date.
    - SMA (20, 50, 200)
    - EMA (12, 26)
    - RSI (14)
    - MACD (12, 26, 9)
    - Bollinger Bands (20, 2 std)
    - 52-Week High & Low
    - Rolling Volatility (20d)
    """
    df = df_ticker.sort_values(by="date").copy()
    close = df["close"]

    # Moving Averages
    df["sma_20"] = close.rolling(window=20, min_periods=1).mean()
    df["sma_50"] = close.rolling(window=50, min_periods=1).mean()
    df["sma_200"] = close.rolling(window=200, min_periods=1).mean()

    df["ema_12"] = close.ewm(span=12, adjust=False).mean()
    df["ema_26"] = close.ewm(span=26, adjust=False).mean()

    # MACD
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    # Bollinger Bands
    rolling_std = close.rolling(window=20, min_periods=1).std().fillna(0.0)
    df["bb_middle"] = df["sma_20"]
    df["bb_upper"] = df["bb_middle"] + (2 * rolling_std)
    df["bb_lower"] = df["bb_middle"] - (2 * rolling_std)

    # RSI (14)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    avg_gain = gain.rolling(window=14, min_periods=1).mean()
    avg_loss = loss.rolling(window=14, min_periods=1).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    df["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

    # 52-Week Range (~252 trading days)
    df["week_52_high"] = close.rolling(window=252, min_periods=1).max()
    df["week_52_low"] = close.rolling(window=252, min_periods=1).min()

    # Rolling Volatility (20d annualised)
    if "daily_return" in df.columns:
        df["volatility_20d"] = df["daily_return"].rolling(window=20, min_periods=1).std() * np.sqrt(252)
    else:
        df["volatility_20d"] = 0.0

    return df


def transform_gold_stock_features():
    """
    Transforms silver_prices → gold_stock_features.
    """
    logger.info("Transforming silver_prices → gold_stock_features...")
    df = read_delta_table(TABLE_PATHS["silver_prices"])
    if df.empty:
        logger.warning("No data in silver_prices table.")
        return df

    features_list = []
    for ticker, group in df.groupby("ticker"):
        computed = compute_technical_indicators(group)
        features_list.append(computed)

    gold_df = pd.concat(features_list, ignore_index=True)
    gold_df["_transformed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    table_path = TABLE_PATHS["gold_stock_features"]
    logger.info("Writing %d rows to gold_stock_features...", len(gold_df))
    write_delta_table(gold_df, table_path, mode="overwrite", partition_by=["ticker"])
    return gold_df


def transform_gold_sentiment_agg():
    """
    Transforms silver_news → gold_sentiment_agg.
    Calculates daily average sentiment and news volume per ticker.
    """
    logger.info("Transforming silver_news → gold_sentiment_agg...")
    df = read_delta_table(TABLE_PATHS["silver_news"])
    if df.empty:
        logger.warning("No data in silver_news table.")
        return df

    # Normalize published_at to date
    df["date"] = pd.to_datetime(df["published_at"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["date"] = df["date"].fillna(datetime.date.today().strftime("%Y-%m-%d"))

    grouped = df.groupby(["ticker", "date"]).agg(
        avg_sentiment=("sentiment_compound", "mean"),
        news_count=("headline", "count"),
        bullish_count=("sentiment_label", lambda s: (s == "Bullish").sum()),
        bearish_count=("sentiment_label", lambda s: (s == "Bearish").sum()),
        neutral_count=("sentiment_label", lambda s: (s == "Neutral").sum()),
    ).reset_index()

    grouped["avg_sentiment"] = grouped["avg_sentiment"].round(3)
    grouped["_transformed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    table_path = TABLE_PATHS["gold_sentiment_agg"]
    logger.info("Writing %d rows to gold_sentiment_agg...", len(grouped))
    write_delta_table(grouped, table_path, mode="overwrite", partition_by=["ticker"])
    return grouped


def transform_gold_portfolio_summary():
    """
    Generates aggregated portfolio summary from gold_stock_features and sentiment.
    """
    logger.info("Generating gold_portfolio_summary...")
    df_prices = read_delta_table(TABLE_PATHS["gold_stock_features"])
    df_sent = read_delta_table(TABLE_PATHS["gold_sentiment_agg"])

    if df_prices.empty:
        logger.warning("No price data available for portfolio summary.")
        return pd.DataFrame()

    # Get latest record for each ticker
    latest_records = df_prices.sort_values(by=["ticker", "date"]).groupby("ticker").last().reset_index()

    # Attach latest sentiment
    if not df_sent.empty:
        latest_sent = df_sent.sort_values(by=["ticker", "date"]).groupby("ticker").last().reset_index()
        merged = pd.merge(latest_records, latest_sent[["ticker", "avg_sentiment", "news_count"]], on="ticker", how="left")
    else:
        merged = latest_records
        merged["avg_sentiment"] = 0.0
        merged["news_count"] = 0

    merged["avg_sentiment"] = merged["avg_sentiment"].fillna(0.0)
    merged["news_count"] = merged["news_count"].fillna(0)

    # Top Gainers & Losers
    top_gainers = merged.sort_values(by="daily_return", ascending=False).head(5)[["ticker", "close", "daily_return"]].to_dict(orient="records")
    top_losers = merged.sort_values(by="daily_return", ascending=True).head(5)[["ticker", "close", "daily_return"]].to_dict(orient="records")

    summary_record = {
        "summary_date": datetime.date.today().strftime("%Y-%m-%d"),
        "total_tracked": len(merged),
        "market_avg_return": float(merged["daily_return"].mean() * 100),
        "market_avg_sentiment": float(merged["avg_sentiment"].mean()),
        "top_gainers": str(top_gainers),
        "top_losers": str(top_losers),
        "_transformed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    summary_df = pd.DataFrame([summary_record])
    table_path = TABLE_PATHS["gold_portfolio_summary"]
    write_delta_table(summary_df, table_path, mode="overwrite")
    logger.info("Gold portfolio summary updated.")
    return summary_df


def run_gold_pipeline():
    """
    Runs all Gold layer transformations sequentially.
    """
    logger.info("=== Starting Gold Pipeline ===")
    transform_gold_stock_features()
    transform_gold_sentiment_agg()
    transform_gold_portfolio_summary()
    logger.info("=== Gold Pipeline Finished ===")


if __name__ == "__main__":
    run_gold_pipeline()
