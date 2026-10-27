"""
FinAiMine-Data — Model Training Pipeline
Trains StockPredictor models on gold_stock_features and outputs predictions to gold_predictions Delta table.
"""

import datetime
import logging
from typing import Optional, List
import pandas as pd

from config.settings import TABLE_PATHS, TRACKED_TICKERS
from config.spark_config import read_delta_table, write_delta_table
from models.predictor import StockPredictor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def train_and_predict_all(tickers: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Trains models for all tracked tickers and writes predictions to gold_predictions.
    """
    logger.info("=== Starting Model Training Pipeline ===")
    df_features = read_delta_table(TABLE_PATHS["gold_stock_features"])

    if df_features.empty:
        logger.warning("No data in gold_stock_features table to train on.")
        return pd.DataFrame()

    target_tickers = tickers or df_features["ticker"].unique().tolist()
    prediction_records = []
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for ticker in target_tickers:
        ticker_data = df_features[df_features["ticker"] == ticker]
        if len(ticker_data) < 10:
            logger.warning("Skipping %s (only %d records)", ticker, len(ticker_data))
            continue

        logger.info("Training predictor for ticker: %s (%d records)...", ticker, len(ticker_data))
        predictor = StockPredictor(model_type="gradient_boosting")
        train_result = predictor.train(ticker_data)
        prediction = predictor.predict_next(ticker_data)

        prediction_records.append({
            "ticker": ticker.upper(),
            "date": today_str,
            "current_close": prediction["current_close"],
            "predicted_close": prediction["predicted_close"],
            "change_pct": prediction["change_pct"],
            "direction": prediction["direction"],
            "confidence": prediction["confidence"],
            "model_version": prediction["model_version"],
            "horizon": prediction["horizon"],
            "_predicted_at": now_iso,
        })

    if not prediction_records:
        logger.warning("No predictions generated.")
        return pd.DataFrame()

    preds_df = pd.DataFrame(prediction_records)
    table_path = TABLE_PATHS["gold_predictions"]
    logger.info("Writing %d predictions to Delta table: %s", len(preds_df), table_path)
    write_delta_table(preds_df, table_path, mode="overwrite")
    logger.info("=== Model Training & Prediction Pipeline Complete ===")
    return preds_df


if __name__ == "__main__":
    train_and_predict_all()
