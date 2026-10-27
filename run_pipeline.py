"""
FinAiMine-Data — Master Pipeline Runner
Runs the full Medallion pipeline:
1. Ingest Bronze (Prices, News, Fundamentals, Mutual Funds)
2. Transform Silver (Deduplication, Cleaning, Sentiment Scoring)
3. Transform Gold (Technical Indicators, Aggregations, Portfolio Summary)
4. Train Models & Predict (ML Predictions to Gold)
"""

import sys
import logging
from etl.ingest_prices import run_ingest_prices
from etl.ingest_news import run_ingest_news
from etl.ingest_fundamentals import run_ingest_fundamentals
from etl.ingest_mutual_funds import run_ingest_mutual_funds
from etl.transform_silver import run_silver_pipeline
from etl.transform_gold import run_gold_pipeline
from models.train import train_and_predict_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinAiMine-Data")


def run_full_pipeline():
    logger.info("==========================================")
    logger.info("   FIN AI MINE — FULL DATA PIPELINE       ")
    logger.info("==========================================")

    # Phase 1: Bronze
    logger.info(">>> STEP 1: BRONZE INGESTION")
    run_ingest_prices()
    run_ingest_news()
    run_ingest_fundamentals()
    run_ingest_mutual_funds()

    # Phase 2: Silver
    logger.info(">>> STEP 2: SILVER TRANSFORMATIONS")
    run_silver_pipeline()

    # Phase 3: Gold
    logger.info(">>> STEP 3: GOLD FEATURE ENGINEERING")
    run_gold_pipeline()

    # Phase 4: Models & Predictions
    logger.info(">>> STEP 4: MODEL TRAINING & PREDICTION")
    train_and_predict_all()

    logger.info("==========================================")
    logger.info("   PIPELINE EXECUTION COMPLETED!          ")
    logger.info("==========================================")


if __name__ == "__main__":
    run_full_pipeline()
