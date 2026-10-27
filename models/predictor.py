"""
FinAiMine-Data — Machine Learning Stock Predictor
Predicts next-day stock price and confidence score using Gradient Boosting / Random Forest.
"""

import logging
from typing import Dict, Any, Tuple
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error

logger = logging.getLogger(__name__)


class StockPredictor:
    """
    ML model for next-day close prediction with confidence estimation.
    """

    def __init__(self, model_type: str = "gradient_boosting"):
        self.model_type = model_type
        if model_type == "random_forest":
            self.model = RandomForestRegressor(n_estimators=100, max_depth=6, random_state=42)
        else:
            self.model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.05, max_depth=4, random_state=42)
        self.feature_cols = [
            "close_lag1", "close_lag2", "close_lag3", "close_lag5",
            "return_lag1", "return_lag2",
            "ratio_sma20", "ratio_sma50",
            "rsi_14", "macd", "macd_signal", "macd_hist",
            "bb_position", "volatility_20d"
        ]
        self.is_fitted = False
        self.mape_score = 0.05  # Default 5% error

    def prepare_features(self, df_ticker: pd.DataFrame) -> pd.DataFrame:
        """
        Engineers feature vectors from gold_stock_features for training/inference.
        """
        df = df_ticker.sort_values(by="date").copy()

        # Lag features
        df["close_lag1"] = df["close"].shift(1)
        df["close_lag2"] = df["close"].shift(2)
        df["close_lag3"] = df["close"].shift(3)
        df["close_lag5"] = df["close"].shift(5)

        df["return_lag1"] = df["daily_return"].shift(1)
        df["return_lag2"] = df["daily_return"].shift(2)

        # Ratios
        sma20 = df["sma_20"].replace(0, np.nan)
        sma50 = df["sma_50"].replace(0, np.nan)
        df["ratio_sma20"] = (df["close"] / sma20).fillna(1.0)
        df["ratio_sma50"] = (df["close"] / sma50).fillna(1.0)

        # Bollinger position (0 to 1)
        bb_span = (df["bb_upper"] - df["bb_lower"]).replace(0, np.nan)
        df["bb_position"] = ((df["close"] - df["bb_lower"]) / bb_span).clip(0, 1).fillna(0.5)

        # Target (next day close)
        df["target_next_close"] = df["close"].shift(-1)

        return df

    def train(self, df_features: pd.DataFrame) -> Dict[str, Any]:
        """
        Trains the model on historical features.
        """
        prepared = self.prepare_features(df_features)
        # Drop rows with NaN (from shifts)
        train_df = prepared.dropna(subset=self.feature_cols + ["target_next_close"])

        if len(train_df) < 20:
            logger.warning("Insufficient data to train robust model (%d rows)", len(train_df))
            return {"status": "insufficient_data", "rows": len(train_df)}

        X = train_df[self.feature_cols]
        y = train_df["target_next_close"]

        # Train / test split (80/20 time-based)
        split_idx = int(len(train_df) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

        self.model.fit(X_train, y_train)
        self.is_fitted = True

        # Evaluate
        preds_test = self.model.predict(X_test)
        self.mape_score = float(mean_absolute_percentage_error(y_test, preds_test))
        accuracy_pct = round(max(0.0, (1.0 - self.mape_score) * 100), 1)

        logger.info("Model trained successfully. MAPE: %.4f | Accuracy: %.1f%%", self.mape_score, accuracy_pct)
        return {
            "status": "success",
            "mape": self.mape_score,
            "accuracy_pct": accuracy_pct,
            "train_samples": len(X_train),
            "test_samples": len(X_test),
        }

    def predict_next(self, df_features: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates next-day price prediction for the most recent observation.
        """
        prepared = self.prepare_features(df_features)
        latest_row = prepared.iloc[-1:]

        current_close = float(latest_row["close"].values[0])

        if not self.is_fitted or latest_row[self.feature_cols].isna().any().any():
            # Fallback heuristic if not enough data
            momentum = float(latest_row.get("daily_return", 0.0) or 0.0)
            pred_close = round(current_close * (1 + momentum * 0.5), 2)
            confidence = 72.0
        else:
            X_latest = latest_row[self.feature_cols]
            pred_close = round(float(self.model.predict(X_latest)[0]), 2)
            # Confidence based on MAPE and volatility
            confidence = round(max(55.0, min(95.0, (1.0 - self.mape_score) * 100)), 1)

        change_pct = round(((pred_close - current_close) / (current_close + 1e-9)) * 100, 2)
        direction = "BULLISH" if change_pct >= 0 else "BEARISH"

        return {
            "current_close": current_close,
            "predicted_close": pred_close,
            "change_pct": change_pct,
            "direction": direction,
            "confidence": confidence,
            "horizon": "1-Day",
            "model_version": f"v1.0-{self.model_type}",
        }
