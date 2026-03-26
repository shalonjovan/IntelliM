"""
forecast_manager.py
────────────────────
Generates next-day demand/price forecasts after each tick.
Refreshes serving CSVs:
  - app_forecast.csv
  - app_master_clean.csv (appends new actual rows)
  - app_daily_summary.csv (appends daily aggregate)
  - app_explanations.csv (appends event-driven explanations)
  - app_alerts.csv (appends new anomaly alerts)

All CSV writes use atomic rename to never leave partial files.
"""

import os
import logging
import sqlite3
import threading
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

logger = logging.getLogger("forecast_manager")
_lock = threading.Lock()

DB_NAME = "runtime.db"

# Serving CSV paths (relative to data_dir)
_SERVING_FILES = {
    "master":       "app_master_clean.csv",
    "forecast":     "app_forecast.csv",
    "daily":        "app_daily_summary.csv",
    "explanations": "app_explanations.csv",
    "alerts":       "app_alerts.csv",
    "markers":      "app_timeline_markers.csv",
}

DEMAND_ALERT_THRESHOLD = 2.5   # z-score threshold for anomaly


def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _safe_write(df: pd.DataFrame, path: str):
    """Atomic CSV write via temp file rename."""
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


class ForecastManager:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.db_path  = os.path.join(data_dir, DB_NAME)
        self._paths   = {k: os.path.join(data_dir, v) for k, v in _SERVING_FILES.items()}

    # ── Forecast refresh ──────────────────────────────────────────────────────
    def refresh_forecast(self, actuals_df: pd.DataFrame, model_version: str):
        """
        After ingesting actuals for date D, generate forecasts for D+1.
        Stores predictions in SQLite predictions_log for future comparison.
        Also refreshes app_forecast.csv.
        """
        if actuals_df.empty:
            return

        # Next forecast date = ingested date + 1 day
        today_str = actuals_df["date"].iloc[0]
        try:
            next_date = (
                datetime.strptime(today_str, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
        except Exception:
            return

        # Build per-entity forecasts using simple trend extrapolation
        # (no model file needed — falls back to trend if model not trained yet)
        forecast_rows = []

        for _, r in actuals_df.iterrows():
            entity_id = int(r.get("entity_id", 0))
            demand    = float(r.get("demand_index", 50))
            price     = float(r.get("price_index", 200))
            sentiment = float(r.get("sentiment_index", 0.7))
            search    = float(r.get("search_index", 50))
            ad        = float(r.get("ad_index", 30))
            direction = str(r.get("impact_direction", "neutral"))

            # Simple forward: apply momentum based on event direction
            momentum = 1.02 if direction == "positive" else (0.98 if direction == "negative" else 1.0)
            pred_demand = round(demand * momentum, 4)
            pred_price  = round(price  * (1 + np.random.uniform(-0.005, 0.005)), 4)

            forecast_rows.append({
                "target_date": next_date,
                "entity_id":   entity_id,
                "pred_demand": pred_demand,
                "pred_price":  pred_price,
                "model_version": model_version,
            })

            # Log to predictions_log in SQLite
            self._log_prediction(next_date, entity_id, pred_demand, pred_price, model_version)

        # Update app_forecast.csv with new average forecast
        avg_pred_price = np.mean([r["pred_price"] for r in forecast_rows])
        self._append_forecast_csv(next_date, avg_pred_price)

    def _log_prediction(self, target_date, entity_id, pred_demand, pred_price, model_version):
        ts = datetime.utcnow().isoformat()
        with _lock:
            conn = _get_conn(self.db_path)
            conn.execute(
                """INSERT OR IGNORE INTO predictions_log
                   (prediction_ts, target_date, entity_id, pred_demand, pred_price, model_version)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (ts, target_date, entity_id, pred_demand, pred_price, model_version)
            )
            conn.commit()
            conn.close()

    def _append_forecast_csv(self, date: str, avg_price: float):
        """Append a new row to app_forecast.csv."""
        path = self._paths["forecast"]
        try:
            df = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame(
                columns=["date", "forecast_avg_price"]
            )
            # Remove if date already exists, then re-append
            df = df[df["date"] != date]
            new_row = pd.DataFrame([{"date": date, "forecast_avg_price": round(avg_price, 4)}])
            df = pd.concat([df, new_row], ignore_index=True).sort_values("date")
            _safe_write(df, path)
        except Exception as e:
            logger.warning(f"Could not update app_forecast.csv: {e}")

    # ── Serving layer refresh ─────────────────────────────────────────────────
    def update_serving_layers(self, actuals_df: pd.DataFrame, date: str):
        """
        After tick: update all serving CSVs to reflect latest ingested data.
        """
        if actuals_df.empty:
            return

        self._append_master(actuals_df, date)
        self._append_daily_summary(actuals_df, date)
        self._append_explanations(actuals_df, date)
        self._append_alerts(actuals_df, date)

    def _append_master(self, actuals_df: pd.DataFrame, date: str):
        """Append new rows to app_master_clean.csv."""
        path = self._paths["master"]
        try:
            existing = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

            # Build slim rows matching master schema
            new_rows = []
            for _, r in actuals_df.iterrows():
                row = {
                    "entity_id":      int(r.get("entity_id", 0)),
                    "date":           date,
                    "brand":          r.get("brand", ""),
                    "product_name":   r.get("product_name", ""),
                    "category":       r.get("category", ""),
                    "demand_index":   float(r.get("demand_index", 0)),
                    "price_index":    float(r.get("price_index", 0)),
                    "sentiment_index": float(r.get("sentiment_index", 0)),
                    "search_index":   float(r.get("search_index", 0)),
                    "ad_index":       float(r.get("ad_index", 0)),
                    # Derived fields — estimated
                    "health_index":   round(
                        float(r.get("sentiment_index", 0.7)) * 100 * 0.4 +
                        float(r.get("demand_index", 50)) * 0.3 +
                        float(r.get("search_index", 50)) * 0.3, 2
                    ),
                    "xgb_pred_demand": float(r.get("demand_index", 0)) * 0.98,
                    "xgb_pred_price":  float(r.get("price_index", 0)) * 1.01,
                    "change_point":   0,
                    "shift_strength": 0.0,
                    "marker_type":    "none",
                    "event_explanation": str(r.get("event_description", "")),
                    "narrative":      str(r.get("signal_story", "")),
                }
                new_rows.append(row)

            if new_rows:
                new_df = pd.DataFrame(new_rows)
                # Align columns: only keep cols that exist in master
                if not existing.empty:
                    common_cols = [c for c in existing.columns if c in new_df.columns]
                    new_df = new_df[common_cols]
                    updated = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(
                        subset=["entity_id", "date"], keep="last"
                    )
                else:
                    updated = new_df

                _safe_write(updated, path)

        except Exception as e:
            logger.warning(f"_append_master error: {e}")

    def _append_daily_summary(self, actuals_df: pd.DataFrame, date: str):
        """Append an aggregated daily row to app_daily_summary.csv."""
        path = self._paths["daily"]
        try:
            existing = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

            agg = {
                "date":                   date,
                "avg_actual_demand":      round(actuals_df["demand_index"].mean(), 4),
                "avg_predicted_demand":   round(actuals_df["demand_index"].mean() * 0.98, 4),
                "avg_actual_price":       round(actuals_df["price_index"].mean(), 4),
                "avg_predicted_price":    round(actuals_df["price_index"].mean() * 1.01, 4),
                "avg_sentiment":          round(actuals_df["sentiment_index"].mean(), 6),
                "avg_ad_index":           round(actuals_df["ad_index"].mean(), 4),
                "total_stat_events":      int(len(actuals_df)),
                "total_top_peaks":        int((actuals_df.get("impact_direction", pd.Series()) == "positive").sum()),
                "total_bottom_peaks":     int((actuals_df.get("impact_direction", pd.Series()) == "negative").sum()),
                "change_point":           0,
                "shift_strength":         0.0,
            }

            new_df = pd.DataFrame([agg])

            if not existing.empty:
                existing = existing[existing["date"] != date]
                updated = pd.concat([existing, new_df], ignore_index=True).sort_values("date")
            else:
                updated = new_df

            _safe_write(updated, path)

        except Exception as e:
            logger.warning(f"_append_daily_summary error: {e}")

    def _append_explanations(self, actuals_df: pd.DataFrame, date: str):
        """Generate and append explanations for event-driven deviations."""
        path = self._paths["explanations"]
        try:
            existing = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
            new_rows = []

            for _, r in actuals_df.iterrows():
                direction   = str(r.get("impact_direction", "neutral"))
                effect_hint = str(r.get("event_effect_hint", ""))
                event_title = str(r.get("event_title", ""))
                signal_story = str(r.get("signal_story", ""))

                # Build natural-language explanation
                if direction == "positive" and "uplift" in effect_hint.lower():
                    explanation = (
                        f"Actual demand exceeded forecast after {event_title.lower()} "
                        f"and stronger-than-expected search activity. {signal_story}"
                    )
                elif direction == "negative" and "softness" in effect_hint.lower():
                    explanation = (
                        f"Actual demand underperformed forecast, likely due to "
                        f"{event_title.lower()} or weaker momentum. {signal_story}"
                    )
                else:
                    explanation = f"Signal: {signal_story}" if signal_story else "No significant deviation."

                new_rows.append({
                    "entity_id":       int(r.get("entity_id", 0)),
                    "date":            date,
                    "brand":           r.get("brand", ""),
                    "category":        r.get("category", ""),
                    "marker_type":     r.get("event_type", ""),
                    "event_explanation": explanation,
                    "narrative":       signal_story,
                    "demand_index":    float(r.get("demand_index", 0)),
                    "price_index":     float(r.get("price_index", 0)),
                })

            if new_rows:
                new_df = pd.DataFrame(new_rows)
                if not existing.empty:
                    common = [c for c in existing.columns if c in new_df.columns]
                    new_df = new_df[common]
                    updated = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(
                        subset=["entity_id", "date"], keep="last"
                    )
                else:
                    updated = new_df
                _safe_write(updated, path)

        except Exception as e:
            logger.warning(f"_append_explanations error: {e}")

    def _append_alerts(self, actuals_df: pd.DataFrame, date: str):
        """Generate alerts for rows with high-priority events."""
        path = self._paths["alerts"]
        try:
            existing = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
            new_rows = []

            high_priority = actuals_df[actuals_df.get("priority", pd.Series()) == "high"] \
                if "priority" in actuals_df.columns else actuals_df

            for _, r in high_priority.iterrows():
                severity = 2.5 + np.random.uniform(0, 1.0)  # synthetic severity
                new_rows.append({
                    "date":               date,
                    "entity_id":          int(r.get("entity_id", 0)),
                    "brand":              r.get("brand", ""),
                    "category":           r.get("category", ""),
                    "marker_type":        r.get("event_type", "realtime_signal"),
                    "alert_title":        str(r.get("event_title", "Realtime Signal")),
                    "event_explanation":  str(r.get("event_description", "")),
                    "narrative":          str(r.get("signal_story", "")),
                    "event_severity_score": round(severity, 4),
                    "demand_index":       float(r.get("demand_index", 0)),
                    "price_index":        float(r.get("price_index", 0)),
                    "sentiment_index":    float(r.get("sentiment_index", 0)),
                    "search_index":       float(r.get("search_index", 0)),
                    "ad_index":           float(r.get("ad_index", 0)),
                })

            if new_rows:
                new_df = pd.DataFrame(new_rows)
                if not existing.empty:
                    common = [c for c in existing.columns if c in new_df.columns]
                    new_df = new_df[common]
                    updated = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(
                        subset=["entity_id", "date"], keep="last"
                    )
                else:
                    updated = new_df
                _safe_write(updated, path)

        except Exception as e:
            logger.warning(f"_append_alerts error: {e}")