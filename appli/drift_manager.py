"""
drift_manager.py
────────────────
Compares forecasts vs actuals, computes rolling MAE drift,
and writes results to SQLite.

Drift score formula:
  combined_drift = 0.5 × rolling_MAE(demand) + 0.5 × rolling_MAE(price_normalised)
"""

import os
import logging
import sqlite3
import threading
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger("drift_manager")
_lock = threading.Lock()

DB_NAME     = "runtime.db"

# Drift threshold — flag if combined drift exceeds this value
DRIFT_THRESHOLD = 5.0


def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


class DriftManager:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.db_path  = os.path.join(data_dir, DB_NAME)

    # ── Core comparison ───────────────────────────────────────────────────────
    def compare_and_log(self, actuals_df: pd.DataFrame, date: str):
        """
        Match actuals against predictions stored in predictions_log.
        Write per-row forecast_vs_actual entries to SQLite.
        """
        # Load predictions for this date from SQLite
        with _lock:
            conn = _get_conn(self.db_path)
            preds_df = pd.read_sql_query(
                "SELECT * FROM predictions_log WHERE target_date = ?",
                conn, params=(date,)
            )
            conn.close()

        if preds_df.empty:
            # No stored predictions — use XGBoost columns from master data as proxy
            logger.info(f"No stored predictions for {date}, using xgb fallback if available")
            self._log_actuals_only(actuals_df, date)
            return

        # Merge on entity_id
        merged = actuals_df.merge(
            preds_df[["entity_id", "pred_demand", "pred_price", "model_version"]],
            on="entity_id", how="left"
        )

        rows = []
        ts = datetime.utcnow().isoformat()

        for _, r in merged.iterrows():
            pred_demand = float(r.get("pred_demand", np.nan))
            actual_demand = float(r.get("demand_index", np.nan))
            pred_price  = float(r.get("pred_price", np.nan))
            actual_price = float(r.get("price_index", np.nan))

            d_error = actual_demand - pred_demand if not (np.isnan(pred_demand) or np.isnan(actual_demand)) else np.nan
            p_error = actual_price  - pred_price  if not (np.isnan(pred_price)  or np.isnan(actual_price))  else np.nan

            rows.append((
                date, int(r.get("entity_id", 0)),
                pred_demand, actual_demand,
                d_error, abs(d_error) if d_error == d_error else None,
                pred_price,  actual_price,
                p_error, abs(p_error) if p_error == p_error else None,
                str(r.get("event_type", "")),
                str(r.get("event_effect_hint", "")),
                str(r.get("model_version", "v1.0")),
                ts
            ))

        with _lock:
            conn = _get_conn(self.db_path)
            conn.executemany(
                """INSERT INTO forecast_vs_actual
                   (date, entity_id, pred_demand, actual_demand, demand_error, abs_demand_error,
                    pred_price, actual_price, price_error, abs_price_error,
                    event_type, event_effect_hint, model_version, ts)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows
            )
            conn.commit()
            conn.close()

    def _log_actuals_only(self, actuals_df: pd.DataFrame, date: str):
        """When no predictions exist, still log actuals with null predictions."""
        ts = datetime.utcnow().isoformat()
        rows = []
        for _, r in actuals_df.iterrows():
            rows.append((
                date, int(r.get("entity_id", 0)),
                None, float(r.get("demand_index", 0)),
                None, None,
                None, float(r.get("price_index", 0)),
                None, None,
                str(r.get("event_type", "")),
                str(r.get("event_effect_hint", "")),
                "v1.0", ts
            ))
        with _lock:
            conn = _get_conn(self.db_path)
            conn.executemany(
                """INSERT INTO forecast_vs_actual
                   (date, entity_id, pred_demand, actual_demand, demand_error, abs_demand_error,
                    pred_price, actual_price, price_error, abs_price_error,
                    event_type, event_effect_hint, model_version, ts)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows
            )
            conn.commit()
            conn.close()

    # ── Drift computation ────────────────────────────────────────────────────
    def compute_rolling_drift(self, window: int = 7) -> float:
        """
        Compute combined drift score over the last `window` comparison rows.
        Returns: combined_drift (float), 0.0 if insufficient data.
        """
        with _lock:
            conn = _get_conn(self.db_path)
            df = pd.read_sql_query(
                f"""SELECT abs_demand_error, abs_price_error, pred_demand, actual_demand
                    FROM forecast_vs_actual
                    WHERE abs_demand_error IS NOT NULL
                    ORDER BY id DESC LIMIT {window}""",
                conn
            )
            conn.close()

        if df.empty:
            return 0.0

        mae_demand = df["abs_demand_error"].mean()

        # Use raw demand MAE as primary drift signal (price lives on different scale)
        combined_drift = float(mae_demand)
        return round(combined_drift, 4)

    def is_drift_high(self, drift_score: float) -> bool:
        return drift_score > DRIFT_THRESHOLD

    def log_drift_snapshot(self, sim_day: int, date: str, drift_score: float):
        """Persist a drift summary row to SQLite."""
        mae_d = drift_score  # primary component

        with _lock:
            conn = _get_conn(self.db_path)
            conn.execute(
                """INSERT INTO drift_summary
                   (sim_day, date, rolling_mae_demand, rolling_mae_price,
                    combined_drift, drift_flag, ts)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (sim_day, date, mae_d, 0.0, drift_score,
                 int(self.is_drift_high(drift_score)),
                 datetime.utcnow().isoformat())
            )
            conn.commit()
            conn.close()

    def get_drift_history(self, limit: int = 30) -> list[dict]:
        """Return recent drift snapshots for the UI."""
        with _lock:
            conn = _get_conn(self.db_path)
            rows = conn.execute(
                """SELECT sim_day, date, combined_drift, drift_flag, ts
                   FROM drift_summary ORDER BY id DESC LIMIT ?""",
                (limit,)
            ).fetchall()
            conn.close()
        return [dict(r) for r in rows]

    def get_fva_recent(self, limit: int = 50) -> list[dict]:
        """Return recent forecast-vs-actual rows for API."""
        with _lock:
            conn = _get_conn(self.db_path)
            rows = conn.execute(
                """SELECT date, entity_id, pred_demand, actual_demand,
                          demand_error, abs_demand_error,
                          pred_price, actual_price, price_error,
                          event_type, event_effect_hint, model_version
                   FROM forecast_vs_actual ORDER BY id DESC LIMIT ?""",
                (limit,)
            ).fetchall()
            conn.close()
        return [dict(r) for r in rows]