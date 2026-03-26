"""
model_manager.py
────────────────
Manages model versioning, retraining decisions, and the model registry.

Strategy (two-level):
  Level 1 — Immediate bias correction on each tick (fast, no full retrain)
  Level 2 — Periodic full XGBoost retrain every RETRAIN_EVERY_N_DAYS
             or when drift threshold is exceeded

No unsafe weight hacking. Retrain is a clean rebuild on expanded history.
"""

import os
import json
import logging
import pickle
import sqlite3
import threading
from datetime import datetime

import pandas as pd
import numpy as np

try:
    from sklearn.ensemble import GradientBoostingRegressor
    _SKLEARN_AVAILABLE = True
except ImportError:
    _SKLEARN_AVAILABLE = False

logger = logging.getLogger("model_manager")
_lock = threading.Lock()

DB_NAME             = "runtime.db"
MODEL_DIR           = "models"
REGISTRY_FILE       = "model_registry.json"

RETRAIN_EVERY_N_DAYS = 7      # retrain every 7 simulated days
DRIFT_RETRAIN_THRESHOLD = 8.0  # force retrain if drift exceeds this


def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


class ModelManager:
    def __init__(self, data_dir: str):
        self.data_dir   = data_dir
        self.db_path    = os.path.join(data_dir, DB_NAME)
        self.model_dir  = os.path.join(data_dir, MODEL_DIR)
        self.registry_path = os.path.join(data_dir, REGISTRY_FILE)
        os.makedirs(self.model_dir, exist_ok=True)
        self._ensure_registry()

    # ── Registry ─────────────────────────────────────────────────────────────
    def _ensure_registry(self):
        if not os.path.exists(self.registry_path):
            registry = {
                "current_version": "v1.0",
                "versions": {
                    "v1.0": {
                        "created_ts": datetime.utcnow().isoformat(),
                        "rows_seen": 0,
                        "rolling_mae": 0.0,
                        "retrain_reason": "initial",
                    }
                }
            }
            with open(self.registry_path, "w") as f:
                json.dump(registry, f, indent=2)

    def _load_registry(self) -> dict:
        with open(self.registry_path, "r") as f:
            return json.load(f)

    def _save_registry(self, registry: dict):
        tmp = self.registry_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(registry, f, indent=2)
        os.replace(tmp, self.registry_path)

    def get_current_version(self) -> str:
        try:
            return self._load_registry()["current_version"]
        except Exception:
            return "v1.0"

    # ── Retrain decision ──────────────────────────────────────────────────────
    def should_retrain(self, sim_day: int, drift_score: float) -> bool:
        """
        Returns True if retraining should be triggered this tick.
        Conditions:
          - sim_day is a multiple of RETRAIN_EVERY_N_DAYS
          - OR drift exceeds DRIFT_RETRAIN_THRESHOLD
        """
        if sim_day > 0 and sim_day % RETRAIN_EVERY_N_DAYS == 0:
            logger.info(f"Retrain: periodic trigger at sim_day={sim_day}")
            return True
        if drift_score > DRIFT_RETRAIN_THRESHOLD:
            logger.info(f"Retrain: drift threshold exceeded ({drift_score:.3f} > {DRIFT_RETRAIN_THRESHOLD})")
            return True
        return False

    # ── Retrain ───────────────────────────────────────────────────────────────
    def retrain(self, new_actuals: pd.DataFrame) -> str:
        """
        Retrain the short-horizon demand model with all available actuals.
        Returns the new model version string.
        """
        old_version = self.get_current_version()

        # Load all historical actuals from SQLite
        with _lock:
            conn = _get_conn(self.db_path)
            history = pd.read_sql_query(
                "SELECT * FROM actuals_log ORDER BY date, entity_id", conn
            )
            conn.close()

        # Merge in newest actuals if not yet committed
        if not new_actuals.empty:
            new_actuals_slim = new_actuals[[
                "date", "entity_id", "demand_index", "price_index",
                "sentiment_index", "search_index", "ad_index"
            ]].copy()
            history = pd.concat([history, new_actuals_slim], ignore_index=True).drop_duplicates(
                subset=["date", "entity_id"]
            )

        features = ["price_index", "sentiment_index", "search_index", "ad_index"]
        target   = "demand_index"

        train_df = history.dropna(subset=features + [target])

        if len(train_df) < 5:
            logger.warning("Not enough data to retrain — skipping")
            return old_version

        X = train_df[features].values
        y = train_df[target].values

        # New version tag
        v_num = float(old_version.replace("v", "")) + 0.1
        new_version = f"v{v_num:.1f}"

        if _SKLEARN_AVAILABLE:
            model = GradientBoostingRegressor(
                n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42
            )
            model.fit(X, y)

            # Compute rolling MAE on training data (in-sample proxy)
            preds = model.predict(X)
            mae = float(np.mean(np.abs(y - preds)))

            # Save model
            model_path = os.path.join(self.model_dir, f"demand_model_{new_version}.pkl")
            with open(model_path, "wb") as f:
                pickle.dump(model, f)
        else:
            # Fallback: simple mean model when sklearn not available
            mae = float(np.std(y))
            logger.warning("sklearn not available — using stub model")

        # Update registry
        registry = self._load_registry()
        registry["current_version"] = new_version
        registry["versions"][new_version] = {
            "created_ts": datetime.utcnow().isoformat(),
            "rows_seen": len(train_df),
            "rolling_mae": round(mae, 4),
            "retrain_reason": "periodic_or_drift",
        }
        self._save_registry(registry)

        # Log retrain job to SQLite
        self._log_retrain_job(old_version, new_version, mae)

        logger.info(f"✅ Retrained: {old_version} → {new_version} | MAE={mae:.3f} | rows={len(train_df)}")
        return new_version

    def _log_retrain_job(self, old_version: str, new_version: str, mae: float):
        with _lock:
            conn = _get_conn(self.db_path)
            sim_day_row = conn.execute(
                "SELECT value FROM autonomous_state WHERE key = 'sim_day'"
            ).fetchone()
            sim_day = int(sim_day_row["value"]) if sim_day_row else 0

            conn.execute(
                """INSERT INTO retrain_jobs
                   (sim_day, trigger, drift_score, old_version, new_version, ts)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (sim_day, "periodic_or_drift", mae, old_version, new_version,
                 datetime.utcnow().isoformat())
            )
            conn.commit()
            conn.close()

    # ── Predict (bias-corrected) ──────────────────────────────────────────────
    def predict_demand(
        self,
        price_index: float,
        sentiment_index: float,
        search_index: float,
        ad_index: float,
        bias_correction: float = 0.0,
    ) -> float:
        """
        Quick prediction using current model.
        bias_correction = Level-1 fast adaptation from recent residuals.
        """
        current_version = self.get_current_version()
        model_path = os.path.join(
            self.model_dir, f"demand_model_{current_version}.pkl"
        )

        try:
            if os.path.exists(model_path) and _SKLEARN_AVAILABLE:
                with open(model_path, "rb") as f:
                    model = pickle.load(f)
                X = [[price_index, sentiment_index, search_index, ad_index]]
                pred = float(model.predict(X)[0])
            else:
                # Naive fallback: weighted mean of features
                pred = (search_index * 0.4 + ad_index * 0.3 +
                        sentiment_index * 100 * 0.2 + 30 * 0.1)
        except Exception as e:
            logger.warning(f"Predict fallback due to: {e}")
            pred = search_index * 0.5 + 25.0

        return round(pred + bias_correction, 4)

    def get_model_status(self) -> dict:
        """Return current model metadata for the API."""
        try:
            registry = self._load_registry()
            ver = registry["current_version"]
            info = registry["versions"].get(ver, {})
            return {
                "current_version": ver,
                "created_ts": info.get("created_ts"),
                "rows_seen": info.get("rows_seen", 0),
                "rolling_mae": info.get("rolling_mae", 0.0),
                "retrain_reason": info.get("retrain_reason"),
                "all_versions": list(registry["versions"].keys()),
            }
        except Exception as e:
            return {"error": str(e)}