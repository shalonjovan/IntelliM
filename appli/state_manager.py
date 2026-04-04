"""
state_manager.py
────────────────
Manages all runtime state in SQLite.
Lightweight, autonomous-write-safe, no CSV dependency for state.

Tables:
  autonomous_state    — current sim pointer, model version, drift
  predictions_log     — forecasts emitted before actuals arrive
  actuals_log         — actual rows ingested from query.csv
  forecast_vs_actual  — per-row comparison store
  drift_summary       — rolling drift snapshots per tick
  model_registry      — model version history
  retrain_jobs        — record of each retraining event
"""

import os
import sqlite3
import threading
from datetime import datetime
from typing import Any

import db_tables

DB_NAME = "runtime.db"

# Thread-local connections for safety in async context
_lock = threading.Lock()


def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe concurrent writes
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


class StateManager:
    def __init__(self, data_dir: str):
        self.db_path = os.path.join(data_dir, DB_NAME)
        self._init_schema()
        self._ensure_defaults()

    # ── Schema Init ──────────────────────────────────────────────────────────
    def _init_schema(self):
        with _lock:
            conn = _get_conn(self.db_path)
            conn.executescript("""
                -- Current simulation pointer
                CREATE TABLE IF NOT EXISTS autonomous_state (
                    key     TEXT PRIMARY KEY,
                    value   TEXT,
                    updated TEXT
                );

                -- Forecasts made before actuals arrive
                CREATE TABLE IF NOT EXISTS predictions_log (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    prediction_ts   TEXT,
                    target_date     TEXT,
                    entity_id       INTEGER,
                    pred_demand     REAL,
                    pred_price      REAL,
                    model_version   TEXT
                );

                -- Actuals ingested from query.csv
                CREATE TABLE IF NOT EXISTS actuals_log (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingested_ts     TEXT,
                    date            TEXT,
                    entity_id       INTEGER,
                    brand           TEXT,
                    category        TEXT,
                    demand_index    REAL,
                    price_index     REAL,
                    sentiment_index REAL,
                    search_index    REAL,
                    ad_index        REAL,
                    event_type      TEXT,
                    event_title     TEXT,
                    impact_direction TEXT,
                    event_effect_hint TEXT
                );

                -- Forecast vs Actual comparison
                CREATE TABLE IF NOT EXISTS forecast_vs_actual (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    date            TEXT,
                    entity_id       INTEGER,
                    pred_demand     REAL,
                    actual_demand   REAL,
                    demand_error    REAL,
                    abs_demand_error REAL,
                    pred_price      REAL,
                    actual_price    REAL,
                    price_error     REAL,
                    abs_price_error REAL,
                    event_type      TEXT,
                    event_effect_hint TEXT,
                    model_version   TEXT,
                    ts              TEXT
                );

                -- Rolling drift snapshots per tick
                CREATE TABLE IF NOT EXISTS drift_summary (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    sim_day         INTEGER,
                    date            TEXT,
                    rolling_mae_demand  REAL,
                    rolling_mae_price   REAL,
                    combined_drift      REAL,
                    drift_flag          INTEGER,
                    ts              TEXT
                );

                -- Model version registry
                CREATE TABLE IF NOT EXISTS model_registry (
                    version         TEXT PRIMARY KEY,
                    created_ts      TEXT,
                    rows_seen       INTEGER,
                    rolling_mae     REAL,
                    retrain_reason  TEXT
                );

                -- Retrain job log
                CREATE TABLE IF NOT EXISTS retrain_jobs (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    sim_day         INTEGER,
                    trigger         TEXT,
                    drift_score     REAL,
                    old_version     TEXT,
                    new_version     TEXT,
                    ts              TEXT
                );
            """)
            conn.commit()
            conn.close()
            
        # Also initialize the serving tables explicitly
        db_tables.init_serving_tables(os.path.dirname(self.db_path))

    def _ensure_defaults(self):
        """Seed default state if first run."""
        defaults = {
            "sim_day": "0",
            "status": "idle",
            "latest_ingested_date": "",
            "model_version": "v1.0",
            "last_retrain_sim_day": "0",
            "last_retrain_ts": "",
            "current_drift": "0.0",
            "rows_processed": "0",
        }
        with _lock:
            conn = _get_conn(self.db_path)
            for k, v in defaults.items():
                conn.execute(
                    "INSERT OR IGNORE INTO autonomous_state (key, value, updated) VALUES (?, ?, ?)",
                    (k, v, datetime.utcnow().isoformat())
                )
            conn.commit()
            conn.close()

    # ── State accessors ───────────────────────────────────────────────────────
    def get_field(self, key: str, default: Any = None) -> Any:
        with _lock:
            conn = _get_conn(self.db_path)
            row = conn.execute(
                "SELECT value FROM autonomous_state WHERE key = ?", (key,)
            ).fetchone()
            conn.close()
        return row["value"] if row else default

    def set_field(self, key: str, value: Any):
        with _lock:
            conn = _get_conn(self.db_path)
            conn.execute(
                """INSERT INTO autonomous_state (key, value, updated)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated=excluded.updated""",
                (str(key), str(value), datetime.utcnow().isoformat())
            )
            conn.commit()
            conn.close()

    def increment_sim_day(self) -> int:
        with _lock:
            conn = _get_conn(self.db_path)
            row = conn.execute(
                "SELECT value FROM autonomous_state WHERE key = 'sim_day'"
            ).fetchone()
            day = int(row["value"]) + 1 if row else 1
            conn.execute(
                """INSERT INTO autonomous_state (key, value, updated)
                   VALUES ('sim_day', ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated=excluded.updated""",
                (str(day), datetime.utcnow().isoformat())
            )
            conn.commit()
            conn.close()
        return day

    def update_tick(
        self,
        sim_day: int,
        latest_date: str,
        drift_score: float,
        model_version: str,
        retrained: bool,
        rows_ingested: int,
    ):
        """Single call to update all state fields after a tick."""
        fields = {
            "sim_day": sim_day,
            "status": "running",
            "latest_ingested_date": latest_date,
            "model_version": model_version,
            "current_drift": round(drift_score, 6),
            "last_retrain_sim_day": self.get_field("last_retrain_sim_day", 0),
            "rows_processed": int(self.get_field("rows_processed", 0)) + rows_ingested,
            "retrained_this_tick": str(retrained),
        }
        for k, v in fields.items():
            self.set_field(k, v)

    def get_all_state(self) -> dict:
        """Return full state snapshot as dict."""
        with _lock:
            conn = _get_conn(self.db_path)
            rows = conn.execute("SELECT key, value, updated FROM autonomous_state").fetchall()
            conn.close()
        return {r["key"]: {"value": r["value"], "updated": r["updated"]} for r in rows}

    # ── Predictions log ───────────────────────────────────────────────────────
    def log_prediction(self, target_date: str, entity_id: int,
                       pred_demand: float, pred_price: float, model_version: str):
        with _lock:
            conn = _get_conn(self.db_path)
            conn.execute(
                """INSERT INTO predictions_log
                   (prediction_ts, target_date, entity_id, pred_demand, pred_price, model_version)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (datetime.utcnow().isoformat(), target_date, entity_id,
                 pred_demand, pred_price, model_version)
            )
            conn.commit()
            conn.close()

    def get_predictions_for_date(self, target_date: str) -> list[dict]:
        """Retrieve all predictions made for a given target date."""
        with _lock:
            conn = _get_conn(self.db_path)
            rows = conn.execute(
                "SELECT * FROM predictions_log WHERE target_date = ?", (target_date,)
            ).fetchall()
            conn.close()
        return [dict(r) for r in rows]

    def get_db_path(self) -> str:
        return self.db_path