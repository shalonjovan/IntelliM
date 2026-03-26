"""
realtime_ingestor.py
────────────────────
Consumes query.csv one date at a time.
Maintains a pointer in SQLite so no date is ever re-consumed.
On each tick, ALL rows for the next chronological date are ingested at once.
"""

import os
import logging
import sqlite3
import threading
from datetime import datetime

import pandas as pd

logger = logging.getLogger("realtime_ingestor")
_lock = threading.Lock()

QUERY_FILE = "query.csv"
DB_NAME    = "runtime.db"


def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


class RealtimeIngestor:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.query_path = os.path.join(data_dir, QUERY_FILE)
        self.db_path    = os.path.join(data_dir, DB_NAME)
        self._query_df: pd.DataFrame | None = None
        self._sorted_dates: list[str] = []
        self._load_query()

    # ── Internal ─────────────────────────────────────────────────────────────
    def _load_query(self):
        """Load query.csv into memory once and sort dates."""
        if not os.path.exists(self.query_path):
            logger.warning(f"query.csv not found at {self.query_path}")
            self._query_df = pd.DataFrame()
            return
        df = pd.read_csv(self.query_path)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        self._query_df = df
        self._sorted_dates = sorted(df["date"].unique().tolist())
        logger.info(f"📂 Loaded query.csv — {len(df)} rows, {len(self._sorted_dates)} dates")

    def _get_pointer(self) -> str | None:
        """Return the last consumed date from SQLite state."""
        with _lock:
            conn = _get_conn(self.db_path)
            row = conn.execute(
                "SELECT value FROM autonomous_state WHERE key = 'latest_ingested_date'"
            ).fetchone()
            conn.close()
        return row["value"] if row and row["value"] else None

    # ── Public API ────────────────────────────────────────────────────────────
    def peek_next_date(self) -> str | None:
        """Return the next date to be ingested, without consuming it."""
        pointer = self._get_pointer()
        for d in self._sorted_dates:
            if pointer is None or d > pointer:
                return d
        return None

    def ingest_next_date(self) -> pd.DataFrame:
        """
        Consume all rows for the next available date.
        Returns the rows as a DataFrame and logs them to actuals_log.
        Returns empty DataFrame if nothing left.
        """
        next_date = self.peek_next_date()
        if next_date is None:
            return pd.DataFrame()

        rows = self._query_df[self._query_df["date"] == next_date].copy()
        if rows.empty:
            return pd.DataFrame()

        # Write to actuals_log in SQLite
        self._log_actuals(rows, next_date)

        logger.info(f"✅ Ingested {len(rows)} rows for date {next_date}")
        return rows

    def _log_actuals(self, df: pd.DataFrame, date: str):
        """Persist ingested rows to actuals_log table."""
        ts = datetime.utcnow().isoformat()
        rows_to_insert = []

        for _, r in df.iterrows():
            rows_to_insert.append((
                ts,
                date,
                int(r.get("entity_id", 0)),
                str(r.get("brand", "")),
                str(r.get("category", "")),
                float(r.get("demand_index", 0)),
                float(r.get("price_index", 0)),
                float(r.get("sentiment_index", 0)),
                float(r.get("search_index", 0)),
                float(r.get("ad_index", 0)),
                str(r.get("event_type", "")),
                str(r.get("event_title", "")),
                str(r.get("impact_direction", "")),
                str(r.get("event_effect_hint", "")),
            ))

        with _lock:
            conn = _get_conn(self.db_path)
            conn.executemany(
                """INSERT INTO actuals_log
                   (ingested_ts, date, entity_id, brand, category,
                    demand_index, price_index, sentiment_index, search_index, ad_index,
                    event_type, event_title, impact_direction, event_effect_hint)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows_to_insert
            )
            conn.commit()
            conn.close()

    def get_all_actuals(self) -> pd.DataFrame:
        """Return all logged actuals from SQLite."""
        with _lock:
            conn = _get_conn(self.db_path)
            df = pd.read_sql_query("SELECT * FROM actuals_log ORDER BY date, entity_id", conn)
            conn.close()
        return df

    def reload_query(self):
        """Hot-reload query.csv (e.g., if it was updated externally)."""
        self._load_query()
        logger.info("🔄 query.csv reloaded")