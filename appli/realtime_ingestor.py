"""
realtime_ingestor.py
────────────────────
Consumes market data one date at a time from SQLite query queue.
Maintains a pointer in SQLite so no date is ever re-consumed.
On each tick, ALL rows for the next chronological date are ingested at once.

On first init, loads query.csv into the serving_query_queue table if not
already populated, then reads exclusively from SQLite.
"""

import os
import logging
import sqlite3
import threading
from datetime import datetime

import pandas as pd

import db_tables

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
        self._sorted_dates: list[str] = []
        self._ensure_query_loaded()

    # ── Internal ─────────────────────────────────────────────────────────────
    def _ensure_query_loaded(self):
        """
        Ensure query data is in SQLite. If the serving_query_queue table
        is empty but query.csv exists, import it once.
        """
        db_tables.init_serving_tables(self.data_dir)
        existing_dates = db_tables.query_queue_dates(self.data_dir)

        if not existing_dates:
            # First run — seed from CSV
            if os.path.exists(self.query_path):
                df = pd.read_csv(self.query_path)
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                rows = []
                for _, r in df.iterrows():
                    row_dict = {}
                    for c in df.columns:
                        v = r[c]
                        row_dict[c] = None if (isinstance(v, float) and v != v) else v
                    rows.append(row_dict)
                db_tables.upsert_query_queue_rows(self.data_dir, rows)
                logger.info(f"📂 Seeded query queue from CSV — {len(df)} rows")
                existing_dates = db_tables.query_queue_dates(self.data_dir)
            else:
                logger.warning(f"query.csv not found at {self.query_path} and queue is empty")

        self._sorted_dates = existing_dates
        logger.info(f"📂 Query queue ready — {len(self._sorted_dates)} dates")

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

        # Read rows from SQLite queue
        row_dicts = db_tables.query_queue_rows_for_date(self.data_dir, next_date)
        if not row_dicts:
            return pd.DataFrame()

        rows = pd.DataFrame(row_dicts)

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
        """Re-import query.csv into the SQLite queue (e.g., if CSV was updated externally)."""
        if os.path.exists(self.query_path):
            df = pd.read_csv(self.query_path)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            rows = []
            for _, r in df.iterrows():
                row_dict = {}
                for c in df.columns:
                    v = r[c]
                    row_dict[c] = None if (isinstance(v, float) and v != v) else v
                rows.append(row_dict)
            db_tables.upsert_query_queue_rows(self.data_dir, rows)
            self._sorted_dates = db_tables.query_queue_dates(self.data_dir)
        logger.info("🔄 query queue reloaded from CSV")