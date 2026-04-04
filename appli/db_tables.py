"""
db_tables.py
────────────
SQLite serving-layer tables and helpers.

All serving data (master, daily summary, forecast, alerts,
explanations, timeline markers, regime shifts, events, query queue)
lives in runtime.db alongside the existing runtime tables.
"""

import os
import sqlite3
import threading
from typing import Any

DB_NAME = "runtime.db"
_lock = threading.Lock()


# ── Connection helper ────────────────────────────────────────────────────────

def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _db_path(data_dir: str) -> str:
    return os.path.join(data_dir, DB_NAME)


# ── Schema initialisation ───────────────────────────────────────────────────

def init_serving_tables(data_dir: str):
    """Create all serving tables if they don't exist."""
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS serving_master (
                entity_id       INTEGER,
                date            TEXT,
                brand           TEXT,
                product_name    TEXT,
                category        TEXT,
                demand_index    REAL,
                price_index     REAL,
                sentiment_index REAL,
                search_index    REAL,
                ad_index        REAL,
                health_index    REAL,
                list_price      REAL,
                xgb_pred_demand REAL,
                xgb_pred_price  REAL,
                change_point    INTEGER DEFAULT 0,
                shift_strength  REAL DEFAULT 0.0,
                marker_type     TEXT DEFAULT 'none',
                event_explanation TEXT DEFAULT '',
                narrative       TEXT DEFAULT '',
                PRIMARY KEY (entity_id, date)
            );

            CREATE TABLE IF NOT EXISTS serving_daily_summary (
                date                TEXT PRIMARY KEY,
                avg_actual_demand   REAL,
                avg_predicted_demand REAL,
                avg_actual_price    REAL,
                avg_predicted_price REAL,
                avg_sentiment       REAL,
                avg_ad_index        REAL,
                total_stat_events   INTEGER,
                total_top_peaks     INTEGER,
                total_bottom_peaks  INTEGER,
                change_point        INTEGER DEFAULT 0,
                shift_strength      REAL DEFAULT 0.0
            );

            CREATE TABLE IF NOT EXISTS serving_forecast (
                date              TEXT PRIMARY KEY,
                forecast_avg_price REAL
            );

            CREATE TABLE IF NOT EXISTS serving_alerts (
                entity_id           INTEGER,
                date                TEXT,
                brand               TEXT,
                category            TEXT,
                marker_type         TEXT,
                alert_title         TEXT,
                event_explanation   TEXT,
                narrative           TEXT,
                event_severity_score REAL,
                demand_index        REAL,
                price_index         REAL,
                sentiment_index     REAL,
                search_index        REAL,
                ad_index            REAL,
                PRIMARY KEY (entity_id, date)
            );

            CREATE TABLE IF NOT EXISTS serving_explanations (
                entity_id           INTEGER,
                date                TEXT,
                brand               TEXT,
                category            TEXT,
                marker_type         TEXT,
                event_explanation   TEXT,
                narrative           TEXT,
                demand_index        REAL,
                price_index         REAL,
                PRIMARY KEY (entity_id, date)
            );

            CREATE TABLE IF NOT EXISTS serving_timeline_markers (
                entity_id               INTEGER,
                date                    TEXT,
                marker_type             TEXT,
                marker_label            TEXT,
                event_severity_score    REAL,
                combined_marker_severity REAL,
                shift_strength          REAL,
                demand_index            REAL,
                price_index             REAL,
                sentiment_index         REAL,
                search_index            REAL,
                ad_index                REAL,
                event_explanation       TEXT,
                narrative               TEXT,
                PRIMARY KEY (entity_id, date, marker_type)
            );

            CREATE TABLE IF NOT EXISTS serving_regime_shifts (
                date              TEXT PRIMARY KEY,
                regime_type       TEXT,
                shift_strength    REAL,
                marker_label      TEXT,
                event_explanation TEXT,
                narrative         TEXT
            );

            CREATE TABLE IF NOT EXISTS serving_events (
                entity_id           INTEGER,
                date                TEXT,
                brand               TEXT,
                product_name        TEXT,
                event_type          TEXT,
                event_title         TEXT,
                event_description   TEXT,
                impact_direction    TEXT,
                priority            TEXT,
                linked_marker_date  TEXT,
                linked_marker_type  TEXT,
                signal_story        TEXT,
                PRIMARY KEY (entity_id, date, event_type)
            );

            CREATE TABLE IF NOT EXISTS serving_query_queue (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT,
                entity_id       INTEGER,
                brand           TEXT,
                category        TEXT,
                product_name    TEXT,
                demand_index    REAL,
                price_index     REAL,
                sentiment_index REAL,
                search_index    REAL,
                ad_index        REAL,
                event_type      TEXT,
                event_title     TEXT,
                event_description TEXT,
                impact_direction TEXT,
                event_effect_hint TEXT,
                priority        TEXT,
                signal_story    TEXT,
                UNIQUE(entity_id, date)
            );

            -- Indexes for common query patterns
            CREATE INDEX IF NOT EXISTS idx_master_entity   ON serving_master(entity_id);
            CREATE INDEX IF NOT EXISTS idx_master_category ON serving_master(category);
            CREATE INDEX IF NOT EXISTS idx_master_brand    ON serving_master(brand);
            CREATE INDEX IF NOT EXISTS idx_alerts_entity   ON serving_alerts(entity_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_brand    ON serving_alerts(brand);
            CREATE INDEX IF NOT EXISTS idx_markers_entity  ON serving_timeline_markers(entity_id);
            CREATE INDEX IF NOT EXISTS idx_events_entity   ON serving_events(entity_id);
            CREATE INDEX IF NOT EXISTS idx_expl_entity     ON serving_explanations(entity_id);
            CREATE INDEX IF NOT EXISTS idx_query_date      ON serving_query_queue(date);
        """)
        conn.commit()
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# READ HELPERS — return list[dict] ready for JSON serialisation
# ══════════════════════════════════════════════════════════════════════════════

def _rows_to_dicts(rows) -> list[dict]:
    return [dict(r) for r in rows]


def _none_for_nan(d: dict) -> dict:
    """Replace NaN / 'nan' strings with None for JSON safety."""
    for k, v in d.items():
        if v != v:  # NaN check
            d[k] = None
        elif isinstance(v, str) and v.lower() == "nan":
            d[k] = None
    return d


# ── Master ────────────────────────────────────────────────────────────────────

def query_master_full(data_dir: str) -> list[dict]:
    """All master rows (used sparingly at startup summaries)."""
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute("SELECT * FROM serving_master ORDER BY date").fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


def query_master_latest_per_entity(data_dir: str) -> list[dict]:
    """Latest row per entity — equivalent to the old `latest_master`."""
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute("""
            SELECT * FROM serving_master
            WHERE (entity_id, date) IN (
                SELECT entity_id, MAX(date) FROM serving_master GROUP BY entity_id
            )
            ORDER BY entity_id
        """).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


def query_master_by_entity(data_dir: str, entity_id: int) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute(
            "SELECT * FROM serving_master WHERE entity_id = ? ORDER BY date",
            (entity_id,)
        ).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


def query_master_latest_one(data_dir: str, entity_id: int) -> dict | None:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        row = conn.execute(
            "SELECT * FROM serving_master WHERE entity_id = ? ORDER BY date DESC LIMIT 1",
            (entity_id,)
        ).fetchone()
        conn.close()
    return _none_for_nan(dict(row)) if row else None


def query_filters(data_dir: str) -> dict:
    """Distinct categories, brands, and entity list for dropdowns."""
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        categories = [r[0] for r in conn.execute(
            "SELECT DISTINCT category FROM serving_master ORDER BY category"
        ).fetchall()]
        brands = [r[0] for r in conn.execute(
            "SELECT DISTINCT brand FROM serving_master ORDER BY brand"
        ).fetchall()]
        # Latest row per entity for the entity picker
        entities = conn.execute("""
            SELECT entity_id, brand, product_name, category
            FROM serving_master
            WHERE (entity_id, date) IN (
                SELECT entity_id, MAX(date) FROM serving_master GROUP BY entity_id
            )
            ORDER BY brand
        """).fetchall()
        conn.close()
    return {
        "categories": categories,
        "brands": brands,
        "entities": [dict(r) for r in entities],
    }


def query_master_by_category(data_dir: str, category: str) -> list[dict]:
    """Latest row per entity in a category."""
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute("""
            SELECT * FROM serving_master
            WHERE LOWER(category) = LOWER(?)
              AND (entity_id, date) IN (
                  SELECT entity_id, MAX(date) FROM serving_master GROUP BY entity_id
              )
            ORDER BY health_index DESC
        """, (category,)).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


def query_unique_counts(data_dir: str) -> dict:
    """Quick entity/brand counts for startup banner."""
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        total = conn.execute("SELECT COUNT(*) FROM serving_master").fetchone()[0]
        entities = conn.execute("SELECT COUNT(DISTINCT entity_id) FROM serving_master").fetchone()[0]
        brands = conn.execute("SELECT COUNT(DISTINCT brand) FROM serving_master").fetchone()[0]
        conn.close()
    return {"total_rows": total, "entities": entities, "brands": brands}


# ── Daily summary ─────────────────────────────────────────────────────────────

def query_daily_summary(data_dir: str) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute("SELECT * FROM serving_daily_summary ORDER BY date").fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


# ── Forecast ──────────────────────────────────────────────────────────────────

def query_forecast(data_dir: str) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute(
            "SELECT date, forecast_avg_price FROM serving_forecast ORDER BY date"
        ).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


# ── Alerts ────────────────────────────────────────────────────────────────────

def query_alerts(data_dir: str, limit: int = 20, brand: str | None = None) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        if brand:
            rows = conn.execute(
                """SELECT * FROM serving_alerts
                   WHERE LOWER(brand) = LOWER(?)
                   ORDER BY event_severity_score DESC LIMIT ?""",
                (brand, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM serving_alerts ORDER BY event_severity_score DESC LIMIT ?",
                (limit,)
            ).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


def query_product_alerts(data_dir: str, entity_id: int, limit: int = 10) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute(
            """SELECT * FROM serving_alerts
               WHERE entity_id = ?
               ORDER BY event_severity_score DESC LIMIT ?""",
            (entity_id, limit)
        ).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


# ── Explanations ──────────────────────────────────────────────────────────────

def query_explanations_for(data_dir: str, entity_id: int, date: str) -> dict | None:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        row = conn.execute(
            "SELECT * FROM serving_explanations WHERE entity_id = ? AND date = ?",
            (entity_id, date)
        ).fetchone()
        conn.close()
    return _none_for_nan(dict(row)) if row else None


# ── Timeline markers ─────────────────────────────────────────────────────────

def query_product_markers(data_dir: str, entity_id: int) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute(
            "SELECT * FROM serving_timeline_markers WHERE entity_id = ?",
            (entity_id,)
        ).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


# ── Regime shifts ─────────────────────────────────────────────────────────────

def query_regime_shifts(data_dir: str) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute(
            "SELECT * FROM serving_regime_shifts ORDER BY date"
        ).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


# ── Events ────────────────────────────────────────────────────────────────────

def query_product_events(data_dir: str, entity_id: int) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute(
            "SELECT * FROM serving_events WHERE entity_id = ?",
            (entity_id,)
        ).fetchall()
        conn.close()
    return [_none_for_nan(dict(r)) for r in rows]


# ── Query queue ───────────────────────────────────────────────────────────────

def query_queue_dates(data_dir: str) -> list[str]:
    """Return sorted unique dates in the queue."""
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute(
            "SELECT DISTINCT date FROM serving_query_queue ORDER BY date"
        ).fetchall()
        conn.close()
    return [r[0] for r in rows]


def query_queue_rows_for_date(data_dir: str, date: str) -> list[dict]:
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        rows = conn.execute(
            "SELECT * FROM serving_query_queue WHERE date = ?", (date,)
        ).fetchall()
        conn.close()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# WRITE HELPERS — upsert rows into serving tables
# ══════════════════════════════════════════════════════════════════════════════

def upsert_master_rows(data_dir: str, rows: list[dict]):
    """Insert or replace master rows keyed on (entity_id, date)."""
    if not rows:
        return
    db = _db_path(data_dir)
    cols = [
        "entity_id", "date", "brand", "product_name", "category",
        "demand_index", "price_index", "sentiment_index", "search_index", "ad_index",
        "health_index", "list_price", "xgb_pred_demand", "xgb_pred_price",
        "change_point", "shift_strength", "marker_type", "event_explanation", "narrative",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    with _lock:
        conn = _get_conn(db)
        conn.executemany(
            f"INSERT OR REPLACE INTO serving_master ({col_names}) VALUES ({placeholders})",
            [tuple(r.get(c) for c in cols) for r in rows]
        )
        conn.commit()
        conn.close()


def upsert_daily_summary(data_dir: str, row: dict):
    """Insert or replace a single daily summary row."""
    db = _db_path(data_dir)
    cols = [
        "date", "avg_actual_demand", "avg_predicted_demand",
        "avg_actual_price", "avg_predicted_price",
        "avg_sentiment", "avg_ad_index",
        "total_stat_events", "total_top_peaks", "total_bottom_peaks",
        "change_point", "shift_strength",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    with _lock:
        conn = _get_conn(db)
        conn.execute(
            f"INSERT OR REPLACE INTO serving_daily_summary ({col_names}) VALUES ({placeholders})",
            tuple(row.get(c) for c in cols)
        )
        conn.commit()
        conn.close()


def upsert_forecast_row(data_dir: str, date: str, forecast_avg_price: float):
    db = _db_path(data_dir)
    with _lock:
        conn = _get_conn(db)
        conn.execute(
            "INSERT OR REPLACE INTO serving_forecast (date, forecast_avg_price) VALUES (?, ?)",
            (date, forecast_avg_price)
        )
        conn.commit()
        conn.close()


def upsert_alert_rows(data_dir: str, rows: list[dict]):
    if not rows:
        return
    db = _db_path(data_dir)
    cols = [
        "entity_id", "date", "brand", "category", "marker_type",
        "alert_title", "event_explanation", "narrative",
        "event_severity_score", "demand_index", "price_index",
        "sentiment_index", "search_index", "ad_index",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    with _lock:
        conn = _get_conn(db)
        conn.executemany(
            f"INSERT OR REPLACE INTO serving_alerts ({col_names}) VALUES ({placeholders})",
            [tuple(r.get(c) for c in cols) for r in rows]
        )
        conn.commit()
        conn.close()


def upsert_explanation_rows(data_dir: str, rows: list[dict]):
    if not rows:
        return
    db = _db_path(data_dir)
    cols = [
        "entity_id", "date", "brand", "category", "marker_type",
        "event_explanation", "narrative", "demand_index", "price_index",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    with _lock:
        conn = _get_conn(db)
        conn.executemany(
            f"INSERT OR REPLACE INTO serving_explanations ({col_names}) VALUES ({placeholders})",
            [tuple(r.get(c) for c in cols) for r in rows]
        )
        conn.commit()
        conn.close()


def upsert_query_queue_rows(data_dir: str, rows: list[dict]):
    """Bulk-insert query queue rows."""
    if not rows:
        return
    db = _db_path(data_dir)
    cols = [
        "date", "entity_id", "brand", "category", "product_name",
        "demand_index", "price_index", "sentiment_index", "search_index", "ad_index",
        "event_type", "event_title", "event_description",
        "impact_direction", "event_effect_hint", "priority", "signal_story",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    with _lock:
        conn = _get_conn(db)
        conn.executemany(
            f"INSERT OR IGNORE INTO serving_query_queue ({col_names}) VALUES ({placeholders})",
            [tuple(r.get(c) for c in cols) for r in rows]
        )
        conn.commit()
        conn.close()
