"""
migrate_csv_to_db.py
────────────────────
One-time script to seed SQLite serving tables from existing CSV files.
Idempotent — uses INSERT OR REPLACE / INSERT OR IGNORE.

Usage:
    cd appli
    python migrate_csv_to_db.py
"""

import os
import sys
import math
import pandas as pd

# Ensure we can import local modules
sys.path.insert(0, os.path.dirname(__file__))

import db_tables

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def _safe_val(v):
    """Convert pandas NaN / numpy types to Python-native for SQLite."""
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(v, "item"):  # numpy scalar
        return v.item()
    return v


def _df_to_dicts(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame rows to list of dicts with safe values."""
    records = []
    for _, row in df.iterrows():
        records.append({k: _safe_val(v) for k, v in row.items()})
    return records


def migrate_master():
    path = os.path.join(DATA_DIR, "app_master_clean.csv")
    if not os.path.exists(path):
        print("⚠  app_master_clean.csv not found — skipping")
        return
    df = pd.read_csv(path)
    rows = _df_to_dicts(df)
    db_tables.upsert_master_rows(DATA_DIR, rows)
    print(f"✅ serving_master: {len(rows)} rows imported")


def migrate_daily_summary():
    path = os.path.join(DATA_DIR, "app_daily_summary.csv")
    if not os.path.exists(path):
        print("⚠  app_daily_summary.csv not found — skipping")
        return
    df = pd.read_csv(path)
    for _, row in df.iterrows():
        db_tables.upsert_daily_summary(DATA_DIR, {k: _safe_val(v) for k, v in row.items()})
    print(f"✅ serving_daily_summary: {len(df)} rows imported")


def migrate_forecast():
    path = os.path.join(DATA_DIR, "app_forecast.csv")
    if not os.path.exists(path):
        print("⚠  app_forecast.csv not found — skipping")
        return
    df = pd.read_csv(path)
    for _, row in df.iterrows():
        db_tables.upsert_forecast_row(
            DATA_DIR,
            str(row["date"]),
            float(row["forecast_avg_price"]) if not pd.isna(row["forecast_avg_price"]) else 0.0,
        )
    print(f"✅ serving_forecast: {len(df)} rows imported")


def migrate_alerts():
    path = os.path.join(DATA_DIR, "app_alerts.csv")
    if not os.path.exists(path):
        print("⚠  app_alerts.csv not found — skipping")
        return
    df = pd.read_csv(path)
    rows = _df_to_dicts(df)
    db_tables.upsert_alert_rows(DATA_DIR, rows)
    print(f"✅ serving_alerts: {len(rows)} rows imported")


def migrate_explanations():
    path = os.path.join(DATA_DIR, "app_explanations.csv")
    if not os.path.exists(path):
        print("⚠  app_explanations.csv not found — skipping")
        return
    df = pd.read_csv(path)
    rows = _df_to_dicts(df)
    db_tables.upsert_explanation_rows(DATA_DIR, rows)
    print(f"✅ serving_explanations: {len(rows)} rows imported")


def migrate_timeline_markers():
    path = os.path.join(DATA_DIR, "app_timeline_markers.csv")
    if not os.path.exists(path):
        print("⚠  app_timeline_markers.csv not found — skipping")
        return
    df = pd.read_csv(path)
    rows = _df_to_dicts(df)

    # Bulk insert via raw SQL (specific columns for this table)
    import sqlite3
    db_path = os.path.join(DATA_DIR, "runtime.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    cols = [
        "entity_id", "date", "marker_type", "marker_label",
        "event_severity_score", "combined_marker_severity", "shift_strength",
        "demand_index", "price_index", "sentiment_index",
        "search_index", "ad_index", "event_explanation", "narrative",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    conn.executemany(
        f"INSERT OR REPLACE INTO serving_timeline_markers ({col_names}) VALUES ({placeholders})",
        [tuple(r.get(c) for c in cols) for r in rows]
    )
    conn.commit()
    conn.close()
    print(f"✅ serving_timeline_markers: {len(rows)} rows imported")


def migrate_regime_shifts():
    path = os.path.join(DATA_DIR, "app_regime_shifts.csv")
    if not os.path.exists(path):
        print("⚠  app_regime_shifts.csv not found — skipping")
        return
    df = pd.read_csv(path)
    rows = _df_to_dicts(df)

    import sqlite3
    db_path = os.path.join(DATA_DIR, "runtime.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    cols = ["date", "regime_type", "shift_strength", "marker_label",
            "event_explanation", "narrative"]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    conn.executemany(
        f"INSERT OR REPLACE INTO serving_regime_shifts ({col_names}) VALUES ({placeholders})",
        [tuple(r.get(c) for c in cols) for r in rows]
    )
    conn.commit()
    conn.close()
    print(f"✅ serving_regime_shifts: {len(rows)} rows imported")


def migrate_events():
    path = os.path.join(DATA_DIR, "events.csv")
    if not os.path.exists(path):
        print("⚠  events.csv not found — skipping")
        return
    df = pd.read_csv(path)
    rows = _df_to_dicts(df)

    import sqlite3
    db_path = os.path.join(DATA_DIR, "runtime.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    cols = [
        "entity_id", "date", "brand", "product_name", "event_type",
        "event_title", "event_description", "impact_direction", "priority",
        "linked_marker_date", "linked_marker_type", "signal_story",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    conn.executemany(
        f"INSERT OR REPLACE INTO serving_events ({col_names}) VALUES ({placeholders})",
        [tuple(r.get(c) for c in cols) for r in rows]
    )
    conn.commit()
    conn.close()
    print(f"✅ serving_events: {len(rows)} rows imported")


def migrate_query_queue():
    path = os.path.join(DATA_DIR, "query.csv")
    if not os.path.exists(path):
        print("⚠  query.csv not found — skipping")
        return
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    rows = _df_to_dicts(df)
    db_tables.upsert_query_queue_rows(DATA_DIR, rows)
    print(f"✅ serving_query_queue: {len(rows)} rows imported")


def main():
    print(f"📂 Data directory: {DATA_DIR}")
    print(f"🗃  Database: {os.path.join(DATA_DIR, 'runtime.db')}")
    print()

    # Ensure tables exist
    db_tables.init_serving_tables(DATA_DIR)
    print("✅ Serving tables created / verified\n")

    migrate_master()
    migrate_daily_summary()
    migrate_forecast()
    migrate_alerts()
    migrate_explanations()
    migrate_timeline_markers()
    migrate_regime_shifts()
    migrate_events()
    migrate_query_queue()

    print("\n🎉 Migration complete!")


if __name__ == "__main__":
    main()
