"""
autonomous_engine.py
────────────────────
Central simulation loop controller.
One tick = 60 seconds real time = 1 simulated day.
Manages start/stop state and orchestrates all sub-systems.
"""

import asyncio
import logging
from datetime import datetime

from state_manager import StateManager
from realtime_ingestor import RealtimeIngestor
from drift_manager import DriftManager
from forecast_manager import ForecastManager
from model_manager import ModelManager

logger = logging.getLogger("autonomous_engine")

# ── Singleton state ─────────────────────────────────────────────────────────
_running = False
_task: asyncio.Task | None = None
_tick_interval_seconds = 60          # 1 simulated day per minute


async def _simulation_loop(data_dir: str):
    """Core loop: run indefinitely until stopped."""
    global _running

    state_mgr   = StateManager(data_dir)
    ingestor    = RealtimeIngestor(data_dir)
    drift_mgr   = DriftManager(data_dir)
    forecast_mgr = ForecastManager(data_dir)
    model_mgr   = ModelManager(data_dir)

    logger.info("🚀 Autonomous simulation loop started")

    while _running:
        tick_start = datetime.utcnow()
        try:
            # 1. Get the next date to ingest
            next_date = ingestor.peek_next_date()
            if next_date is None:
                logger.info("⏹  No more dates in query file — loop paused")
                state_mgr.set_field("status", "exhausted")
                await asyncio.sleep(_tick_interval_seconds)
                continue

            logger.info(f"⏱  Tick → ingesting date: {next_date}")

            # 2. Ingest all rows for that date
            actuals = ingestor.ingest_next_date()
            if actuals.empty:
                await asyncio.sleep(_tick_interval_seconds)
                continue

            # 3. Compare actuals vs stored predictions → write forecast_vs_actual
            drift_mgr.compare_and_log(actuals, next_date)

            # 4. Compute rolling drift metrics
            drift_score = drift_mgr.compute_rolling_drift(window=7)

            # 5. Determine whether to retrain
            sim_day = state_mgr.increment_sim_day()
            should_retrain = model_mgr.should_retrain(sim_day, drift_score)

            model_version = state_mgr.get_field("model_version", "v1.0")

            if should_retrain:
                logger.info(f"🔁 Retraining triggered — sim_day={sim_day}, drift={drift_score:.4f}")
                model_version = model_mgr.retrain(actuals)
                state_mgr.set_field("model_version", model_version)
                state_mgr.set_field("last_retrain_sim_day", sim_day)
                state_mgr.set_field("last_retrain_ts", datetime.utcnow().isoformat())

            # 6. Regenerate next-day forecasts
            forecast_mgr.refresh_forecast(actuals, model_version)

            # 7. Update all serving CSVs
            forecast_mgr.update_serving_layers(actuals, next_date)

            # 8. Persist tick metadata to SQLite state
            state_mgr.update_tick(
                sim_day=sim_day,
                latest_date=next_date,
                drift_score=drift_score,
                model_version=model_version,
                retrained=should_retrain,
                rows_ingested=len(actuals),
            )

            elapsed = (datetime.utcnow() - tick_start).total_seconds()
            logger.info(f"✅ Tick done in {elapsed:.2f}s — sim_day={sim_day}, drift={drift_score:.4f}")

        except Exception as exc:
            logger.exception(f"❌ Error in simulation tick: {exc}")
            state_mgr.set_field("status", "error")

        # Sleep for remainder of tick interval
        elapsed = (datetime.utcnow() - tick_start).total_seconds()
        sleep_for = max(0, _tick_interval_seconds - elapsed)
        await asyncio.sleep(sleep_for)

    logger.info("🛑 Autonomous simulation loop stopped")


# ── Public API ───────────────────────────────────────────────────────────────

def start(data_dir: str) -> dict:
    """Start the simulation loop. Idempotent."""
    global _running, _task

    if _running:
        return {"status": "already_running"}

    _running = True

    loop = asyncio.get_event_loop()
    _task = loop.create_task(_simulation_loop(data_dir))

    return {"status": "started"}


def stop() -> dict:
    """Stop the simulation loop gracefully."""
    global _running, _task

    if not _running:
        return {"status": "not_running"}

    _running = False
    if _task:
        _task.cancel()
        _task = None

    return {"status": "stopped"}


def is_running() -> bool:
    return _running