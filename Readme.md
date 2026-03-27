# Intellim — Competitive Intelligence Platform

A real-time market intelligence and demand forecasting platform for consumer electronics. Tracks 70 products across 7 categories, runs autonomous ML retraining loops, and surfaces competitive signals through an analytics SPA and an AI-powered chat interface.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                      FastAPI Application                │
│                          main.py                        │
├──────────────┬──────────────────────────┬───────────────┤
│  Auth Layer  │      REST API (~25 routes)│  Chat Route   │
│  Google OAuth│  /api/daily-summary      │  /chat (POST) │
│  Session Mgmt│  /api/forecast           │  Gemini LLM   │
│              │  /api/alerts             │               │
│              │  /api/product/:id/*      │               │
│              │  /api/autonomous/*       │               │
│              │  /api/realtime/*         │               │
│              │  /api/model/*            │               │
└──────────────┴──────────────────────────┴───────────────┘
        │                    │
        ▼                    ▼
┌───────────────┐   ┌────────────────────────────────────┐
│   MongoDB     │   │        Autonomous Engine           │
│  (Chat store) │   │        autonomous_engine.py        │
│               │   │                                    │
│  messages     │   │  1 tick = 60s real = 1 sim day     │
│  collection   │   │                                    │
└───────────────┘   │  ┌──────────────────────────────┐  │
                    │  │  RealtimeIngestor             │  │
                    │  │  query.csv → date-by-date     │  │
                    │  ├──────────────────────────────┤  │
                    │  │  DriftManager                 │  │
                    │  │  MAE drift → retrain trigger  │  │
                    │  ├──────────────────────────────┤  │
                    │  │  ModelManager                 │  │
                    │  │  XGBoost retrain + versioning │  │
                    │  ├──────────────────────────────┤  │
                    │  │  ForecastManager              │  │
                    │  │  Next-day forecasts + serving │  │
                    │  ├──────────────────────────────┤  │
                    │  │  StateManager                 │  │
                    │  │  SQLite — all runtime state   │  │
                    │  └──────────────────────────────┘  │
                    └────────────────────────────────────┘
                                     │
                    ┌────────────────▼───────────────────┐
                    │           data/ directory           │
                    │  app_master_clean.csv  (11 MB)      │
                    │  app_daily_summary.csv              │
                    │  app_forecast.csv                   │
                    │  app_alerts.csv                     │
                    │  app_explanations.csv               │
                    │  app_timeline_markers.csv           │
                    │  query.csv  (ingest queue)          │
                    │  runtime.db (SQLite state)          │
                    │  models/                            │
                    │    xgb_demand_model.pkl             │
                    │    xgb_price_model.pkl              │
                    │    lstm_model.h5                    │
                    └────────────────────────────────────┘
```

---

## Stack

| Layer | Technology |
|---|---|
| Web framework | FastAPI 0.110 + Uvicorn |
| Templating | Jinja2 |
| Auth | Google OAuth 2.0 via Authlib |
| Session | Starlette SessionMiddleware (signed cookies) |
| Chat persistence | MongoDB Atlas (pymongo) |
| LLM | Google Gemini (google-genai) |
| ML models | XGBoost (demand + price), LSTM (price sequence) |
| State store | SQLite WAL mode (runtime.db) |
| Data layer | Pandas, NumPy |
| Frontend charts | Chart.js 4.4.1 |
| Frontend fonts | DM Sans, Cormorant Garamond (Google Fonts) |

---

## Project Structure

```
.
├── main.py                    # FastAPI app, all routes, data loading
├── autonomous_engine.py       # Simulation loop controller (asyncio)
├── state_manager.py           # SQLite state r/w (thread-safe, WAL)
├── realtime_ingestor.py       # query.csv consumer — one date per tick
├── drift_manager.py           # Forecast vs actual comparison + MAE drift
├── model_manager.py           # XGBoost retrain logic + model registry
├── forecast_manager.py        # Next-day forecast generation + CSV refresh
├── llm.py                     # Gemini API wrapper
├── db.py                      # MongoDB connection helper
├── mongo_test.py              # MongoDB connectivity check
├── requirements.txt
├── .env                       # Environment variables (not committed)
├── templates/
│   ├── login.html             # Google OAuth login page
│   ├── dashboard.html         # AI chat interface
│   ├── explore.html           # Analytics SPA (all pages)
│   └── index.html             # Landing / redirect
└── data/
    ├── app_master_clean.csv   # Full entity-level daily time series
    ├── app_daily_summary.csv  # Market-level daily aggregates
    ├── app_forecast.csv       # Next 14-day price forecasts
    ├── app_alerts.csv         # Anomaly alerts by entity
    ├── app_explanations.csv   # Event-driven signal narratives
    ├── app_timeline_markers.csv
    ├── app_regime_shifts.csv  # Structural market shift dates
    ├── app_daily_summary.csv
    ├── query.csv              # Pending ingest queue (new actuals)
    ├── events.csv             # Historical event log
    ├── feature_metadata.json  # Feature schema descriptor
    ├── model_registry.json    # Active model versions + metrics
    ├── runtime.db             # SQLite — autonomous engine state
    └── models/
        ├── xgb_demand_model.pkl
        ├── xgb_price_model.pkl
        ├── lstm_model.h5
        └── lstm_scaler.pkl
```

---

## Data Coverage

- **Products:** 70 tracked entities
- **Categories:** Earbuds, Headphones, Smartphones, Smartwatches, Bluetooth Speakers, Tablets, Power Banks
- **History:** Jan 1 2025 – Jun 29 2025 (180 days, ~6,750 entity-day rows in serving layer)
- **Signals per row:** Price index, demand index, sentiment index, search index, ad index, health score, statistical event flags, peak/bottom markers, change-point detection, XGBoost predictions

---

## ML Models

### XGBoost — Demand Index
- **Target:** `demand_index` (composite score from search interest, reviews, sentiment, ad activity)
- **MAE:** 0.389 | **RMSE:** 0.550 | **R²:** 0.9987
- **Features:** ~100+ engineered features including rolling lags (1/2/3/7d), z-scores, momentum signals, category rank, event flags

### XGBoost — Price Index
- **Target:** `price_index`
- **MAE:** 4.12 | **RMSE:** 13.20 | **R²:** 0.9959

### LSTM — Price Sequence Forecast
- **Target:** `avg_actual_price` (market-level 14-day forward)
- **MAE:** 3.14 | **RMSE:** 4.30
- **Note:** R² is negative on held-out data — the LSTM is used for directional trend signals, not point estimates

---

## Autonomous Simulation Engine

The engine runs as a background asyncio task. Each tick (60 seconds real time) represents one simulated market day.

**Tick sequence:**

1. `RealtimeIngestor.peek_next_date()` — check for unconsumed dates in `query.csv`
2. `RealtimeIngestor.ingest_next_date()` — load all rows for that date into memory
3. `DriftManager.compare_and_log()` — match actuals against stored predictions, write `forecast_vs_actual` entries to SQLite
4. `DriftManager.compute_rolling_drift(window=7)` — rolling combined MAE over last 7 sim days
5. `ModelManager.should_retrain()` — trigger if `sim_day % 7 == 0` or `drift_score > 8.0`
6. `ModelManager.retrain()` (conditional) — clean XGBoost rebuild on expanded history, bumps model version
7. `ForecastManager.refresh_forecast()` — generate next-day demand/price predictions for all entities
8. `ForecastManager.update_serving_layers()` — atomic CSV refresh for all `app_*.csv` files
9. `StateManager.update_tick()` — persist tick metadata (sim_day, drift, model version, retrain flag) to SQLite

**Drift formula:**
```
combined_drift = 0.5 × rolling_MAE(demand_index) + 0.5 × rolling_MAE(price_index_normalised)
```

**Retrain policy (from `model_registry.json`):**
- Scheduled: every 3 simulated days (configurable via `RETRAIN_EVERY_N_DAYS`)
- Drift-triggered: if `combined_drift > 0.15` (production threshold; engine internal uses 8.0 for tabular stability)

**Engine control endpoints:**
```
POST /api/autonomous/start     — start simulation loop
POST /api/autonomous/stop      — stop gracefully
GET  /api/autonomous/status    — current sim_day, drift, model version, retrain history
```

---

## API Reference

### Data endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/daily-summary` | Market-level aggregates: actual + predicted demand/price, sentiment, event flags |
| GET | `/api/forecast` | 14-day price forecast from LSTM (reads live from `app_forecast.csv`) |
| GET | `/api/regime-shifts` | Structural shift dates, types, strength scores |
| GET | `/api/alerts?limit=N&brand=X` | Anomaly alerts filtered by brand (live CSV read) |
| GET | `/api/filters` | Available categories, brands, entity IDs for dropdowns |
| GET | `/api/category/{category}/summary` | Category-level KPIs, top brands, demand curve |

### Product endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/product/{entity_id}/overview` | KPIs: price, demand, sentiment, health, ad index |
| GET | `/api/product/{entity_id}/series` | Full time series for charting |
| GET | `/api/product/{entity_id}/markers` | Peak/bottom/change-point markers |
| GET | `/api/product/{entity_id}/alerts` | Entity-specific anomaly history |
| GET | `/api/product/{entity_id}/events` | Event log for that product |

### Realtime / model endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/api/realtime/ingest-next-date` | Manually trigger one tick of ingest |
| GET | `/api/realtime/drift` | Current drift score and rolling history |
| GET | `/api/realtime/forecast-vs-actual` | Recent prediction accuracy rows |
| POST | `/api/model/retrain` | Manually trigger XGBoost retrain |
| GET | `/api/model/status` | Active model version, metrics, retrain log |

### Chat endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/chat/new` | Create new chat session, returns `chat_id` |
| GET | `/chats` | List all chat sessions for authenticated user |
| GET | `/chat/{chat_id}` | Load message history for a session |
| POST | `/chat` | Send message, get Gemini reply with market context injected |

---

## Frontend — Explore SPA

`explore.html` is a single-page application with client-side routing (no page reloads). All market data is embedded in the page as a `const D = {...}` JSON blob at render time to avoid waterfall API calls.

**Pages / sections:**

| Section | Content |
|---|---|
| Dashboard | KPI cards, 12-week demand trend (actual + predicted), scatter, radar, gauge, timeline |
| Trends | Brand demand curves by category, intensity heatmap, market share pie |
| Competitors | Top 12 competitor cards with price/sentiment sparklines and anomaly flags |
| Signals | Regime shifts, structural change markers, alert feed |
| Simulator | What-if projection — adjustable demand curve against 12-week baseline |
| Pipeline | Autonomous engine architecture diagram |

**Demand chart — Next Day button:**
The demand chart supports a "Next Day" button that appends one day at a time from `D.next_days` (7 entries: Jun 30 from `query.csv` actuals, Jul 1–6 projected). Both the actual (solid gold) and predicted (dashed blue, hollow points) datasets extend on each click. The button disables after all 7 days are consumed and resets on category switch.

---

## Frontend — Dashboard (Chat)

`dashboard.html` is the AI chat interface. Messages from the Gemini API are rendered through a client-side Markdown formatter (`formatMarkdown()`) that handles:

- `**bold**` — rendered as gold-coloured `<strong>`
- `*italic*` — rendered as `<em>`
- `# / ## / ###` — serif headings with gold colour
- `* item` / `- item` — custom bullet list with gold `›` marker
- `1. item` — custom numbered list with gold counters
- `` `code` `` — inline code chip
- `---` — horizontal rule

User messages are set via `.textContent` (safe, unparsed). Bot messages use `innerHTML` with the formatted output.

---

## Environment Variables

```env
MONGO_URI=              # MongoDB Atlas connection string
GOOGLE_CLIENT_ID=       # Google OAuth 2.0 client ID
GOOGLE_CLIENT_SECRET=   # Google OAuth 2.0 client secret
SESSION_SECRET=         # Starlette session signing key
GEMINI_API_KEY=         # Google Gemini API key
```

---

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Fill in MONGO_URI, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SESSION_SECRET, GEMINI_API_KEY

# 3. Ensure data directory is populated
ls data/
# Required: app_master_clean.csv, app_daily_summary.csv, app_forecast.csv,
#           app_alerts.csv, app_explanations.csv, app_timeline_markers.csv,
#           app_regime_shifts.csv, query.csv, runtime.db, models/

# 4. Run
uvicorn main:app --reload --port 8000
```

The application will be available at `http://localhost:8000`. Login requires a Google account authorised in the OAuth consent screen configuration.

---

## SQLite Schema (runtime.db)

| Table | Purpose |
|---|---|
| `autonomous_state` | Key-value store for current sim pointer, model version, drift score |
| `predictions_log` | Forecasts emitted before actuals arrive (entity × date × predicted values) |
| `actuals_log` | Actual rows ingested from `query.csv` |
| `forecast_vs_actual` | Per-row comparison: predicted vs actual, absolute error |
| `drift_summary` | Rolling drift snapshots per tick |
| `model_registry` | Model version history with training timestamps and metrics |
| `retrain_jobs` | Record of each retrain event: trigger reason, drift at time, version bumped to |

All tables use WAL journal mode (`PRAGMA journal_mode=WAL`) for safe concurrent reads during async tick execution.

---

## Notes

- `app_*.csv` serving files are never written in-place. `ForecastManager` uses atomic rename (write to `.tmp`, then `os.replace()`) to prevent partial reads by the API layer.
- The autonomous engine runs as a single asyncio Task on the FastAPI event loop. It does not use threading. SQLite writes inside the engine use a module-level `threading.Lock` as a defensive guard for the WAL pragma.
- `query.csv` is the ingest queue. Adding new dates to this file extends the simulation without restarting the server. The `RealtimeIngestor` reads it once at startup and maintains a date pointer in SQLite to ensure idempotency.
- Gemini chat calls inject a structured market context string (top competitors, demand trend, recent alerts, regime state) as system context before the user message. This context is rebuilt per request from the in-memory DataFrames.