from fastapi import FastAPI, Request, Body
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from authlib.integrations.starlette_client import OAuth
from dotenv import load_dotenv
from datetime import datetime
import uuid
import os

from db import get_db
from llm import chat_with_gemini

# ── New autonomous modules ────────────────────────────────────────────────────
import autonomous_engine
import db_tables
from state_manager import StateManager
from realtime_ingestor import RealtimeIngestor
from drift_manager import DriftManager
from model_manager import ModelManager

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
SESSION_SECRET = os.getenv("SESSION_SECRET")
if not MONGO_URI or not SESSION_SECRET:
    raise RuntimeError("Missing env vars: MONGO_URI or SESSION_SECRET")

db = get_db(MONGO_URI)
users = db["users"]
messages = db["messages"]

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
templates = Jinja2Templates(directory="templates")

oauth = OAuth()
oauth.register(
    name="google",
    client_id=os.getenv("GOOGLE_CLIENT_ID"),
    client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

# ─────────────────────────────────────────
# Initialise SQLite serving tables
# ─────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

db_tables.init_serving_tables(DATA_DIR)

counts = db_tables.query_unique_counts(DATA_DIR)
print(f"✅ DB ready — {counts['total_rows']} rows · {counts['entities']} products · {counts['brands']} brands")

# ── Autonomous module singletons (lazy, data_dir bound) ───────────────────────
_state_mgr   = StateManager(DATA_DIR)
_ingestor    = RealtimeIngestor(DATA_DIR)
_drift_mgr   = DriftManager(DATA_DIR)
_model_mgr   = ModelManager(DATA_DIR)


# ─────────────────────────────────────────
# Auth helpers
# ─────────────────────────────────────────
def require_user(request: Request):
    return request.session.get("user")

def get_chat_history(user_id: str, chat_id: str, limit: int = 20):
    cursor = (
        messages.find({"user_id": user_id, "chat_id": chat_id})
        .sort("timestamp", 1)
        .limit(limit)
    )
    history = [
        "You are IntelliM, a market intelligence analyst for consumer electronics. "
        "You have access to the following real data (Jan–Jun 2025):\n\n"
        "DATASET: 70 products, 73 brands, 7 categories (earbuds, smartphones, smartwatches, bluetooth_speakers, power_banks, headphones, tablets), 6,750 data rows.\n\n"
        "CATEGORY PERFORMANCE:\n"
        "- Headphones: avg demand 51.5, sentiment 74.3%, top brands: BassAudio (demand 67.4, anomaly Apr-24 severity 3.49), DeepAudio (demand 53.1, anomaly May-28), VoiceAudio (50.5), WaveGear (50.4, anomaly May-27), EasyGear (46.0), CasualGear (41.4)\n"
        "- Earbuds: avg demand 40.5, sentiment 67.3%, top brands: ProAudio (65.3), SonicAudio (62.2), SoundWave (59.1), GoldAudio (56.0, health 97.9), TurboWave (50.8), LuxAudio (anomaly Jun-29)\n"
        "- Smartphones: avg demand 42.1, sentiment 66.7%, top brands: ApexTech (67.1, price $1000, anomaly Jun-6), NovaTech (61.9, product announcement Jan), HyperTech (61.2), VivaTech (61.1), MegaMobile (54.9)\n"
        "- Smartwatches: best health brand LuxBrand (99.4 health, price $350, sentiment 84.4%, upgraded Jan 21), EliteBrand (97.5, sentiment 91.1%), ChronoBrand (97.2, anomaly May-21)\n"
        "- Bluetooth Speakers: StudioWave (98.3 health, demand 61.8), CrystalAudio (97.6, anomaly Jun-25), WaveAudio (anomaly May-29 severity 3.45), BlastAudio (anomaly May-25), BoomAudio (product announcement Jan)\n"
        "- Tablets: TabSlate (97.5 health), EduTech (97.6), TabTech (ad campaigns Jan & Feb)\n"
        "- Power Banks: PowerTech, SlimPower, ChargeTech, NanoPower, TurboPower, CellPower — stable low demand\n\n"
        "REGIME SHIFTS (6 total): positive Apr-3, negative Apr-18, positive May-2, negative May-14, negative Jun-11, positive Jun-21 (latest)\n\n"
        "TOP ANOMALY ALERTS (677 total): BassAudio Apr-24 (3.49), WaveAudio May-29 (3.45), DeepAudio May-28 (3.44), BassAudio May-20 (3.23), CrystalAudio Jun-25 (3.14), BlastAudio May-25 (3.11), LuxAudio Jun-29 (3.10), ApexTech Jun-6 (3.09)\n\n"
        "KEY EVENTS: TechGear, CoreMobile, ApexTech ad campaigns; NovaTech & BoomAudio product announcements; BassAudio & LuxBrand product upgrades Jan; SwiftTech & PureAudio campaigns Feb; DealTech & SonicAudio ad burst before Mar spike; ClearWave Apr campaign.\n\n"
        "PRICE TRENDS: Overall avg price -1.5% over 30 days. Smartphones $200–$1000. Headphones $87–$275. Earbuds $16–$149.\n\n"
        "Answer questions concisely and data-driven. Reference specific brands, dates, and metrics when relevant.\n"
    ]
    for m in cursor:
        role = "User" if m["role"] == "user" else "Assistant"
        history.append(f"{role}: {m['content']}")
    return history


# ─────────────────────────────────────────
# Auth routes
# ─────────────────────────────────────────
@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/login")
async def login(request: Request):
    return await oauth.google.authorize_redirect(request, request.url_for("auth_callback"))

@app.get("/auth/callback")
async def auth_callback(request: Request):
    token = await oauth.google.authorize_access_token(request)
    info = token.get("userinfo")
    if not info:
        return HTMLResponse("Login failed", 400)
    user = users.find_one({"email": info["email"]})
    if not user:
        user = {
            "google_id": info["sub"],
            "name": info["name"],
            "email": info["email"],
            "picture": info["picture"],
            "created_at": datetime.utcnow()
        }
        user["_id"] = users.insert_one(user).inserted_id
    request.session["user"] = {
        "id": str(user["_id"]),
        "name": user["name"],
        "email": user["email"],
        "picture": user["picture"]
    }
    return RedirectResponse("/dashboard")

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/")


# ─────────────────────────────────────────
# Page routes
# ─────────────────────────────────────────
@app.get("/dashboard")
def dashboard(request: Request):
    """AI Chat dashboard — Gemini-powered chat interface."""
    user = require_user(request)
    if not user:
        return RedirectResponse("/")
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": user})

@app.get("/explore")
def explore(request: Request):
    """Analytics SPA — real data powering all pages via SQLite."""
    user = require_user(request)
    if not user:
        return RedirectResponse("/")
    return templates.TemplateResponse("explore.html", {"request": request, "user": user})


# ─────────────────────────────────────────
# Chat routes
# ─────────────────────────────────────────
@app.post("/chat/new")
def new_chat(request: Request):
    user = require_user(request)
    if not user:
        return JSONResponse({"error": "unauth"}, 401)
    return {"chat_id": str(uuid.uuid4())}

@app.get("/chats")
def list_chats(request: Request):
    user = require_user(request)
    if not user:
        return JSONResponse({"error": "unauth"}, 401)
    chat_ids = messages.distinct("chat_id", {"user_id": user["id"]})
    return {"chats": chat_ids}

@app.get("/chat/{chat_id}")
def load_chat(chat_id: str, request: Request):
    user = require_user(request)
    if not user:
        return JSONResponse({"error": "unauth"}, 401)
    docs = messages.find({"user_id": user["id"], "chat_id": chat_id}).sort("timestamp", 1)
    return {"messages": [{"role": d["role"], "content": d["content"]} for d in docs]}

@app.post("/chat")
async def chat(request: Request, data: dict = Body(...)):
    user = require_user(request)
    if not user:
        return JSONResponse({"error": "unauth"}, 401)
    chat_id = data.get("chat_id")
    msg = data.get("message")
    if not chat_id or not msg:
        return JSONResponse({"error": "bad request"}, 400)
    messages.insert_one({
        "user_id": user["id"], "chat_id": chat_id,
        "role": "user", "content": msg, "timestamp": datetime.utcnow()
    })
    history = get_chat_history(user["id"], chat_id)
    reply = chat_with_gemini(history)
    messages.insert_one({
        "user_id": user["id"], "chat_id": chat_id,
        "role": "assistant", "content": reply, "timestamp": datetime.utcnow()
    })
    return {"reply": reply}


# ─────────────────────────────────────────
# Data API — /api/* routes backed by SQLite
# ─────────────────────────────────────────

@app.get("/api/filters")
def api_filters():
    """Return available categories, brands, and entity IDs for dropdowns."""
    return db_tables.query_filters(DATA_DIR)

@app.get("/api/daily-summary")
def api_daily_summary():
    """Global daily market overview — demand, price, sentiment."""
    rows = db_tables.query_daily_summary(DATA_DIR)
    return rows

@app.get("/api/forecast")
def api_forecast():
    """Price forecast for the next 14 days — live from SQLite."""
    return db_tables.query_forecast(DATA_DIR)

@app.get("/api/regime-shifts")
def api_regime_shifts():
    """Structural regime shift dates and types."""
    return db_tables.query_regime_shifts(DATA_DIR)

@app.get("/api/alerts")
def api_alerts(limit: int = 20, brand: str = None):
    """Top signal alerts, optionally filtered by brand."""
    return db_tables.query_alerts(DATA_DIR, limit=limit, brand=brand)

@app.get("/api/product/{entity_id}/overview")
def api_product_overview(entity_id: int):
    """Latest KPI snapshot for a single product."""
    r = db_tables.query_master_latest_one(DATA_DIR, entity_id)
    if not r:
        return JSONResponse({"error": "not found"}, 404)
    return {
        "entity_id": int(r["entity_id"]),
        "brand": r["brand"],
        "product_name": r["product_name"],
        "category": r["category"],
        "demand_index": round(float(r.get("demand_index", 0)), 2),
        "price_index": round(float(r.get("price_index", 0)), 2),
        "sentiment_index": round(float(r.get("sentiment_index", 0)), 4),
        "search_index": round(float(r.get("search_index", 0)), 2),
        "ad_index": round(float(r.get("ad_index", 0)), 2),
        "health_index": round(float(r.get("health_index", 0)), 2),
        "list_price": round(float(r.get("list_price", 0) or 0), 2),
    }

@app.get("/api/product/{entity_id}/series")
def api_product_series(entity_id: int):
    """Full time-series for a product: demand, price, sentiment, search, ad."""
    rows = db_tables.query_master_by_entity(DATA_DIR, entity_id)
    if not rows:
        return JSONResponse({"error": "not found"}, 404)
    cols = ["date", "demand_index", "price_index", "sentiment_index",
            "search_index", "ad_index", "health_index",
            "change_point", "shift_strength"]
    return [{k: r.get(k) for k in cols} for r in rows]

@app.get("/api/product/{entity_id}/markers")
def api_product_markers(entity_id: int):
    """Anomaly, peak, and dip markers for a product."""
    rows = db_tables.query_product_markers(DATA_DIR, entity_id)
    if not rows:
        return []
    cols = ["date", "marker_type", "marker_label", "event_severity_score",
            "combined_marker_severity", "shift_strength",
            "demand_index", "price_index", "sentiment_index",
            "search_index", "ad_index",
            "event_explanation", "narrative"]
    return [{k: r.get(k) for k in cols} for r in rows]

@app.get("/api/product/{entity_id}/alerts")
def api_product_alerts(entity_id: int, limit: int = 10):
    """Top alerts for a specific product."""
    rows = db_tables.query_product_alerts(DATA_DIR, entity_id, limit=limit)
    cols = ["date", "marker_type", "alert_title", "event_explanation",
            "narrative", "event_severity_score",
            "demand_index", "price_index", "sentiment_index"]
    return [{k: r.get(k) for k in cols} for r in rows]

@app.get("/api/product/{entity_id}/events")
def api_product_events(entity_id: int):
    """Ad campaigns, product announcements, upgrades for a product."""
    rows = db_tables.query_product_events(DATA_DIR, entity_id)
    if not rows:
        return []
    cols = ["date", "brand", "product_name", "event_type", "event_title",
            "event_description", "impact_direction", "priority",
            "linked_marker_date", "linked_marker_type", "signal_story"]
    return [{k: r.get(k) for k in cols} for r in rows]

@app.post("/api/explain")
async def api_explain(data: dict = Body(...)):
    """
    AI-powered explanation for a product signal event.
    Pass: { entity_id, date } or { question }
    """
    entity_id = data.get("entity_id")
    date = data.get("date")
    question = data.get("question", "")

    context_parts = []

    if entity_id:
        r = db_tables.query_master_latest_one(DATA_DIR, entity_id)
        if r:
            context_parts.append(
                f"Product: {r['brand']} {r['product_name']} ({r['category']})\n"
                f"Latest metrics — Demand: {float(r.get('demand_index',0)):.1f}, "
                f"Price: {float(r.get('price_index',0)):.1f}, "
                f"Sentiment: {float(r.get('sentiment_index',0))*100:.1f}%, "
                f"Search: {float(r.get('search_index',0)):.1f}, "
                f"Ad Index: {float(r.get('ad_index',0)):.1f}, "
                f"Health: {float(r.get('health_index',0)):.1f}"
            )

    if entity_id and date:
        exp = db_tables.query_explanations_for(DATA_DIR, entity_id, date)
        if exp:
            context_parts.append(
                f"Signal on {date}: {exp.get('marker_type','unknown')}\n"
                f"Explanation: {exp.get('event_explanation','')}\n"
                f"Narrative: {exp.get('narrative','')}"
            )

    context = "\n\n".join(context_parts) if context_parts else ""
    prompt = f"""You are IntelliM, a market intelligence analyst.
Context data:
{context}

Question: {question or f"Explain the market signal for entity {entity_id} on {date}"}

Give a concise, insightful 2-3 sentence analysis."""

    try:
        from llm import chat_with_gemini
        answer = chat_with_gemini([prompt])
        return {"answer": answer}
    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)

@app.get("/api/category/{category}/summary")
def api_category_summary(category: str):
    """Aggregate metrics for all products in a category."""
    rows = db_tables.query_master_by_category(DATA_DIR, category)
    if not rows:
        return JSONResponse({"error": "not found"}, 404)

    import numpy as np
    demand_vals = [float(r.get("demand_index", 0)) for r in rows]
    price_vals  = [float(r.get("price_index", 0)) for r in rows]
    sent_vals   = [float(r.get("sentiment_index", 0)) for r in rows]
    ad_vals     = [float(r.get("ad_index", 0)) for r in rows]
    health_vals = [float(r.get("health_index", 0)) for r in rows]

    best_row = max(rows, key=lambda r: float(r.get("health_index", 0)))

    return {
        "category": category,
        "count": len(rows),
        "avg_demand": round(float(np.mean(demand_vals)), 2),
        "avg_price": round(float(np.mean(price_vals)), 2),
        "avg_sentiment": round(float(np.mean(sent_vals)) * 100, 1),
        "avg_ad_index": round(float(np.mean(ad_vals)), 2),
        "avg_health": round(float(np.mean(health_vals)), 2),
        "top_brand": best_row["brand"],
        "products": [
            {k: r.get(k) for k in ["entity_id", "brand", "product_name",
                                     "demand_index", "price_index",
                                     "sentiment_index", "health_index"]}
            for r in rows
        ],
    }


# ═════════════════════════════════════════════════════════════════════════════
# AUTONOMOUS LOOP ROUTES — /api/autonomous/*
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/api/autonomous/start")
async def autonomous_start():
    """Start the 60s simulation loop."""
    result = autonomous_engine.start(DATA_DIR)
    return result

@app.post("/api/autonomous/stop")
async def autonomous_stop():
    """Stop the simulation loop."""
    result = autonomous_engine.stop()
    return result

@app.get("/api/autonomous/status")
async def autonomous_status():
    """
    Full status snapshot:
      - running flag
      - current sim day
      - latest ingested date
      - latest prediction vs actual
      - rolling drift
      - model version
      - whether retraining occurred last tick
    """
    state = _state_mgr.get_all_state()
    # Flatten to {key: value} for easy UI consumption
    flat = {k: v["value"] for k, v in state.items()}
    flat["is_running"] = autonomous_engine.is_running()

    # Attach latest FVA row
    fva = _drift_mgr.get_fva_recent(limit=1)
    flat["latest_fva"] = fva[0] if fva else None

    # Next date waiting to be ingested
    flat["next_pending_date"] = _ingestor.peek_next_date()

    return flat


# ─────────────────────────────────────────
# REALTIME ROUTES — /api/realtime/*
# ─────────────────────────────────────────

@app.post("/api/realtime/ingest-next-date")
async def realtime_ingest_next():
    """
    Manually trigger ingestion of the next date from the query queue.
    Useful for demos / step-through mode without the full loop.
    """
    next_date = _ingestor.peek_next_date()
    if next_date is None:
        return JSONResponse({"status": "exhausted", "message": "No more dates in query queue"})

    actuals = _ingestor.ingest_next_date()
    if actuals.empty:
        return JSONResponse({"status": "empty", "date": next_date})

    # Run the same processing pipeline as the autonomous loop
    _drift_mgr.compare_and_log(actuals, next_date)
    drift_score = _drift_mgr.compute_rolling_drift(window=7)

    sim_day = _state_mgr.increment_sim_day()
    should_retrain = _model_mgr.should_retrain(sim_day, drift_score)
    model_version = _state_mgr.get_field("model_version", "v1.0")

    if should_retrain:
        model_version = _model_mgr.retrain(actuals)
        _state_mgr.set_field("model_version", model_version)

    from forecast_manager import ForecastManager
    fm = ForecastManager(DATA_DIR)
    fm.refresh_forecast(actuals, model_version)
    fm.update_serving_layers(actuals, next_date)

    _state_mgr.update_tick(
        sim_day=sim_day,
        latest_date=next_date,
        drift_score=drift_score,
        model_version=model_version,
        retrained=should_retrain,
        rows_ingested=len(actuals),
    )

    return {
        "status": "ok",
        "date_ingested": next_date,
        "rows": len(actuals),
        "sim_day": sim_day,
        "drift_score": drift_score,
        "model_version": model_version,
        "retrained": should_retrain,
    }

@app.get("/api/realtime/drift")
async def realtime_drift():
    """Rolling drift metrics and recent history."""
    current_drift = float(_state_mgr.get_field("current_drift", 0.0))
    history = _drift_mgr.get_drift_history(limit=30)
    return {
        "current_drift": current_drift,
        "drift_flag": current_drift > 5.0,
        "threshold": 5.0,
        "history": history,
    }

@app.get("/api/realtime/forecast-vs-actual")
async def realtime_fva(limit: int = 50):
    """Recent forecast vs actual comparison rows."""
    rows = _drift_mgr.get_fva_recent(limit=limit)
    return {"count": len(rows), "rows": rows}


# ─────────────────────────────────────────
# MODEL ROUTES — /api/model/*
# ─────────────────────────────────────────

@app.post("/api/model/retrain")
async def model_retrain():
    """Manually trigger a model retrain."""
    actuals = _ingestor.get_all_actuals()
    if actuals.empty:
        return JSONResponse({"status": "no_data", "message": "No actuals ingested yet"})
    new_version = _model_mgr.retrain(actuals)
    _state_mgr.set_field("model_version", new_version)
    _state_mgr.set_field("last_retrain_ts", datetime.utcnow().isoformat())
    return {"status": "ok", "new_version": new_version}

@app.get("/api/model/status")
async def model_status():
    """Current model version and registry info."""
    status = _model_mgr.get_model_status()
    status["current_sim_day"] = _state_mgr.get_field("sim_day", 0)
    status["last_retrain_sim_day"] = _state_mgr.get_field("last_retrain_sim_day", 0)
    return status