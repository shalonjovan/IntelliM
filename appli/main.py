from fastapi import FastAPI, Request, Body
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from authlib.integrations.starlette_client import OAuth
from dotenv import load_dotenv
from datetime import datetime
import uuid
import os
import pandas as pd
import numpy as np
import httpx
import asyncio

from db import get_db
from llm import chat_with_gemini

# ── HF API CONFIG ─────────────────────────────────────────────────────────────
HF_BASE_URL = os.getenv("HF_API_BASE_URL", "https://tyra1586-intellim-analytics-engine.hf.space")

async def hf_fetch(endpoint: str, method: str = "GET", params: dict = None, json_data: dict = None):
    """Utility to fetch data from the remote HF Analytics Engine."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            if method == "GET":
                resp = await client.get(f"{HF_BASE_URL}{endpoint}", params=params)
            else:
                resp = await client.post(f"{HF_BASE_URL}{endpoint}", json=json_data)
            
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"❌ HF API Error [{endpoint}]: {e}")
            return None

# ── New autonomous modules (kept for local state if needed, but bypassed for core data) ───────

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
# Initial Data State (Refetched from API or Fallback)
# ─────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Global data holders
master = pd.DataFrame()
markers_df = pd.DataFrame()
alerts_df = pd.DataFrame()
explanations = pd.DataFrame()
daily = pd.DataFrame()
forecast_df = pd.DataFrame()
regime_df = pd.DataFrame()
events_df = pd.DataFrame()
latest_master = pd.DataFrame()

async def refresh_all_data():
    """Initial load of data from HF Space."""
    global master, markers_df, alerts_df, explanations, daily, forecast_df, regime_df, events_df, latest_master
    
    print("📡 Fetching historical data from HF Analytics Engine...")
    
    # 1. Master Data
    master_res = await hf_fetch("/api/master-data")
    if master_res and "rows" in master_res:
        master = pd.DataFrame(master_res["rows"])
        if not master.empty:
            if "entity_id" in master.columns:
                master["entity_id"] = master["entity_id"].astype(str)
            latest_master = master.sort_values("date").groupby("entity_id").last().reset_index()

    # 2. Daily Summary
    daily_res = await hf_fetch("/api/daily-summary")
    if daily_res and "rows" in daily_res:
        daily = pd.DataFrame(daily_res["rows"])

    # 3. Events
    events_res = await hf_fetch("/api/events")
    if events_res and "rows" in events_res:
        events_df = pd.DataFrame(events_res["rows"])

    # 4. Markers
    markers_res = await hf_fetch("/api/timeline-markers")
    if markers_res and "rows" in markers_res:
        markers_df = pd.DataFrame(markers_res["rows"])

    # 5. Regime Shifts
    regime_res = await hf_fetch("/api/regime-shifts")
    if regime_res and "rows" in regime_res:
        regime_df = pd.DataFrame(regime_res["rows"])

    # 6. Alerts
    alerts_res = await hf_fetch("/api/alerts")
    if alerts_res and "rows" in alerts_res:
        alerts_df = pd.DataFrame(alerts_res["rows"])

    # 7. Forecast
    forecast_res = await hf_fetch("/api/forecast")
    if forecast_res and "rows" in forecast_res:
        forecast_df = pd.DataFrame(forecast_res["rows"])

    print(f"✅ Data Refresh: {len(master)} master rows · {len(daily)} summary rows")

@app.on_event("startup")
async def startup_event():
    await refresh_all_data()

# Pre-compute latest state per entity
# ── Autonomous module singletons (bypassed, using remote API) ───────────────────
_state_mgr   = None
_ingestor    = None
_drift_mgr   = None
_model_mgr   = None


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
    return RedirectResponse("/explore")

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
    """Analytics SPA — real CSV data powering all pages."""
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
# Data API — /api/* routes backed by CSVs
# ─────────────────────────────────────────

@app.get("/api/filters")
def api_filters():
    """Return available categories, brands, and entity IDs from remote API cache."""
    if master.empty:
        return {"categories": [], "brands": [], "entities": []}
    return {
        "categories": sorted(master["category"].unique().tolist()) if "category" in master.columns else [],
        "brands": sorted(master["brand"].unique().tolist()) if "brand" in master.columns else [],
        "entities": latest_master[["entity_id", "brand", "product_name", "category"]]
                    .sort_values("brand")
                    .to_dict("records") if not latest_master.empty else [],
    }

@app.get("/api/daily-summary")
def api_daily_summary():
    """Global daily market overview from remote API cache."""
    if daily.empty:
        return []
    cols = ["date", "avg_actual_demand", "avg_predicted_demand",
            "avg_actual_price", "avg_predicted_price",
            "avg_sentiment", "avg_ad_index",
            "total_stat_events", "total_top_peaks", "total_bottom_peaks",
            "change_point", "shift_strength"]
    available = [c for c in cols if c in daily.columns]
    df = daily[available].copy()
    df = df.where(pd.notnull(df), None)
    return df.to_dict("records")

@app.get("/api/forecast")
def api_forecast():
    """Price forecast from remote API cache."""
    if forecast_df.empty:
        return []
    df = forecast_df[["date", "forecast_avg_price"]].copy()
    df = df.where(pd.notnull(df), None)
    return df.to_dict("records")

@app.get("/api/regime-shifts")
def api_regime_shifts():
    """Structural regime shift dates from remote API cache."""
    if regime_df.empty:
        return []
    df = regime_df[["date", "regime_type", "shift_strength",
                    "marker_label", "event_explanation", "narrative"]].copy()
    df = df.where(pd.notnull(df), None)
    return df.to_dict("records")

@app.get("/api/alerts")
def api_alerts(limit: int = 20, brand: str = None):
    """Top signal alerts from remote API cache."""
    if alerts_df.empty:
        return []
    df = alerts_df.copy()
    if brand:
        df = df[df["brand"].str.lower() == brand.lower()]
    df = df.sort_values("event_severity_score", ascending=False).head(limit)
    cols = ["date", "entity_id", "brand", "category", "marker_type",
            "alert_title", "event_explanation", "narrative",
            "event_severity_score", "demand_index", "price_index",
            "sentiment_index", "search_index", "ad_index"]
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols].where(pd.notnull(df[available_cols]), None)
    return df.to_dict("records")

@app.get("/api/product/{entity_id}/overview")
def api_product_overview(entity_id: str):
    """Latest KPI snapshot for a single product."""
    row = latest_master[latest_master["entity_id"] == str(entity_id)]
    if row.empty:
        return JSONResponse({"error": "not found"}, 404)
    r = row.iloc[0]
    return {
        "entity_id": r["entity_id"],
        "brand": r["brand"],
        "product_name": r["product_name"],
        "category": r["category"],
        "demand_index": round(float(r["demand_index"]), 2),
        "price_index": round(float(r["price_index"]), 2),
        "sentiment_index": round(float(r["sentiment_index"]), 4),
        "search_index": round(float(r["search_index"]), 2),
        "ad_index": round(float(r["ad_index"]), 2),
        "health_index": round(float(r.get("health_index", 0)), 2),
        "list_price": round(float(r.get("list_price", 0)), 2),
    }

@app.get("/api/product/{entity_id}/series")
def api_product_series(entity_id: str):
    """Full time-series for a product from remote API cache."""
    if master.empty:
        return []
    df = master[master["entity_id"] == str(entity_id)].sort_values("date")
    if df.empty:
        return JSONResponse({"error": "not found"}, 404)
    cols = ["date", "demand_index", "price_index", "sentiment_index",
            "search_index", "ad_index", "health_index",
            "change_point", "shift_strength"]
    available = [c for c in cols if c in df.columns]
    df = df[available].where(pd.notnull(df[available]), None)
    return df.to_dict("records")

@app.get("/api/product/{entity_id}/markers")
def api_product_markers(entity_id: str):
    """Anomaly, peak, and dip markers for a product."""
    if markers_df.empty:
        return []
    df = markers_df[markers_df["entity_id"].astype(str) == str(entity_id)].copy()
    if df.empty:
        return []
    cols = ["date", "marker_type", "marker_label", "event_severity_score",
            "combined_marker_severity", "shift_strength",
            "demand_index", "price_index", "sentiment_index",
            "search_index", "ad_index",
            "event_explanation", "narrative"]
    available = [c for c in cols if c in df.columns]
    df = df[available].where(pd.notnull(df[available]), None)
    return df.to_dict("records")

@app.get("/api/product/{entity_id}/alerts")
def api_product_alerts(entity_id: str, limit: int = 10):
    """Top alerts for a specific product."""
    if alerts_df.empty:
        return []
    df = alerts_df[alerts_df["entity_id"].astype(str) == str(entity_id)].copy()
    df = df.sort_values("event_severity_score", ascending=False).head(limit)
    cols = ["date", "marker_type", "alert_title", "event_explanation",
            "narrative", "event_severity_score",
            "demand_index", "price_index", "sentiment_index"]
    available = [c for c in cols if c in df.columns]
    df = df[available].where(pd.notnull(df[available]), None)
    return df.to_dict("records")

@app.get("/api/product/{entity_id}/events")
def api_product_events(entity_id: str):
    """Ad campaigns, product announcements, upgrades for a product."""
    if events_df.empty:
        return []
    df = events_df[events_df["entity_id"].astype(str) == str(entity_id)].copy()
    if df.empty:
        return []
    cols = ["date", "brand", "product_name", "event_type", "event_title",
            "event_description", "impact_direction", "priority",
            "linked_marker_date", "linked_marker_type", "signal_story"]
    available = [c for c in cols if c in df.columns]
    df = df[available].where(pd.notnull(df[available]), None)
    return df.to_dict("records")

@app.post("/api/explain")
async def api_explain(data: dict = Body(...)):
    """AI-powered explanation — proxied to remote HF engine."""
    entity_id = data.get("entity_id")
    date = data.get("date")
    
    # Proxy to remote
    res = await hf_fetch(f"/api/explain", params={"entity_id": str(entity_id), "target_date": date})
    if res and res.get("status") == "ok" and res.get("rows"):
        row = res["rows"][0]
        # Format like the UI expects
        return {"answer": row.get("explanation_text", "No detailed explanation available.")}
    
    return {"answer": "AI Engine is currently unavailable. Please try again later."}

@app.get("/api/category/{category}/summary")
def api_category_summary(category: str):
    """Aggregate metrics for all products in a category."""
    df = latest_master[latest_master["category"].str.lower() == category.lower()]
    if df.empty:
        return JSONResponse({"error": "not found"}, 404)
    return {
        "category": category,
        "count": int(len(df)),
        "avg_demand": round(float(df["demand_index"].mean()), 2),
        "avg_price": round(float(df["price_index"].mean()), 2),
        "avg_sentiment": round(float(df["sentiment_index"].mean() * 100), 1),
        "avg_ad_index": round(float(df["ad_index"].mean()), 2),
        "avg_health": round(float(df["health_index"].mean()), 2),
        "top_brand": df.nlargest(1, "health_index").iloc[0]["brand"],
        "products": df[["entity_id", "brand", "product_name",
                        "demand_index", "price_index",
                        "sentiment_index", "health_index"]]
                   .sort_values("health_index", ascending=False)
                   .to_dict("records"),
    }


# ═════════════════════════════════════════════════════════════════════════════
# AUTONOMOUS LOOP ROUTES — /api/autonomous/*
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/api/autonomous/status")
async def autonomous_status():
    """Proxied simulation status from remote HF engine."""
    res = await hf_fetch("/api/autonomous/status")
    if res:
        return res
    return {"status": "error", "message": "Remote engine unreachable"}

@app.post("/api/realtime/ingest-next-date")
async def realtime_ingest_next():
    """Trigger remote simulation cycle."""
    res = await hf_fetch("/api/engine/run-cycle", method="POST")
    if res:
        # After ingest, we SHOULD refresh our local cache to show latest actuals
        await refresh_all_data()
        return res
    return {"status": "error", "message": "Remote ingest failed"}

@app.get("/api/realtime/drift")
async def realtime_drift():
    """Rolling drift metrics proxied from remote HF engine."""
    res = await hf_fetch("/api/realtime/drift")
    if res:
        # Adapt to UI structure if needed
        return {
            "current_drift": res.get("mae", 0.0), # Example mapping
            "drift_flag": res.get("drift_status") != "stable",
            "threshold": 5.0,
            "history": [] # Remote doesn't expose history easily in current API
        }
    return {"status": "error"}

@app.get("/api/realtime/forecast-vs-actual")
async def realtime_fva(limit: int = 50):
    """Recent forecast vs actual comparison from remote HF engine."""
    res = await hf_fetch("/api/history/forecast-vs-actual", params={"limit": limit})
    if res:
        return {"count": res.get("count", 0), "rows": res.get("rows", [])}
    return {"count": 0, "rows": []}


# ─────────────────────────────────────────
# MODEL ROUTES — /api/model/*
# ─────────────────────────────────────────

@app.post("/api/model/retrain")
async def model_retrain():
    """Manually trigger a model retrain on the remote engine."""
    res = await hf_fetch("/models/retrain", method="POST")
    return res or {"status": "error"}

@app.get("/api/model/status")
async def model_status():
    """Current model status from remote engine."""
    res = await hf_fetch("/models/active")
    return res or {"status": "error"}