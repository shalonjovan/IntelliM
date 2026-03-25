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

# ---------- ENV ----------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
SESSION_SECRET = os.getenv("SESSION_SECRET")

if not MONGO_URI or not SESSION_SECRET:
    raise RuntimeError("Missing env vars")

# ---------- DB ----------
db = get_db(MONGO_URI)
users = db["users"]
messages = db["messages"]

# ---------- APP ----------
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
templates = Jinja2Templates(directory="templates")

# ---------- OAUTH ----------
oauth = OAuth()
oauth.register(
    name="google",
    client_id=os.getenv("GOOGLE_CLIENT_ID"),
    client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

# ---------- HELPERS ----------
def require_user(request: Request):
    user = request.session.get("user")
    if not user:
        return None
    return user

def get_chat_history(user_id: str, chat_id: str, limit: int = 20):
    cursor = (
        messages.find(
            {"user_id": user_id, "chat_id": chat_id}
        )
        .sort("timestamp", 1)
        .limit(limit)
    )

    history = ["You are a helpful chatbot.\n"]
    for m in cursor:
        role = "User" if m["role"] == "user" else "Assistant"
        history.append(f"{role}: {m['content']}")

    return history

# ---------- ROUTES ----------
@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/login")
async def login(request: Request):
    return await oauth.google.authorize_redirect(
        request, request.url_for("auth_callback")
    )

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

@app.get("/dashboard")
def dashboard(request: Request):
    if not require_user(request):
        return RedirectResponse("/")
    return templates.TemplateResponse("dashboard.html", {"request": request})

# ---------- CHAT API ----------
@app.post("/chat/new")
def new_chat(request: Request):
    user = require_user(request)
    if not user:
        return JSONResponse({"error": "unauth"}, 401)

    chat_id = str(uuid.uuid4())
    return {"chat_id": chat_id}

@app.get("/chats")
def list_chats(request: Request):
    user = require_user(request)
    if not user:
        return JSONResponse({"error": "unauth"}, 401)

    chat_ids = messages.distinct(
        "chat_id",
        {"user_id": user["id"]}
    )

    return {"chats": chat_ids}

@app.get("/chat/{chat_id}")
def load_chat(chat_id: str, request: Request):
    user = require_user(request)
    if not user:
        return JSONResponse({"error": "unauth"}, 401)

    docs = messages.find(
        {"user_id": user["id"], "chat_id": chat_id}
    ).sort("timestamp", 1)

    return {
        "messages": [
            {"role": d["role"], "content": d["content"]}
            for d in docs
        ]
    }

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
        "user_id": user["id"],
        "chat_id": chat_id,
        "role": "user",
        "content": msg,
        "timestamp": datetime.utcnow()
    })

    history = get_chat_history(user["id"], chat_id)
    reply = chat_with_gemini(history)

    messages.insert_one({
        "user_id": user["id"],
        "chat_id": chat_id,
        "role": "assistant",
        "content": reply,
        "timestamp": datetime.utcnow()
    })

    return {"reply": reply}

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/")
