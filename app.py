"""SevenSlots Streaming Dashboard - FastAPI Backend."""
import hashlib
import os
import secrets
from fastapi import FastAPI, Request, Form, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

import db
import youtube_api as yt

SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app = FastAPI(title="SevenSlots Dashboard")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Simple token store (in-memory, survives within a single process)
_valid_tokens: set[str] = set()


def _make_token(username: str) -> str:
    raw = f"{username}:{SECRET_KEY}:{secrets.token_hex(16)}"
    token = hashlib.sha256(raw.encode()).hexdigest()
    _valid_tokens.add(token)
    return token


def _check_auth(request: Request) -> bool:
    token = request.cookies.get("ss_token")
    return token in _valid_tokens if token else False


# ── Auth middleware ──

class AuthMiddleware(BaseHTTPMiddleware):
    OPEN_PATHS = {"/login", "/oauth/callback"}

    async def dispatch(self, request, call_next):
        path = request.url.path
        if path in self.OPEN_PATHS or path.startswith("/static"):
            return await call_next(request)
        if not _check_auth(request):
            if path.startswith("/api/") or path == "/oauth/status":
                return JSONResponse({"error": "unauthorized"}, status_code=401)
            return RedirectResponse("/login")
        return await call_next(request)

app.add_middleware(AuthMiddleware)


@app.on_event("startup")
def startup():
    try:
        db.init_db()
        print("[STARTUP] DB initialized OK")
        # Ensure required user accounts exist
        for uname in ("paul", "costi", "sevenslots"):
            try:
                db.create_user(uname, "Liv2026!")
                print(f"[STARTUP] Created user '{uname}'")
            except Exception:
                pass  # already exists
        # Remove old accounts
        for old in ("streamers", "managers"):
            db.delete_user(old)
        # One-time fix: migrate program data from 0-indexed to 1-indexed months
        db.migrate_program_months()
    except Exception as e:
        print(f"[STARTUP] DB init failed: {e}")


# ── Auth ──

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = None):
    if _check_auth(request):
        return RedirectResponse("/")
    return templates.TemplateResponse("login.html", {"request": request, "error": error})


@app.post("/login")
async def login_submit(username: str = Form(...), password: str = Form(...)):
    if db.verify_user(username, password):
        token = _make_token(username)
        resp = RedirectResponse("/", status_code=303)
        resp.set_cookie("ss_token", token, httponly=True, samesite="lax", max_age=86400 * 30)
        return resp
    return RedirectResponse("/login?error=1", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("ss_token")
    if token:
        _valid_tokens.discard(token)
    resp = RedirectResponse("/login")
    resp.delete_cookie("ss_token")
    return resp


# ── Pages ──

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    try:
        sessions = db.get_sessions()
        authenticated = yt.is_authenticated()
        return templates.TemplateResponse("index.html", {
            "request": request,
            "sessions": sessions,
            "authenticated": authenticated,
        })
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


# ── OAuth ──

@app.get("/oauth/login")
async def oauth_login():
    try:
        url = yt.start_oauth_flow()
        return RedirectResponse(url)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/oauth/callback")
async def oauth_callback(code: str):
    try:
        yt.complete_oauth_flow(code)
        return RedirectResponse("/?msg=youtube_connected")
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/oauth/status")
async def oauth_status():
    return {"authenticated": yt.is_authenticated()}


# ── YouTube Import ──

@app.post("/api/youtube/import")
async def youtube_import(streamer: str = Form("Seven"), year: int = Form(None), month: int = Form(None)):
    creds = yt.get_credentials()
    if not creds:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    print(f"[IMPORT] streamer={streamer} year={year} month={month}")
    videos = yt.fetch_live_streams(creds, year=year, month=month)
    print(f"[IMPORT] found {len(videos)} videos")
    imported = 0
    for v in videos:
        if db.session_exists_by_video_id(v["video_id"]):
            continue
        db.add_session({
            "streamer": streamer,
            "date": v["date"],
            "title": v["title"],
            "link": f"https://youtube.com/watch?v={v['video_id']}",
            "duration": v.get("duration", ""),
            "views": v.get("views", 0),
            "unique_viewers": v.get("unique_viewers", 0),
            "avg_duration": v.get("avg_duration", ""),
            "peak_concurrent": v.get("peak_concurrent", 0),
            "likes": v.get("likes", 0),
            "avg_viewers": v.get("avg_viewers", 0),
            "new_subs": v.get("new_subs", 0),
            "discord": 0,
            "casino": "",
            "provider": "",
            "video_id": v["video_id"],
            "note": "",
        })
        imported += 1

    return {"imported": imported, "total_found": len(videos)}


# ── Sessions API ──

@app.get("/api/sessions")
async def api_sessions(streamer: str = None):
    return db.get_sessions(streamer)


@app.post("/api/sessions")
async def api_add_session(
    streamer: str = Form(...),
    date: str = Form(...),
    title: str = Form(""),
    link: str = Form(""),
    duration: str = Form(""),
    views: int = Form(0),
    unique_viewers: int = Form(0),
    avg_duration: str = Form(""),
    peak_concurrent: int = Form(0),
    likes: int = Form(0),
    avg_viewers: int = Form(0),
    new_subs: int = Form(0),
    discord: int = Form(0),
    casino: str = Form(""),
    provider: str = Form(""),
):
    sid = db.add_session({
        "streamer": streamer,
        "date": date,
        "title": title,
        "link": link,
        "duration": duration,
        "views": views,
        "unique_viewers": unique_viewers,
        "avg_duration": avg_duration,
        "peak_concurrent": peak_concurrent,
        "likes": likes,
        "avg_viewers": avg_viewers,
        "new_subs": new_subs,
        "discord": discord,
        "casino": casino,
        "provider": provider,
        "video_id": "",
        "note": "",
    })
    return RedirectResponse("/?tab=stats", status_code=303)


@app.delete("/api/sessions/{sid}")
async def api_delete_session(sid: int):
    db.delete_session(sid)
    return {"ok": True}


# ── Program API ──

@app.get("/api/program/{year}/{month}")
async def api_get_program(year: int, month: int, streamer: str = None):
    return db.get_program(year, month, streamer)


@app.post("/api/program")
async def api_save_program(
    year: int = Form(...),
    month: int = Form(...),
    day: int = Form(...),
    streamer: str = Form(""),
    casino: str = Form(""),
    provider: str = Form(""),
):
    db.save_program_day(year, month, day, streamer, casino, provider)
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
