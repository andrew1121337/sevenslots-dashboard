"""SevenSlots Streaming Dashboard - FastAPI Backend."""
import base64
import hashlib
import os
import secrets
from fastapi import FastAPI, Request, Form, Cookie, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
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
        channels = yt.get_connected_channels()
        resp = templates.TemplateResponse("index.html", {
            "request": request,
            "sessions": sessions,
            "authenticated": authenticated,
            "channels": channels,
        })
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return resp
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


# ── OAuth ──

_pending_channel_name: str = "SevenSlots"


@app.get("/oauth/login")
async def oauth_login(channel: str = "SevenSlots"):
    global _pending_channel_name
    _pending_channel_name = channel
    try:
        url = yt.start_oauth_flow()
        return RedirectResponse(url)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/oauth/callback")
async def oauth_callback(code: str):
    try:
        yt.complete_oauth_flow(code, channel_name=_pending_channel_name)
        return RedirectResponse("/?msg=youtube_connected")
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/oauth/status")
async def oauth_status():
    channels = yt.get_connected_channels()
    return {"authenticated": len(channels) > 0, "channels": channels}


@app.delete("/oauth/channel/{channel_name}")
async def oauth_disconnect(channel_name: str):
    db.delete_channel_token(channel_name)
    return {"ok": True}


# ── YouTube Import ──

def _import_videos_to_db(videos: list, streamer: str) -> int:
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
    return imported


@app.post("/api/youtube/import")
async def youtube_import(streamer: str = Form("Seven"), channel: str = Form("SevenSlots"),
                         year: int = Form(None), month: int = Form(None)):
    creds = yt.get_credentials(channel)
    if not creds:
        return JSONResponse({"error": f"Channel '{channel}' not authenticated"}, status_code=401)

    print(f"[IMPORT] streamer={streamer} channel={channel} year={year} month={month}")
    videos = yt.fetch_live_streams(creds, year=year, month=month)
    print(f"[IMPORT] found {len(videos)} live streams")
    imported = _import_videos_to_db(videos, streamer)
    return {"imported": imported, "total_found": len(videos)}


@app.post("/api/youtube/import-videos")
async def youtube_import_videos(streamer: str = Form("Catalin"), channel: str = Form("Catalin"),
                                year: int = Form(None), month: int = Form(None)):
    creds = yt.get_credentials(channel)
    if not creds:
        return JSONResponse({"error": f"Channel '{channel}' not authenticated"}, status_code=401)

    print(f"[IMPORT-VIDEOS] streamer={streamer} channel={channel} year={year} month={month}")
    videos = yt.fetch_videos(creds, year=year, month=month)
    print(f"[IMPORT-VIDEOS] found {len(videos)} videos")
    imported = _import_videos_to_db(videos, streamer)
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
    done: int = Form(0),
):
    db.save_program_day(year, month, day, streamer, casino, provider, done)
    return {"ok": True}


# ── Thumbnails API ──

@app.post("/api/thumbnails/upload")
async def thumbnail_upload(streamer: str = Form(...), date: str = Form(...), file: UploadFile = File(...)):
    data = await file.read()
    if len(data) > 5 * 1024 * 1024:
        return JSONResponse({"error": "File too large (max 5MB)"}, status_code=400)
    b64 = base64.b64encode(data).decode()
    tid = db.add_thumbnail(streamer, date, file.filename, file.content_type or "image/png", b64)
    return {"ok": True, "id": tid}


@app.get("/api/thumbnails/download/{tid}")
async def thumbnail_download(tid: int):
    t = db.get_thumbnail(tid)
    if not t:
        return JSONResponse({"error": "not found"}, status_code=404)
    data = base64.b64decode(t["image_data"])
    return Response(content=data, media_type=t["content_type"],
                    headers={"Content-Disposition": f'attachment; filename="{t["filename"]}"'})


@app.get("/api/thumbnails/view/{tid}")
async def thumbnail_view(tid: int):
    t = db.get_thumbnail(tid)
    if not t:
        return JSONResponse({"error": "not found"}, status_code=404)
    data = base64.b64decode(t["image_data"])
    return Response(content=data, media_type=t["content_type"])


@app.delete("/api/thumbnails/{tid}")
async def thumbnail_delete(tid: int):
    db.delete_thumbnail(tid)
    return {"ok": True}


@app.get("/api/thumbnails/{year}/{month}")
async def thumbnail_list(year: int, month: int, streamer: str = None):
    return db.get_thumbnails_for_month(year, month, streamer)


# ── Meetings API ──

@app.get("/api/meetings")
async def api_meetings():
    return db.get_meetings()


@app.post("/api/meetings")
async def api_create_meeting(date: str = Form(...)):
    mid = db.create_meeting(date)
    return {"ok": True, "id": mid}


@app.delete("/api/meetings/{mid}")
async def api_delete_meeting(mid: int):
    db.delete_meeting(mid)
    return {"ok": True}


@app.get("/api/meetings/{mid}/tasks")
async def api_meeting_tasks(mid: int):
    return db.get_meeting_tasks(mid)


@app.post("/api/meetings/{mid}/tasks")
async def api_add_task(mid: int, text: str = Form(...)):
    tid = db.add_meeting_task(mid, text)
    return {"ok": True, "id": tid}


@app.put("/api/meetings/tasks/{tid}")
async def api_toggle_task(tid: int, done: int = Form(...)):
    db.toggle_meeting_task(tid, done)
    return {"ok": True}


@app.delete("/api/meetings/tasks/{tid}")
async def api_delete_task(tid: int):
    db.delete_meeting_task(tid)
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
