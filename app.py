"""SevenSlots Streaming Dashboard - FastAPI Backend."""
import os
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates

import db
import youtube_api as yt

app = FastAPI(title="SevenSlots Dashboard")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


@app.on_event("startup")
def startup():
    db.init_db()
    # Copy client secret if needed
    src = os.path.expanduser(
        "~/Downloads/client_secret_936904037043-a20308c78j5nnhuv08q94tq16ecr00fl.apps.googleusercontent.com.json"
    )
    dst = os.path.join(os.path.dirname(__file__), "client_secret.json")
    if not os.path.exists(dst) and os.path.exists(src):
        import shutil
        shutil.copy2(src, dst)


# ── Pages ──

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    sessions = db.get_sessions()
    authenticated = yt.is_authenticated()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "sessions": sessions,
        "authenticated": authenticated,
    })


# ── OAuth ──

@app.get("/oauth/login")
async def oauth_login():
    url = yt.start_oauth_flow()
    return RedirectResponse(url)


@app.get("/oauth/callback")
async def oauth_callback(code: str):
    yt.complete_oauth_flow(code)
    return RedirectResponse("/?msg=youtube_connected")


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
