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


def _import_prof_jan26():
    """One-time import of El Profesor January 2026 sessions."""
    # Check if already imported
    all_sess = db.get_sessions("El Profesor")
    jan_dates = [s["date"] for s in all_sess if s["date"].startswith("2026-01")]
    if len(jan_dates) >= 10:
        return  # Already imported
    ROWS = [
        ("2026-01-03","EYpsGLZOwdk","https://www.youtube.com/live/EYpsGLZOwdk","2:30:00",11000,6600,"9:11",1400,1000,930,0,0,"Superbet","",""),
        ("2026-01-05","t7kcmcAuUbs","https://www.youtube.com/live/t7kcmcAuUbs","2:35:00",6800,4500,"7:05",666,531,455,0,0,"","",""),
        ("2026-01-06","RExHYsXluHY","https://www.youtube.com/live/RExHYsXluHY","1:35:00",4900,3300,"5:06",510,232,345,0,0,"","",""),
        ("2026-01-07","","","4:00:00",0,0,"",0,0,0,0,0,"","",""),
        ("2026-01-08","6vSdwERKPo4","https://studio.youtube.com/video/6vSdwERKPo4","2:39:00",4427,2900,"5:01",364,320,240,0,0,"Netbet","Pragmatic",""),
        ("2026-01-09","fE_9cZMY8o8","https://youtube.com/live/fE_9cZMY8o8","2:50:00",7770,4600,"8:42",1067,557,731,0,0,"Betano","Pateplay",""),
        ("2026-01-11","","https://kick.com/sevenslots/videos/e3f6c579","2:06:00",0,0,"",0,0,0,0,0,"Betano","Pateplay",""),
        ("2026-01-12","","https://kick.com/sevenslots/videos/4aafb132","2:12:00",0,0,"",0,0,0,0,0,"","",""),
        ("2026-01-13","","https://kick.com/sevenslots/videos/555a4d81","2:19:00",0,0,"",0,0,0,0,0,"Netbet","Pateplay",""),
        ("2026-01-14","wKwHdkPcRig","https://youtube.com/live/wKwHdkPcRig","4:00:00",8403,5600,"12:20",1088,0,754,0,0,"","",""),
        ("2026-01-16","utE2Rk42oag","https://youtube.com/live/utE2Rk42oag","2:30:00",3800,2700,"6:07",357,0,259,0,0,"","Greentube",""),
        ("2026-01-17","KY2PDSZOfiA","https://www.youtube.com/watch?v=KY2PDSZOfiA","4:47:00",18100,10100,"15:44",2300,2500,1332,0,0,"","",""),
        ("2026-01-18","_jUwS89UtgM","https://www.youtube.com/watch?v=_jUwS89UtgM","3:13:00",10400,6000,"13:35",1635,1200,1074,0,0,"Joker","Pragmatic",""),
        ("2026-01-19","7PyX-zZw07c","https://www.youtube.com/watch?v=7PyX-zZw07c","3:10:00",11000,6800,"11:18",1503,1230,1097,0,0,"Superbet","",""),
        ("2026-01-20","SMptnp__yXY","https://www.youtube.com/watch?v=SMptnp__yXY","3:15:00",9000,5300,"10:39",967,643,643,0,0,"MrBit","Greentube",""),
        ("2026-01-21","vJbj0Dl6A44","https://www.youtube.com/watch?v=vJbj0Dl6A44","3:11:00",4248,3300,"13:42",1239,0,809,0,0,"Betano","Pragmatic","PRINCIPAL"),
        ("2026-01-22","","https://kick.com/sevenslots/videos/ea739f0d","3:01:00",0,0,"",0,0,0,0,0,"Superbet","",""),
        ("2026-01-23","jszr2yyaY5s","https://www.youtube.com/watch?v=jszr2yyaY5s","3:14:00",3000,2000,"",0,243,0,0,0,"Betano","Pragmatic",""),
        ("2026-01-26","jyoqtCOv9cc","https://youtube.com/live/jyoqtCOv9cc","2:10:00",2700,1900,"4:20",307,0,193,0,0,"","Greentube",""),
        ("2026-01-27","mDldUoDizUU","https://youtube.com/live/mDldUoDizUU","12:00:00",28896,14700,"21:28",2082,0,1030,0,0,"","Pragmatic",""),
        ("2026-01-28","1qaNDwEi4Fs","https://youtube.com/live/1qaNDwEi4Fs","12:00:00",30282,14500,"16:28",1849,0,993,0,0,"","Pragmatic",""),
        ("2026-01-30","n5JEz2oWD2A","https://www.youtube.com/live/n5JEz2oWD2A","2:00:00",2300,1600,"7:27",282,0,161,0,0,"WIN2","Pateplay",""),
    ]
    # Fix: delete old incomplete entries for days 14 and 16 Jan
    for s in all_sess:
        if s["date"] in ("2026-01-14","2026-01-16") and not s.get("video_id"):
            db.delete_session(s["id"])
    count = 0
    for date,vid,link,dur,views,uniq,avgd,peak,likes,avgv,subs,disc,cas,prov,title in ROWS:
        if vid and db.session_exists_by_video_id(vid):
            continue
        db.add_session({"streamer":"El Profesor","date":date,"title":title,"link":link,
            "duration":dur,"views":views,"unique_viewers":uniq,"avg_duration":avgd,
            "peak_concurrent":peak,"likes":likes,"avg_viewers":avgv,"new_subs":subs,
            "discord":disc,"casino":cas,"provider":prov,"video_id":vid,"note":""})
        count += 1
    if count:
        print(f"[STARTUP] Imported {count} El Profesor Jan 2026 sessions")


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
        # Seed licente if empty
        if db.licente_count() == 0:
            _LICENTE = [
                ("L1203785W001253","Magic Jackpot"),("L1170664W000663","Betano"),
                ("L1234166W001528","Winner"),("L1200662W001197","Favbet"),
                ("L1203785W001253","Superbet"),("L1173107W000815","Conti Cazino"),
                ("L1213823W001269","Magnum"),("L1213823W001269","Luck"),
                ("L1213823W001269","Winboss"),("L1183150W000826","Winbet"),
                ("L1173107W000815","Getsbet"),("L1213823W001269","ExcellBet"),
                ("L1213823W001269","Yoji"),("L1160661W000651","Maxbet"),
                ("L1213822W001268","Stanley"),("L1213822W001268","Gameworld"),
                ("L1234008W001473","Million"),("L1160651W000195","Netbet"),
                ("L1213854W001295","Mr Bit"),("L1160657W000330","Unibet"),
                ("L1160657W000330","Vlad Casino"),("L1213854W001295","Frank Casino"),
                ("L1213854W001295","SlotsV"),("L1234101W001500","Don"),
                ("L1234008W001473","Prima Casino"),("L1203414W001173","Mozzart"),
                ("L1213823W001269","Princess"),("L1234166W001528","Lady Casino"),
                ("L1234166W001528","Elite Slots"),("L1234166W001528","Bet 7"),
                ("L1234166W001528","Hot Spins"),("L1160650W000194","Pariuri Plus"),
                ("L1213823W001269","Powerbet"),("L1213823W001269","Wacko"),
                ("L1160650W000194","WinMaster"),("L1234144W001522","TotalBet"),
            ]
            for code, name in _LICENTE:
                db.add_licenta(code, name)
            print(f"[STARTUP] Seeded {len(_LICENTE)} licente")
        # Seed paysafes if empty
        existing_psf = db.get_paysafes()
        if not existing_psf:
            _PSF = [
                ("dav1dush",1,0,"rezolvat","Instagram"),
                ("byochan999",1,0,"rezolvat","Instagram"),
                ("acesiuu",1,0,"rezolvat","Instagram"),
                ("brkn8422",1,0,"rezolvat","Instagram"),
                ("alexx10922",0,1,"rezolvat","Discord"),
                ("caponaru_militaru",0,1,"rezolvat","Discord"),
                ("Andrei.dum.26",1,0,"rezolvat","Instagram"),
            ]
            for u,a,b,s,p in _PSF:
                db.add_paysafe(u,a,b,s,p)
            print(f"[STARTUP] Seeded {len(_PSF)} paysafes")
        # One-time fix: migrate program data from 0-indexed to 1-indexed months
        db.migrate_program_months()
        # One-time import: El Profesor January 2026
        _import_prof_jan26()
        # Set default targets if not already set
        t = db.get_all_targets(2026, 3)
        if "El Profesor" not in t or not t["El Profesor"].get("hours"):
            db.set_target("El Profesor", 2026, 3, 0, 60)
            print("[STARTUP] Set El Profesor target: 60h")
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


# ── Licente API ──

@app.get("/api/licente")
async def api_licente():
    return db.get_licente()


@app.post("/api/licente")
async def api_add_licenta(license_code: str = Form(...), casino_name: str = Form(...)):
    lid = db.add_licenta(license_code, casino_name)
    return {"ok": True, "id": lid}


@app.delete("/api/licente/{lid}")
async def api_delete_licenta(lid: int):
    db.delete_licenta(lid)
    return {"ok": True}


# ── PaySafe API ──

@app.get("/api/paysafes")
async def api_paysafes():
    return db.get_paysafes()


@app.post("/api/paysafes")
async def api_add_paysafe(username: str = Form(...), psf25: int = Form(0),
                           psf50: int = Form(0), status: str = Form(""),
                           platform: str = Form("Instagram")):
    pid = db.add_paysafe(username, psf25, psf50, status, platform)
    return {"ok": True, "id": pid}


@app.put("/api/paysafes/{pid}")
async def api_update_paysafe(pid: int, psf25: int = Form(0), psf50: int = Form(0),
                              status: str = Form("")):
    db.update_paysafe(pid, psf25, psf50, status)
    return {"ok": True}


@app.delete("/api/paysafes/{pid}")
async def api_delete_paysafe(pid: int):
    db.delete_paysafe(pid)
    return {"ok": True}


# ── Roata APP API ──

@app.get("/api/roata/{category}")
async def api_roata(category: str):
    return db.get_roata(category)


@app.post("/api/roata")
async def api_add_roata(category: str = Form(...), date: str = Form(...),
                         rotiri: str = Form(""), user_app: str = Form(""),
                         username_cazino: str = Form(""), status: str = Form("")):
    rid = db.add_roata(category, date, rotiri, user_app, username_cazino, status)
    return {"ok": True, "id": rid}


@app.put("/api/roata/edit/{rid}")
async def api_edit_roata(rid: int, rotiri: str = Form(""), user_app: str = Form(""),
                          username_cazino: str = Form(""), status: str = Form("")):
    db.update_roata_entry(rid, rotiri, user_app, username_cazino, status)
    return {"ok": True}


@app.put("/api/roata/{rid}")
async def api_update_roata(rid: int, status: str = Form(...)):
    db.update_roata_status(rid, status)
    return {"ok": True}


@app.delete("/api/roata/{rid}")
async def api_delete_roata(rid: int):
    db.delete_roata(rid)
    return {"ok": True}



# ── Targets ──

@app.get("/api/targets/{year}/{month}")
async def api_get_targets(year: int, month: int):
    try:
        return db.get_all_targets(year, month)
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/targets")
async def api_set_target(streamer: str = Form(...), year: int = Form(...),
                          month: int = Form(...), views_target: int = Form(0),
                          hours_target: int = Form(0)):
    db.set_target(streamer, year, month, views_target, hours_target)
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
