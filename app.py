"""SevenSlots Streaming Dashboard - FastAPI Backend."""
import base64
import hashlib
import os
import secrets
from fastapi import FastAPI, Request, Form, Cookie, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from fastapi.staticfiles import StaticFiles

import db
import youtube_api as yt

SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app = FastAPI(title="SevenSlots Dashboard")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Simple token store (in-memory, survives within a single process)
_valid_tokens: dict[str, str] = {}  # token -> username


def _make_token(username: str) -> str:
    raw = f"{username}:{SECRET_KEY}:{secrets.token_hex(16)}"
    token = hashlib.sha256(raw.encode()).hexdigest()
    _valid_tokens[token] = username
    return token


def _check_auth(request: Request) -> bool:
    token = request.cookies.get("ss_token")
    return token in _valid_tokens if token else False


def _get_user(request: Request) -> str:
    token = request.cookies.get("ss_token")
    return _valid_tokens.get(token, "unknown") if token else "unknown"


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


def _import_seven_feb26():
    """Import Seven February 2026 sessions (always replaces)."""
    for s in db.get_sessions("Seven"):
        if s["date"].startswith("2026-02"):
            db.delete_session(s["id"])
    ROWS = [
        # (date, video_id, link, duration, views, unique_viewers, avg_duration, peak, likes, avg_viewers, new_subs, discord, casino, provider, title)
        ("2026-02-01","1LLjnXHFTwU","https://www.youtube.com/live/1LLjnXHFTwU","3:30:00",8006,4000,"14:54:00",1222,810,826,19250,0,"Betano","",""),
        ("2026-02-02","dMR5tLJ1VDY","https://www.youtube.com/live/dMR5tLJ1VDY","3:30:00",17000,9800,"17:38:00",2101,1430,1632,19250,0,"MrBit","",""),
        ("2026-02-03","0--6DiwyYGw","https://www.youtube.com/watch?v=0--6DiwyYGw","4:03:00",18000,9200,"18:36:00",2131,1635,1565,19250,0,"Win2","",""),
        ("2026-02-04","CTfWzuVw0bc","https://www.youtube.com/watch?v=CTfWzuVw0bc","4:06:00",20000,11000,"26:42:00",2579,1700,1753,19250,0,"TopBet","",""),
        ("2026-02-05","cdN_oNHUE_E","https://www.youtube.com/live/cdN_oNHUE_E","3:51:00",18900,10700,"18:25:00",2264,1631,1712,19500,0,"Joker","",""),
        ("2026-02-06","m19F2zdhehU","https://www.youtube.com/live/m19F2zdhehU","4:00:00",18700,10100,"17:58:00",2032,1672,1606,19500,0,"Superbet","",""),
        ("2026-02-09","YGxjRVw03rc","https://www.youtube.com/watch?v=YGxjRVw03rc","3:34:00",17000,9300,"11:21:00",2073,1257,1468,19500,0,"Betano","",""),
        ("2026-02-10","4D4I92ZUGT8","https://www.youtube.com/watch?v=4D4I92ZUGT8","3:37:00",12000,6400,"",2064,1210,1359,19550,0,"MrBit","",""),
        ("2026-02-12","fsneGmeGQgs","https://www.youtube.com/live/fsneGmeGQgs","3:49:00",14250,8000,"",1867,1321,1275,19600,0,"Betano","",""),
        ("2026-02-13","-fYmOSWEwzc","https://www.youtube.com/watch?v=-fYmOSWEwzc","4:07:00",18800,11200,"18:54:00",1885,1575,1489,19650,0,"Joker","",""),
        ("2026-02-15","oonwAoLJmCE","https://www.youtube.com/watch?v=oonwAoLJmCE","3:54:00",18100,10300,"17:37:00",2161,1418,1686,19700,0,"Win2","",""),
        ("2026-02-16","8JOzuVvmrc4","https://www.youtube.com/watch?v=8JOzuVvmrc4","4:24:00",18300,10100,"19:04:00",2281,1482,1694,19800,0,"MrBit","",""),
        ("2026-02-17","12zvYpyQe1c","https://www.youtube.com/watch?v=12zvYpyQe1c","5:05:00",22300,10300,"18:40:00",2034,1679,1638,19800,0,"Joker","",""),
        ("2026-02-18","uxUEA6VYPIo","https://www.youtube.com/live/uxUEA6VYPIo","4:30:00",26200,14100,"21:13:00",2436,1537,1795,20000,0,"Win2","",""),
        ("2026-02-19","9LzB9henSis","https://youtube.com/live/9LzB9henSis","2:30:00",27700,18000,"22:05:00",3716,1947,2735,20100,0,"Cazino live","",""),
        ("2026-02-20","wuyv3M6dGOw","https://www.youtube.com/watch?v=wuyv3M6dGOw","4:55:00",25800,15000,"16:32:00",2355,1650,1750,20150,0,"Betano","",""),
        ("2026-02-22","Yoyvmb8Yazo","https://www.youtube.com/watch?v=Yoyvmb8Yazo","3:00:00",23800,14000,"10:27",3898,2070,2456,20250,0,"MrBit","",""),
        ("2026-02-23","QyoQV1PLfh0","https://www.youtube.com/live/QyoQV1PLfh0","5:11:18",24000,12300,"17:27:00",2555,2064,1982,20300,0,"Betano","",""),
        ("2026-02-24","OGQb7G9hPmQ","https://www.youtube.com/live/OGQb7G9hPmQ","3:49:09",29850,14700,"16:24:00",3063,2167,2454,20300,0,"Joker","",""),
        ("2026-02-25","9JntKpBhsB4","https://www.youtube.com/watch?v=9JntKpBhsB4","4:00:00",13000,6600,"16:52:00",1613,1146,1203,20300,0,"GetsBet","",""),
        ("2026-02-26","1rc--bW5Pr4","https://www.youtube.com/live/1rc--bW5Pr4","3:25:00",20150,11400,"15:49:00",2468,1747,1904,20300,0,"Win2","",""),
        ("2026-02-27","ghIuuLBf5zI","https://www.youtube.com/watch?v=ghIuuLBf5zI","4:10:00",20600,11600,"17:37:00",2470,1775,1797,20300,0,"Betano","",""),
        ("2026-02-28","JVARun-vDds","https://www.youtube.com/watch?v=JVARun-vDds","3:00:00",15600,9200,"16:56:00",1227,1273,833,20350,0,"","",""),
    ]
    # Delete any session with conflicting video_ids (from YouTube API imports)
    vids = {r[1] for r in ROWS if r[1]}
    for s in db.get_sessions():
        if s.get("video_id") and s["video_id"] in vids:
            db.delete_session(s["id"])
    count = 0
    for date,vid,link,dur,views,uniq,avgd,peak,likes,avgv,subs,disc,cas,prov,title in ROWS:
        db.add_session({"streamer":"Seven","date":date,"title":title,"link":link,
            "duration":dur,"views":views,"unique_viewers":uniq,"avg_duration":avgd,
            "peak_concurrent":peak,"likes":likes,"avg_viewers":avgv,"new_subs":subs,
            "discord":disc,"casino":cas,"provider":prov,"video_id":vid,"note":""})
        count += 1
    if count:
        print(f"[STARTUP] Imported {count} Seven Feb 2026 sessions")


def _import_seven_mar26():
    """Import Seven March 2026 sessions (always replaces)."""
    for s in db.get_sessions("Seven"):
        if s["date"].startswith("2026-03"):
            db.delete_session(s["id"])
    ROWS = [
        # (date, video_id, link, duration, views, unique_viewers, avg_duration, peak, likes, avg_viewers, new_subs, discord, casino, provider, title)
        ("2026-03-01","oYcP0O9Znr8","https://www.youtube.com/watch?v=oYcP0O9Znr8","4:41:30",27900,15800,"18:17:00",3526,2362,2523,20500,0,"","",""),
        ("2026-03-02","25veVpoZM_I","https://www.youtube.com/watch?v=25veVpoZM_I","7:20:00",27000,13800,"22:06:00",3038,2299,2009,20500,0,"MrBit","",""),
        ("2026-03-03","lh44lNU7iVU","https://www.youtube.com/watch?v=lh44lNU7iVU","3:50:00",22600,13000,"14:46:00",2748,1638,1971,20500,0,"Winboss","",""),
        ("2026-03-04","Wg8NCbVYjLE","https://www.youtube.com/watch?v=Wg8NCbVYjLE","3:31:00",21500,12700,"12:44:00",2655,1673,1905,20500,0,"Netbet","",""),
        ("2026-03-05","AQ7H-ZfUaT8","https://www.youtube.com/watch?v=AQ7H-ZfUaT8","4:00:00",35300,18900,"14:12:00",3903,3212,2650,21100,0,"Superbet","",""),
        ("2026-03-06","rvlAIY5Olw4","https://www.youtube.com/watch?v=rvlAIY5Olw4","3:42:00",19500,11000,"14:00:00",1786,1200,1319,21100,0,"Win2","",""),
        ("2026-03-09","orFYHxxgNQQ","https://www.youtube.com/watch?v=orFYHxxgNQQ","4:50:00",22000,12900,"19:59:00",2730,1800,2088,21150,0,"Conti","",""),
        ("2026-03-10","DKgt1bsyqg4","https://www.youtube.com/watch?v=DKgt1bsyqg4","4:00:00",21000,12200,"12:19:00",2036,1253,1447,21200,0,"Winboss","",""),
        ("2026-03-11","hkLlpmmp1Io","https://www.youtube.com/watch?v=hkLlpmmp1Io","3:36:00",10000,5700,"15:54:00",1294,640,934,21200,0,"Win2","",""),
        ("2026-03-12","TG2ZxsiJIUE","https://www.youtube.com/watch?v=TG2ZxsiJIUE","6:03:00",23500,12500,"17:33:00",2522,2481,1807,21200,0,"Maxbet","",""),
        ("2026-03-13","BOw8BuehSrg","https://www.youtube.com/watch?v=BOw8BuehSrg","4:27:00",21000,11800,"19:30:00",2323,1360,1728,21200,0,"Win2","",""),
        ("2026-03-16","el5fkCD78SU","https://www.youtube.com/watch?v=el5fkCD78SU","5:13:00",20000,0,"31:42:00",2459,1700,1888,21200,0,"Gets","",""),
        ("2026-03-17","dPhirpKxYgI","https://www.youtube.com/watch?v=dPhirpKxYgI","4:35:00",22900,0,"",1562,0,0,21400,0,"Maxbet","",""),
        ("2026-03-18","OlHsGlIYOm4","https://www.youtube.com/live/OlHsGlIYOm4","3:50:00",18500,0,"",1120,0,0,21500,0,"Joker","",""),
        ("2026-03-19","8QJ6PDLfwkI","https://www.youtube.com/live/8QJ6PDLfwkI","3:08:00",17500,0,"",1355,0,0,21500,0,"Superbet&Napoleon","",""),
        ("2026-03-20","DUBAjhzhyN4","https://www.youtube.com/live/DUBAjhzhyN4","5:04:00",25900,0,"",1468,0,0,21800,0,"MrBit","",""),
        ("2026-03-21","","","3:04:30",31000,0,"",2608,0,0,0,0,"","",""),
        ("2026-03-22","EQXG8tTChwg","https://www.youtube.com/live/EQXG8tTChwg","4:11:00",27000,0,"",2269,0,0,21900,0,"Netbet","",""),
        ("2026-03-23","0DrWi-uPz4w","https://www.youtube.com/live/0DrWi-uPz4w","3:58:00",25700,0,"",1819,0,0,21900,0,"Betano","",""),
        ("2026-03-24","G-0kzeVNHsU","https://www.youtube.com/watch?v=G-0kzeVNHsU","5:05:00",20000,0,"",1567,0,0,22100,0,"Mrbit","",""),
        ("2026-03-25","pbaqk1KQQJE","https://www.youtube.com/live/pbaqk1KQQJE","3:35:21",17000,0,"",1383,0,0,22150,0,"Napoleon","",""),
    ]
    # Delete any session with conflicting video_ids
    vids = {r[1] for r in ROWS if r[1]}
    for s in db.get_sessions():
        if s.get("video_id") and s["video_id"] in vids:
            db.delete_session(s["id"])
    count = 0
    for date,vid,link,dur,views,uniq,avgd,peak,likes,avgv,subs,disc,cas,prov,title in ROWS:
        db.add_session({"streamer":"Seven","date":date,"title":title,"link":link,
            "duration":dur,"views":views,"unique_viewers":uniq,"avg_duration":avgd,
            "peak_concurrent":peak,"likes":likes,"avg_viewers":avgv,"new_subs":subs,
            "discord":disc,"casino":cas,"provider":prov,"video_id":vid,"note":""})
        count += 1
    if count:
        print(f"[STARTUP] Imported {count} Seven Mar 2026 sessions")


def _import_prof_feb26():
    """Import El Profesor February 2026 sessions (always replaces)."""
    for s in db.get_sessions("El Profesor"):
        if s["date"].startswith("2026-02"):
            db.delete_session(s["id"])
    ROWS = [
        # (date, video_id, link, duration, views, unique_viewers, avg_duration, peak, likes, avg_viewers, new_subs, discord, casino, provider, title)
        ("2026-02-03","","https://kick.com/sevenslots/videos/9980273e-128a-4728-9e82-73005422f25e","2:30:00",0,0,"",0,0,0,0,0,"","",""),
        ("2026-02-04","zYfvHTbUb7Y","https://www.youtube.com/watch?v=zYfvHTbUb7Y","2:20:00",1800,1300,"5:00:00",232,150,135,0,0,"","",""),
        ("2026-02-05","Dr6G08l-glI","https://www.youtube.com/watch?v=Dr6G08l-glI","2:20:00",2800,1800,"5:11:00",234,159,172,0,0,"","",""),
        ("2026-02-07","2UpNN27XfXk","https://www.youtube.com/watch?v=2UpNN27XfXk","3:34:00",10396,6100,"11:33:00",1565,933,865,0,0,"","",""),
        ("2026-02-08","mMewXnbZV9E","https://www.youtube.com/watch?v=mMewXnbZV9E","3:06:00",11282,6900,"10:53:00",1589,1110,1089,0,0,"","",""),
        ("2026-02-09","W1nHmMj6dzY","https://www.youtube.com/watch?v=W1nHmMj6dzY","2:40:00",2560,1900,"4:43:00",335,200,214,0,0,"","",""),
        ("2026-02-10","KrKQT1cfYbk","https://youtube.com/live/KrKQT1cfYbk","2:10:00",3569,2300,"8:55:00",334,0,226,0,0,"","",""),
        ("2026-02-12","mNiM50y4BJw","https://youtube.com/live/mNiM50y4BJw","2:12:00",6100,4300,"12:29:00",732,526,442,0,0,"","",""),
        ("2026-02-14","","https://youtube.com/live/wKwHdkPcRig","3:01:00",8500,5400,"12:40:00",1088,712,754,0,0,"","",""),
        ("2026-02-16","","https://youtube.com/live/utE2Rk42oag","2:24:00",0,2700,"6:19",359,232,262,0,0,"","",""),
        ("2026-02-17","r7S5s6sLC0M","https://youtube.com/live/r7S5s6sLC0M","2:11:00",0,3000,"9:24:00",452,214,309,0,0,"","",""),
        ("2026-02-19","","","5:00:00",0,0,"",0,0,0,0,0,"","",""),
        ("2026-02-21","EcEyhfXOals","https://youtube.com/live/EcEyhfXOals","3:15:00",11000,7600,"12:20:00",1645,1053,1017,0,0,"","",""),
        ("2026-02-23","Yet7SBQziEo","https://youtube.com/live/Yet7SBQziEo","2:19:00",5700,4000,"8:59:00",707,449,438,0,0,"","",""),
        ("2026-02-24","HYGCfl2mzbA","https://youtube.com/live/HYGCfl2mzbA","2:36:00",4300,2700,"6:14:00",447,0,248,0,0,"","",""),
        ("2026-02-25","","https://youtube.com/live/9JntKpBhsB4","4:00:00",15000,0,"",0,0,0,0,0,"","",""),
        ("2026-02-26","","","2:37:00",0,0,"",0,0,0,0,0,"","",""),
        ("2026-02-28","","","3:19:00",0,0,"",0,0,0,0,0,"","",""),
    ]
    # Delete any session with conflicting video_ids
    vids = {r[1] for r in ROWS if r[1]}
    for s in db.get_sessions():
        if s.get("video_id") and s["video_id"] in vids:
            db.delete_session(s["id"])
    count = 0
    for date,vid,link,dur,views,uniq,avgd,peak,likes,avgv,subs,disc,cas,prov,title in ROWS:
        db.add_session({"streamer":"El Profesor","date":date,"title":title,"link":link,
            "duration":dur,"views":views,"unique_viewers":uniq,"avg_duration":avgd,
            "peak_concurrent":peak,"likes":likes,"avg_viewers":avgv,"new_subs":subs,
            "discord":disc,"casino":cas,"provider":prov,"video_id":vid,"note":""})
        count += 1
    if count:
        print(f"[STARTUP] Imported {count} El Profesor Feb 2026 sessions")


def _import_prof_mar26():
    """Import El Profesor March 2026 sessions (always replaces)."""
    for s in db.get_sessions("El Profesor"):
        if s["date"].startswith("2026-03"):
            db.delete_session(s["id"])
    ROWS = [
        # (date, video_id, link, duration, views, unique_viewers, avg_duration, peak, likes, avg_viewers, new_subs, discord, casino, provider, title)
        ("2026-03-01","mJHDtoKIc9Q","https://youtube.com/live/mJHDtoKIc9Q","3:18:00",9000,5400,"12:43:00",1229,670,699,0,0,"","",""),
        ("2026-03-02","hVjplzPoT5M","https://youtube.com/live/hVjplzPoT5M","2:12:00",5400,3900,"9:08:00",574,341,416,0,0,"","",""),
        ("2026-03-03","JfwMiQC-beg","https://youtube.com/live/JfwMiQC-beg","2:08:00",6600,4700,"9:33",774,431,416,0,0,"","",""),
        ("2026-03-05","7jmN6duTMkg","https://youtube.com/live/7jmN6duTMkg","2:19:00",4900,3600,"6:20:00",603,0,305,0,0,"","",""),
        ("2026-03-07","CZa_TXVBGuw","https://youtube.com/live/CZa_TXVBGuw","3:11:00",7900,4400,"14:26:00",979,624,639,0,0,"Winboss","",""),
        ("2026-03-09","p5n-onHYL3I","https://youtube.com/live/p5n-onHYL3I","2:09:00",4000,3300,"8:37:00",429,234,268,0,0,"Netbet","",""),
        ("2026-03-10","wHrP7vKirJY","https://youtube.com/live/wHrP7vKirJY","2:18:00",2800,1800,"8:26:00",256,210,175,0,0,"","",""),
        ("2026-03-11","","https://youtube.com/live/hkLlpmmp1Io","3:38:00",9400,7672,"23:55:00",1288,604,916,0,0,"","",""),
        ("2026-03-12","RrXjTBXTlpI","https://youtube.com/live/RrXjTBXTlpI","2:10:00",3400,2400,"10:50:00",426,264,247,0,0,"Winboss","",""),
        ("2026-03-14","0Jo4WDgVvKw","https://youtube.com/live/0Jo4WDgVvKw","3:07:00",8100,5100,"11:30:00",1094,657,658,0,0,"luck","",""),
        ("2026-03-15","olrqyYMngO0","https://youtube.com/live/olrqyYMngO0","3:05:00",7500,4800,"10:21:00",1006,541,554,0,0,"","",""),
        ("2026-03-16","d8E1BCi11XI","https://youtube.com/live/d8E1BCi11XI","3:00:00",6100,4000,"8:34:00",727,518,417,0,0,"","",""),
        ("2026-03-17","P2CJz7xUF_E","https://youtube.com/live/P2CJz7xUF_E","2:50:00",4666,2300,"10:57:00",402,236,270,0,0,"Winboss","",""),
        ("2026-03-18","","https://youtube.com/live/OlHsGlIYOm4","3:50:00",18000,11000,"19:18:00",2172,0,1708,0,0,"","",""),
        ("2026-03-19","x7bdORnS02Y","https://youtube.com/live/x7bdORnS02Y","3:00:00",5400,3900,"7:48:00",438,0,288,0,0,"Luck","",""),
        ("2026-03-21","ij5DSSFl6lA","https://youtube.com/live/ij5DSSFl6lA","3:03:00",41000,24000,"28:18:00",4132,0,3065,0,0,"Betano","",""),
        ("2026-03-23","-sRrf2p_waw","https://youtube.com/live/-sRrf2p_waw","2:30:00",4900,3300,"7:53:00",716,0,382,0,0,"Betano","",""),
        ("2026-03-24","XgexN9mPWl8","https://youtube.com/live/XgexN9mPWl8","2:30:00",4000,2700,"10:47:00",350,0,250,0,0,"Unibet","",""),
        ("2026-03-25","","https://youtube.com/live/pbaqk1KQQJE","3:36:00",17000,7100,"",2392,0,1809,0,0,"","",""),
    ]
    # Delete any session with conflicting video_ids
    vids = {r[1] for r in ROWS if r[1]}
    for s in db.get_sessions():
        if s.get("video_id") and s["video_id"] in vids:
            db.delete_session(s["id"])
    count = 0
    for date,vid,link,dur,views,uniq,avgd,peak,likes,avgv,subs,disc,cas,prov,title in ROWS:
        db.add_session({"streamer":"El Profesor","date":date,"title":title,"link":link,
            "duration":dur,"views":views,"unique_viewers":uniq,"avg_duration":avgd,
            "peak_concurrent":peak,"likes":likes,"avg_viewers":avgv,"new_subs":subs,
            "discord":disc,"casino":cas,"provider":prov,"video_id":vid,"note":""})
        count += 1
    if count:
        print(f"[STARTUP] Imported {count} El Profesor Mar 2026 sessions")


@app.on_event("startup")
def startup():
    try:
        db.init_db()
        print("[STARTUP] DB initialized OK")
        # Ensure required user accounts exist
        for uname in ("paul", "costi", "sevenslots", "catalin"):
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
        # One-time imports
        _import_prof_jan26()
        _import_prof_feb26()
        _import_seven_feb26()
        _import_seven_mar26()
        _import_prof_mar26()
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
        db.log_activity(username, "Login")
        resp = RedirectResponse("/", status_code=303)
        resp.set_cookie("ss_token", token, httponly=True, samesite="lax", max_age=86400 * 30)
        return resp
    return RedirectResponse("/login?error=1", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("ss_token")
    if token:
        _valid_tokens.pop(token, None)
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
            "current_user": _get_user(request),
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
    request: Request,
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
    db.log_activity(_get_user(request), "Sesiune adaugata", f"{streamer} — {date} — {title or 'fara titlu'} ({duration})")
    return RedirectResponse("/?tab=stats", status_code=303)


@app.delete("/api/sessions/{sid}")
async def api_delete_session(request: Request, sid: int):
    db.log_activity(_get_user(request), "Sesiune stearsa", f"ID #{sid}")
    db.delete_session(sid)
    return {"ok": True}


# ── Program API ──

@app.get("/api/program/{year}/{month}")
async def api_get_program(year: int, month: int, streamer: str = None):
    return db.get_program(year, month, streamer)


@app.post("/api/program")
async def api_save_program(
    request: Request,
    year: int = Form(...),
    month: int = Form(...),
    day: int = Form(...),
    streamer: str = Form(""),
    casino: str = Form(""),
    provider: str = Form(""),
    done: int = Form(0),
):
    db.save_program_day(year, month, day, streamer, casino, provider, done)
    db.log_activity(_get_user(request), "Program modificat", f"{day}/{month}/{year} — {streamer} — {casino}/{provider}")
    return {"ok": True}


# ── Thumbnails API ──

@app.post("/api/thumbnails/upload")
async def thumbnail_upload(request: Request, streamer: str = Form(...), date: str = Form(...), file: UploadFile = File(...)):
    data = await file.read()
    if len(data) > 5 * 1024 * 1024:
        return JSONResponse({"error": "File too large (max 5MB)"}, status_code=400)
    b64 = base64.b64encode(data).decode()
    tid = db.add_thumbnail(streamer, date, file.filename, file.content_type or "image/png", b64)
    db.log_activity(_get_user(request), "Thumbnail uploadat", f"{streamer} — {date} — {file.filename}")
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


@app.get("/api/activity")
async def api_activity(request: Request, limit: int = 100):
    if _get_user(request) not in ADMIN_USERS:
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    return db.get_activity_log(limit)


ADMIN_USERS = {"costi", "paul", "catalin"}

@app.post("/api/targets")
async def api_set_target(request: Request, streamer: str = Form(...), year: int = Form(...),
                          month: int = Form(...), views_target: int = Form(0),
                          hours_target: int = Form(0)):
    user = _get_user(request)
    if user not in ADMIN_USERS:
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    db.set_target(streamer, year, month, views_target, hours_target)
    db.log_activity(user, "Target modificat", f"{streamer} — {month}/{year} — views:{views_target} ore:{hours_target}")
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
