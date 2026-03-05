"""Database for SevenSlots Dashboard. Uses PostgreSQL if DATABASE_URL is set, else SQLite."""
import os
import sqlite3
from datetime import datetime, date
from urllib.parse import urlparse

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    if DATABASE_URL:
        import pg8000.native
        import ssl
        p = urlparse(DATABASE_URL)
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        conn = pg8000.native.Connection(
            user=p.username, password=p.password,
            host=p.hostname, port=p.port or 5432,
            database=p.path.lstrip("/"),
            ssl_context=ssl_ctx,
        )
        return conn
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), "sevenslots.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _pg_run(conn, sql, params=None):
    """Run SQL on pg8000 native connection."""
    return conn.run(sql, **({"parameters": params} if params else {}))


def _pg_columns(conn):
    """Get column names from last query."""
    return [d["name"] for d in conn.columns] if conn.columns else []


def _serialize(val):
    """Convert non-JSON-serializable types to strings."""
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return val


def _pg_to_dicts(conn, rows):
    """Convert pg8000 rows to list of dicts with serializable values."""
    cols = _pg_columns(conn)
    if not cols:
        return []
    return [{c: _serialize(v) for c, v in zip(cols, r)} for r in rows]


def init_db():
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS oauth_tokens (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            token_json TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS sessions (
            id SERIAL PRIMARY KEY,
            streamer TEXT NOT NULL, date TEXT NOT NULL, title TEXT DEFAULT '',
            link TEXT DEFAULT '', duration TEXT DEFAULT '', views INTEGER DEFAULT 0,
            unique_viewers INTEGER DEFAULT 0, avg_duration TEXT DEFAULT '',
            peak_concurrent INTEGER DEFAULT 0, likes INTEGER DEFAULT 0,
            avg_viewers INTEGER DEFAULT 0, new_subs INTEGER DEFAULT 0,
            discord INTEGER DEFAULT 0, casino TEXT DEFAULT '', provider TEXT DEFAULT '',
            video_id TEXT DEFAULT '', note TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS programs (
            year INTEGER NOT NULL, month INTEGER NOT NULL, day INTEGER NOT NULL,
            streamer TEXT NOT NULL DEFAULT '', casino TEXT DEFAULT '', provider TEXT DEFAULT '',
            PRIMARY KEY (year, month, day, streamer))""")
        _pg_run(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_video ON sessions(video_id) WHERE video_id != ''")
        conn.close()
    else:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS oauth_tokens (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                token_json TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                streamer TEXT NOT NULL, date TEXT NOT NULL, title TEXT DEFAULT '',
                link TEXT DEFAULT '', duration TEXT DEFAULT '', views INTEGER DEFAULT 0,
                unique_viewers INTEGER DEFAULT 0, avg_duration TEXT DEFAULT '',
                peak_concurrent INTEGER DEFAULT 0, likes INTEGER DEFAULT 0,
                avg_viewers INTEGER DEFAULT 0, new_subs INTEGER DEFAULT 0,
                discord INTEGER DEFAULT 0, casino TEXT DEFAULT '', provider TEXT DEFAULT '',
                video_id TEXT DEFAULT '', note TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS programs (
                year INTEGER NOT NULL, month INTEGER NOT NULL, day INTEGER NOT NULL,
                streamer TEXT NOT NULL DEFAULT '', casino TEXT DEFAULT '', provider TEXT DEFAULT '',
                PRIMARY KEY (year, month, day, streamer));
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_video
                ON sessions(video_id) WHERE video_id != '';
        """)
        conn.commit()
        conn.close()


# ── Token storage ──

def save_token(token_json: str):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM oauth_tokens WHERE id = 1")
        _pg_run(conn, "INSERT INTO oauth_tokens (id, token_json) VALUES (1, :tj)", {"tj": token_json})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("INSERT OR REPLACE INTO oauth_tokens (id, token_json, updated_at) VALUES (1, ?, CURRENT_TIMESTAMP)", (token_json,))
        conn.commit()
        conn.close()


def get_token() -> str | None:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT token_json FROM oauth_tokens WHERE id = 1")
        conn.close()
        return rows[0][0] if rows else None
    else:
        conn = get_conn()
        row = conn.execute("SELECT token_json FROM oauth_tokens WHERE id = 1").fetchone()
        conn.close()
        return row["token_json"] if row else None


# ── Sessions ──

def add_session(data: dict) -> int:
    cols = ["streamer","date","title","link","duration","views","unique_viewers",
            "avg_duration","peak_concurrent","likes","avg_viewers","new_subs",
            "discord","casino","provider","video_id","note"]
    if DATABASE_URL:
        conn = get_conn()
        placeholders = ", ".join(f":{c}" for c in cols)
        params = {c: data[c] for c in cols}
        rows = _pg_run(conn, f"INSERT INTO sessions ({','.join(cols)}) VALUES ({placeholders}) RETURNING id", params)
        sid = rows[0][0]
        conn.close()
        return sid
    else:
        conn = get_conn()
        vals = [data[c] for c in cols]
        cur = conn.execute(f"INSERT INTO sessions ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})", vals)
        conn.commit()
        sid = cur.lastrowid
        conn.close()
        return sid


def get_sessions(streamer: str = None) -> list[dict]:
    cols = ["id","streamer","date","title","link","duration","views","unique_viewers",
            "avg_duration","peak_concurrent","likes","avg_viewers","new_subs",
            "discord","casino","provider","video_id","note","created_at"]
    if DATABASE_URL:
        conn = get_conn()
        if streamer:
            rows = _pg_run(conn, "SELECT * FROM sessions WHERE streamer = :s ORDER BY date DESC", {"s": streamer})
        else:
            rows = _pg_run(conn, "SELECT * FROM sessions ORDER BY date DESC")
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        if streamer:
            rows = conn.execute("SELECT * FROM sessions WHERE streamer = ? ORDER BY date DESC", (streamer,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM sessions ORDER BY date DESC").fetchall()
        conn.close()
        return [dict(r) for r in rows]


def delete_session(sid: int):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM sessions WHERE id = :id", {"id": sid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM sessions WHERE id = ?", (sid,))
        conn.commit()
        conn.close()


def session_exists_by_video_id(video_id: str) -> bool:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT 1 FROM sessions WHERE video_id = :v", {"v": video_id})
        conn.close()
        return len(rows) > 0
    else:
        conn = get_conn()
        row = conn.execute("SELECT 1 FROM sessions WHERE video_id = ?", (video_id,)).fetchone()
        conn.close()
        return row is not None


# ── Programs ──

def save_program_day(year: int, month: int, day: int, streamer: str, casino: str, provider: str):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM programs WHERE year=:y AND month=:m AND day=:d AND streamer=:s",
                {"y": year, "m": month, "d": day, "s": streamer})
        _pg_run(conn, "INSERT INTO programs (year,month,day,streamer,casino,provider) VALUES (:y,:m,:d,:s,:c,:p)",
                {"y": year, "m": month, "d": day, "s": streamer, "c": casino, "p": provider})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("INSERT OR REPLACE INTO programs (year,month,day,streamer,casino,provider) VALUES (?,?,?,?,?,?)",
                     (year, month, day, streamer, casino, provider))
        conn.commit()
        conn.close()


def get_program(year: int, month: int, streamer: str = None) -> dict:
    if DATABASE_URL:
        conn = get_conn()
        if streamer:
            rows = _pg_run(conn, "SELECT * FROM programs WHERE year=:y AND month=:m AND streamer=:s",
                          {"y": year, "m": month, "s": streamer})
        else:
            rows = _pg_run(conn, "SELECT * FROM programs WHERE year=:y AND month=:m", {"y": year, "m": month})
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return {r["day"]: r for r in result}
    else:
        conn = get_conn()
        if streamer:
            rows = conn.execute("SELECT * FROM programs WHERE year=? AND month=? AND streamer=?", (year, month, streamer)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM programs WHERE year=? AND month=?", (year, month)).fetchall()
        conn.close()
        return {dict(r)["day"]: dict(r) for r in rows}
