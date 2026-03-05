"""Database for SevenSlots Dashboard. Uses PostgreSQL if DATABASE_URL is set, else SQLite."""
import os
import sqlite3

DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras


def get_conn():
    if DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), "sevenslots.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _dict_row(cur):
    """Convert cursor row to dict (works for both sqlite and psycopg2)."""
    if DATABASE_URL:
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    return [dict(r) for r in cur.fetchall()]


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    if DATABASE_URL:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS oauth_tokens (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                token_json TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY,
                streamer TEXT NOT NULL,
                date TEXT NOT NULL,
                title TEXT DEFAULT '',
                link TEXT DEFAULT '',
                duration TEXT DEFAULT '',
                views INTEGER DEFAULT 0,
                unique_viewers INTEGER DEFAULT 0,
                avg_duration TEXT DEFAULT '',
                peak_concurrent INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                avg_viewers INTEGER DEFAULT 0,
                new_subs INTEGER DEFAULT 0,
                discord INTEGER DEFAULT 0,
                casino TEXT DEFAULT '',
                provider TEXT DEFAULT '',
                video_id TEXT DEFAULT '',
                note TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS programs (
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                streamer TEXT NOT NULL DEFAULT '',
                casino TEXT DEFAULT '',
                provider TEXT DEFAULT '',
                PRIMARY KEY (year, month, day, streamer)
            )
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_video
                ON sessions(video_id) WHERE video_id != ''
        """)
    else:
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS oauth_tokens (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                token_json TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                streamer TEXT NOT NULL,
                date TEXT NOT NULL,
                title TEXT DEFAULT '',
                link TEXT DEFAULT '',
                duration TEXT DEFAULT '',
                views INTEGER DEFAULT 0,
                unique_viewers INTEGER DEFAULT 0,
                avg_duration TEXT DEFAULT '',
                peak_concurrent INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                avg_viewers INTEGER DEFAULT 0,
                new_subs INTEGER DEFAULT 0,
                discord INTEGER DEFAULT 0,
                casino TEXT DEFAULT '',
                provider TEXT DEFAULT '',
                video_id TEXT DEFAULT '',
                note TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS programs (
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                streamer TEXT NOT NULL DEFAULT '',
                casino TEXT DEFAULT '',
                provider TEXT DEFAULT '',
                PRIMARY KEY (year, month, day, streamer)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_video
                ON sessions(video_id) WHERE video_id != '';
        """)
    conn.commit()
    cur.close()
    conn.close()


# ── Token storage ──

def save_token(token_json: str):
    conn = get_conn()
    cur = conn.cursor()
    if DATABASE_URL:
        cur.execute(
            "INSERT INTO oauth_tokens (id, token_json, updated_at) VALUES (1, %s, CURRENT_TIMESTAMP) "
            "ON CONFLICT (id) DO UPDATE SET token_json = %s, updated_at = CURRENT_TIMESTAMP",
            (token_json, token_json),
        )
    else:
        cur.execute(
            "INSERT OR REPLACE INTO oauth_tokens (id, token_json, updated_at) VALUES (1, ?, CURRENT_TIMESTAMP)",
            (token_json,),
        )
    conn.commit()
    cur.close()
    conn.close()


def get_token() -> str | None:
    conn = get_conn()
    cur = conn.cursor()
    if DATABASE_URL:
        cur.execute("SELECT token_json FROM oauth_tokens WHERE id = 1")
    else:
        cur.execute("SELECT token_json FROM oauth_tokens WHERE id = 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    return row[0] if DATABASE_URL else row["token_json"]


# ── Sessions ──

def add_session(data: dict) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cols = ["streamer", "date", "title", "link", "duration", "views", "unique_viewers",
            "avg_duration", "peak_concurrent", "likes", "avg_viewers", "new_subs",
            "discord", "casino", "provider", "video_id", "note"]
    vals = [data[c] for c in cols]
    if DATABASE_URL:
        placeholders = ", ".join(["%s"] * len(cols))
        cur.execute(
            f"INSERT INTO sessions ({', '.join(cols)}) VALUES ({placeholders}) RETURNING id",
            vals,
        )
        sid = cur.fetchone()[0]
    else:
        placeholders = ", ".join(["?"] * len(cols))
        cur.execute(
            f"INSERT INTO sessions ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        sid = cur.lastrowid
    conn.commit()
    cur.close()
    conn.close()
    return sid


def get_sessions(streamer: str = None) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor()
    ph = "%s" if DATABASE_URL else "?"
    if streamer:
        cur.execute(f"SELECT * FROM sessions WHERE streamer = {ph} ORDER BY date DESC", (streamer,))
    else:
        cur.execute("SELECT * FROM sessions ORDER BY date DESC")
    rows = _dict_row(cur)
    cur.close()
    conn.close()
    return rows


def delete_session(sid: int):
    conn = get_conn()
    cur = conn.cursor()
    ph = "%s" if DATABASE_URL else "?"
    cur.execute(f"DELETE FROM sessions WHERE id = {ph}", (sid,))
    conn.commit()
    cur.close()
    conn.close()


def session_exists_by_video_id(video_id: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    ph = "%s" if DATABASE_URL else "?"
    cur.execute(f"SELECT 1 FROM sessions WHERE video_id = {ph}", (video_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row is not None


# ── Programs ──

def save_program_day(year: int, month: int, day: int, streamer: str, casino: str, provider: str):
    conn = get_conn()
    cur = conn.cursor()
    if DATABASE_URL:
        cur.execute(
            "INSERT INTO programs (year, month, day, streamer, casino, provider) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (year, month, day, streamer) DO UPDATE SET casino = %s, provider = %s",
            (year, month, day, streamer, casino, provider, casino, provider),
        )
    else:
        cur.execute(
            "INSERT OR REPLACE INTO programs (year, month, day, streamer, casino, provider) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (year, month, day, streamer, casino, provider),
        )
    conn.commit()
    cur.close()
    conn.close()


def get_program(year: int, month: int, streamer: str = None) -> dict:
    conn = get_conn()
    cur = conn.cursor()
    ph = "%s" if DATABASE_URL else "?"
    if streamer:
        cur.execute(
            f"SELECT * FROM programs WHERE year = {ph} AND month = {ph} AND streamer = {ph}",
            (year, month, streamer),
        )
    else:
        cur.execute(
            f"SELECT * FROM programs WHERE year = {ph} AND month = {ph}",
            (year, month),
        )
    rows = _dict_row(cur)
    cur.close()
    conn.close()
    return {r["day"]: r for r in rows}
