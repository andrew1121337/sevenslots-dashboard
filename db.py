"""Database for SevenSlots Dashboard. Uses PostgreSQL if DATABASE_URL is set, else SQLite."""
import os
import sqlite3
from datetime import datetime, date
from urllib.parse import urlparse

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    if DATABASE_URL:
        import pg8000.native
        p = urlparse(DATABASE_URL)
        kwargs = dict(
            user=p.username, password=p.password,
            host=p.hostname, port=p.port or 5432,
            database=p.path.lstrip("/"),
        )
        # Use SSL only for external connections
        if ".internal" not in (p.hostname or ""):
            import ssl
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            kwargs["ssl_context"] = ssl_ctx
        conn = pg8000.native.Connection(**kwargs)
        return conn
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), "sevenslots.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _pg_run(conn, sql, params=None):
    """Run SQL on pg8000 native connection."""
    if params:
        return conn.run(sql, **params)
    return conn.run(sql)


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
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS oauth_tokens (
            id SERIAL PRIMARY KEY,
            channel_name TEXT NOT NULL DEFAULT 'SevenSlots',
            channel_id TEXT DEFAULT '',
            token_json TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        # Migrate: add channel columns if missing
        try:
            _pg_run(conn, "ALTER TABLE oauth_tokens ADD COLUMN channel_name TEXT NOT NULL DEFAULT 'SevenSlots'")
        except Exception:
            pass
        try:
            _pg_run(conn, "ALTER TABLE oauth_tokens ADD COLUMN channel_id TEXT DEFAULT ''")
        except Exception:
            pass
        # Drop old id=1 constraint if exists
        try:
            _pg_run(conn, "ALTER TABLE oauth_tokens DROP CONSTRAINT IF EXISTS oauth_tokens_id_check")
        except Exception:
            pass
        # Create unique index on channel_name
        try:
            _pg_run(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_oauth_channel ON oauth_tokens(channel_name)")
        except Exception:
            pass
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
            done INTEGER DEFAULT 0,
            PRIMARY KEY (year, month, day, streamer))""")
        _pg_run(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_video ON sessions(video_id) WHERE video_id != ''")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS thumbnails (
            id SERIAL PRIMARY KEY,
            streamer TEXT NOT NULL DEFAULT 'Seven',
            date TEXT NOT NULL,
            filename TEXT NOT NULL,
            content_type TEXT DEFAULT 'image/png',
            image_data TEXT NOT NULL,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        try:
            _pg_run(conn, "CREATE INDEX IF NOT EXISTS idx_thumb_date ON thumbnails(date)")
        except Exception:
            pass
        try:
            _pg_run(conn, "ALTER TABLE thumbnails ADD COLUMN streamer TEXT NOT NULL DEFAULT 'Seven'")
        except Exception:
            pass
        # Add done column if missing (existing tables)
        try:
            _pg_run(conn, "ALTER TABLE programs ADD COLUMN done INTEGER DEFAULT 0")
        except Exception:
            pass
        conn.close()
    else:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS oauth_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name TEXT NOT NULL DEFAULT 'SevenSlots',
                channel_id TEXT DEFAULT '',
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
                done INTEGER DEFAULT 0,
                PRIMARY KEY (year, month, day, streamer));
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_video
                ON sessions(video_id) WHERE video_id != '';
            CREATE TABLE IF NOT EXISTS thumbnails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                streamer TEXT NOT NULL DEFAULT 'Seven',
                date TEXT NOT NULL,
                filename TEXT NOT NULL,
                content_type TEXT DEFAULT 'image/png',
                image_data TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE INDEX IF NOT EXISTS idx_thumb_date ON thumbnails(date);
        """)
        try:
            conn.execute("ALTER TABLE programs ADD COLUMN done INTEGER DEFAULT 0")
        except Exception:
            pass
        conn.commit()
        conn.close()


# ── Users ──

def create_user(username: str, password: str):
    import hashlib
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "INSERT INTO users (username, password_hash) VALUES (:u, :p)", {"u": username, "p": pw_hash})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, pw_hash))
        conn.commit()
        conn.close()


def delete_user(username: str):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM users WHERE username = :u", {"u": username})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
        conn.close()


def verify_user(username: str, password: str) -> bool:
    import hashlib
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT 1 FROM users WHERE username = :u AND password_hash = :p", {"u": username, "p": pw_hash})
        conn.close()
        return len(rows) > 0
    else:
        conn = get_conn()
        row = conn.execute("SELECT 1 FROM users WHERE username = ? AND password_hash = ?", (username, pw_hash)).fetchone()
        conn.close()
        return row is not None


def user_count() -> int:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT count(*) FROM users")
        conn.close()
        return rows[0][0]
    else:
        conn = get_conn()
        row = conn.execute("SELECT count(*) FROM users").fetchone()
        conn.close()
        return row[0]


# ── Token storage (multi-channel) ──

def save_token(token_json: str, channel_name: str = "SevenSlots", channel_id: str = ""):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM oauth_tokens WHERE channel_name = :cn", {"cn": channel_name})
        _pg_run(conn, "INSERT INTO oauth_tokens (channel_name, channel_id, token_json) VALUES (:cn, :ci, :tj)",
                {"cn": channel_name, "ci": channel_id, "tj": token_json})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM oauth_tokens WHERE channel_name = ?", (channel_name,))
        conn.execute("INSERT INTO oauth_tokens (channel_name, channel_id, token_json, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                     (channel_name, channel_id, token_json))
        conn.commit()
        conn.close()


def get_token(channel_name: str = None) -> str | None:
    if DATABASE_URL:
        conn = get_conn()
        if channel_name:
            rows = _pg_run(conn, "SELECT token_json FROM oauth_tokens WHERE channel_name = :cn", {"cn": channel_name})
        else:
            rows = _pg_run(conn, "SELECT token_json FROM oauth_tokens ORDER BY id LIMIT 1")
        conn.close()
        return rows[0][0] if rows else None
    else:
        conn = get_conn()
        if channel_name:
            row = conn.execute("SELECT token_json FROM oauth_tokens WHERE channel_name = ?", (channel_name,)).fetchone()
        else:
            row = conn.execute("SELECT token_json FROM oauth_tokens ORDER BY id LIMIT 1").fetchone()
        conn.close()
        return dict(row)["token_json"] if row else None


def get_all_channels() -> list[dict]:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT channel_name, channel_id FROM oauth_tokens ORDER BY id")
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        rows = conn.execute("SELECT channel_name, channel_id FROM oauth_tokens ORDER BY id").fetchall()
        conn.close()
        return [dict(r) for r in rows]


def delete_channel_token(channel_name: str):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM oauth_tokens WHERE channel_name = :cn", {"cn": channel_name})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM oauth_tokens WHERE channel_name = ?", (channel_name,))
        conn.commit()
        conn.close()


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

def save_program_day(year: int, month: int, day: int, streamer: str, casino: str, provider: str, done: int = 0):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM programs WHERE year=:y AND month=:m AND day=:d AND streamer=:s",
                {"y": year, "m": month, "d": day, "s": streamer})
        _pg_run(conn, "INSERT INTO programs (year,month,day,streamer,casino,provider,done) VALUES (:y,:m,:d,:s,:c,:p,:dn)",
                {"y": year, "m": month, "d": day, "s": streamer, "c": casino, "p": provider, "dn": done})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("INSERT OR REPLACE INTO programs (year,month,day,streamer,casino,provider,done) VALUES (?,?,?,?,?,?,?)",
                     (year, month, day, streamer, casino, provider, done))
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


def migrate_program_months():
    """One-time: wipe program data with wrong month indexing."""
    pass


# ── Thumbnails ──

def add_thumbnail(streamer: str, date: str, filename: str, content_type: str, image_data: str) -> int:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "INSERT INTO thumbnails (streamer, date, filename, content_type, image_data) VALUES (:s, :d, :f, :ct, :img) RETURNING id",
                       {"s": streamer, "d": date, "f": filename, "ct": content_type, "img": image_data})
        tid = rows[0][0]
        conn.close()
        return tid
    else:
        conn = get_conn()
        cur = conn.execute("INSERT INTO thumbnails (streamer, date, filename, content_type, image_data) VALUES (?, ?, ?, ?, ?)",
                           (streamer, date, filename, content_type, image_data))
        conn.commit()
        tid = cur.lastrowid
        conn.close()
        return tid


def get_thumbnails_for_month(year: int, month: int, streamer: str = None) -> list[dict]:
    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1}-01-01"
    else:
        end = f"{year}-{month + 1:02d}-01"
    if DATABASE_URL:
        conn = get_conn()
        if streamer:
            rows = _pg_run(conn, "SELECT id, streamer, date, filename, content_type FROM thumbnails WHERE date >= :s AND date < :e AND streamer = :st ORDER BY date",
                           {"s": start, "e": end, "st": streamer})
        else:
            rows = _pg_run(conn, "SELECT id, streamer, date, filename, content_type FROM thumbnails WHERE date >= :s AND date < :e ORDER BY date",
                           {"s": start, "e": end})
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        if streamer:
            rows = conn.execute("SELECT id, streamer, date, filename, content_type FROM thumbnails WHERE date >= ? AND date < ? AND streamer = ? ORDER BY date",
                                (start, end, streamer)).fetchall()
        else:
            rows = conn.execute("SELECT id, streamer, date, filename, content_type FROM thumbnails WHERE date >= ? AND date < ? ORDER BY date",
                                (start, end)).fetchall()
        conn.close()
        return [dict(r) for r in rows]


def get_thumbnail(tid: int) -> dict | None:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT id, date, filename, content_type, image_data FROM thumbnails WHERE id = :id", {"id": tid})
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result[0] if result else None
    else:
        conn = get_conn()
        row = conn.execute("SELECT id, date, filename, content_type, image_data FROM thumbnails WHERE id = ?", (tid,)).fetchone()
        conn.close()
        return dict(row) if row else None


def delete_thumbnail(tid: int):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM thumbnails WHERE id = :id", {"id": tid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM thumbnails WHERE id = ?", (tid,))
        conn.commit()
        conn.close()
