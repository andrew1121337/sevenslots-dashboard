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


def _safe_add_column(conn, table, column, col_type):
    """Add column to PG table only if it doesn't already exist."""
    rows = conn.run(
        "SELECT 1 FROM information_schema.columns WHERE table_name=:t AND column_name=:c",
        t=table, c=column)
    if not rows:
        conn.run(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


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
        _safe_add_column(conn, "oauth_tokens", "channel_name", "TEXT NOT NULL DEFAULT 'SevenSlots'")
        _safe_add_column(conn, "oauth_tokens", "channel_id", "TEXT DEFAULT ''")
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
        _safe_add_column(conn, "thumbnails", "streamer", "TEXT NOT NULL DEFAULT 'Seven'")
        _safe_add_column(conn, "thumbnails", "used", "INTEGER DEFAULT 0")
        # Add done column if missing (existing tables)
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS meetings (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS meeting_tasks (
            id SERIAL PRIMARY KEY,
            meeting_id INTEGER NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            done INTEGER DEFAULT 0,
            sort_order INTEGER DEFAULT 0)""")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS licente (
            id SERIAL PRIMARY KEY,
            license_code TEXT NOT NULL,
            casino_name TEXT NOT NULL)""")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS paysafes (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            psf25 INTEGER DEFAULT 0,
            psf50 INTEGER DEFAULT 0,
            status TEXT DEFAULT '',
            platform TEXT DEFAULT 'Instagram')""")
        _pg_run(conn, "DROP TABLE IF EXISTS roata_entries")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS roata (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            date TEXT NOT NULL,
            rotiri TEXT DEFAULT '',
            user_app TEXT DEFAULT '',
            username_cazino TEXT DEFAULT '',
            status TEXT DEFAULT '')""")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS targets (
            id SERIAL PRIMARY KEY,
            streamer TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            views_target INTEGER DEFAULT 0,
            hours_target INTEGER DEFAULT 0,
            UNIQUE(streamer, year, month))""")
        _pg_run(conn, """CREATE TABLE IF NOT EXISTS activity_log (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            action TEXT NOT NULL,
            details TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        # Safe column additions — check existence first to avoid transaction abort
        _safe_add_column(conn, "programs", "done", "INTEGER DEFAULT 0")
        _safe_add_column(conn, "targets", "hours_target", "INTEGER DEFAULT 0")
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
                used INTEGER DEFAULT 0,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE INDEX IF NOT EXISTS idx_thumb_date ON thumbnails(date);
            CREATE TABLE IF NOT EXISTS meetings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS meeting_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meeting_id INTEGER NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
                text TEXT NOT NULL,
                done INTEGER DEFAULT 0,
                sort_order INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS licente (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_code TEXT NOT NULL,
                casino_name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS paysafes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                psf25 INTEGER DEFAULT 0,
                psf50 INTEGER DEFAULT 0,
                status TEXT DEFAULT '',
                platform TEXT DEFAULT 'Instagram');
            CREATE TABLE IF NOT EXISTS roata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                date TEXT NOT NULL,
                rotiri TEXT DEFAULT '',
                user_app TEXT DEFAULT '',
                username_cazino TEXT DEFAULT '',
                status TEXT DEFAULT '');
            CREATE TABLE IF NOT EXISTS targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                streamer TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                views_target INTEGER DEFAULT 0,
                hours_target INTEGER DEFAULT 0,
                UNIQUE(streamer, year, month));
        """)
        conn.execute("""CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            action TEXT NOT NULL,
            details TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        try:
            conn.execute("ALTER TABLE programs ADD COLUMN done INTEGER DEFAULT 0")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE targets ADD COLUMN hours_target INTEGER DEFAULT 0")
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


def update_session(sid: int, data: dict):
    cols = ["date","title","link","duration","views","unique_viewers",
            "avg_duration","peak_concurrent","likes","avg_viewers","new_subs",
            "discord","casino","provider"]
    if DATABASE_URL:
        conn = get_conn()
        sets = ", ".join(f"{c} = :{c}" for c in cols)
        params = {c: data.get(c, "") for c in cols}
        params["id"] = sid
        _pg_run(conn, f"UPDATE sessions SET {sets} WHERE id = :id", params)
        conn.close()
    else:
        conn = get_conn()
        sets = ", ".join(f"{c} = ?" for c in cols)
        vals = [data.get(c, "") for c in cols] + [sid]
        conn.execute(f"UPDATE sessions SET {sets} WHERE id = ?", vals)
        conn.commit()
        conn.close()


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
            rows = _pg_run(conn, "SELECT id, streamer, date, filename, content_type, COALESCE(used,0) as used FROM thumbnails WHERE date >= :s AND date < :e AND streamer = :st ORDER BY date",
                           {"s": start, "e": end, "st": streamer})
        else:
            rows = _pg_run(conn, "SELECT id, streamer, date, filename, content_type, COALESCE(used,0) as used FROM thumbnails WHERE date >= :s AND date < :e ORDER BY date",
                           {"s": start, "e": end})
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        if streamer:
            rows = conn.execute("SELECT id, streamer, date, filename, content_type, COALESCE(used,0) as used FROM thumbnails WHERE date >= ? AND date < ? AND streamer = ? ORDER BY date",
                                (start, end, streamer)).fetchall()
        else:
            rows = conn.execute("SELECT id, streamer, date, filename, content_type, COALESCE(used,0) as used FROM thumbnails WHERE date >= ? AND date < ? ORDER BY date",
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


def update_thumbnail(tid: int, data: dict):
    sets, params = [], {}
    for col in ("date", "used"):
        if col in data:
            sets.append(f"{col} = :{col}")
            params[col] = data[col]
    if not sets:
        return
    params["id"] = tid
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, f"UPDATE thumbnails SET {', '.join(sets)} WHERE id = :id", params)
        conn.close()
    else:
        conn = get_conn()
        sql = f"UPDATE thumbnails SET {', '.join(s.replace(':','?') for s in sets)} WHERE id = ?"
        vals = [data.get(col) for col in ("date", "used") if col in data] + [tid]
        # SQLite uses ? placeholders
        set_parts = [f"{col} = ?" for col in ("date", "used") if col in data]
        vals = [data[col] for col in ("date", "used") if col in data] + [tid]
        conn.execute(f"UPDATE thumbnails SET {', '.join(set_parts)} WHERE id = ?", vals)
        conn.commit()
        conn.close()


# ── Meetings ──

def create_meeting(date: str) -> int:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "INSERT INTO meetings (date) VALUES (:d) RETURNING id", {"d": date})
        mid = rows[0][0]
        conn.close()
        return mid
    else:
        conn = get_conn()
        cur = conn.execute("INSERT INTO meetings (date) VALUES (?)", (date,))
        conn.commit()
        mid = cur.lastrowid
        conn.close()
        return mid


def get_meetings() -> list[dict]:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT id, date, created_at FROM meetings ORDER BY date DESC")
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        rows = conn.execute("SELECT id, date, created_at FROM meetings ORDER BY date DESC").fetchall()
        conn.close()
        return [dict(r) for r in rows]


def delete_meeting(mid: int):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM meeting_tasks WHERE meeting_id = :id", {"id": mid})
        _pg_run(conn, "DELETE FROM meetings WHERE id = :id", {"id": mid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM meeting_tasks WHERE meeting_id = ?", (mid,))
        conn.execute("DELETE FROM meetings WHERE id = ?", (mid,))
        conn.commit()
        conn.close()


def add_meeting_task(meeting_id: int, text: str) -> int:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "INSERT INTO meeting_tasks (meeting_id, text) VALUES (:m, :t) RETURNING id",
                       {"m": meeting_id, "t": text})
        tid = rows[0][0]
        conn.close()
        return tid
    else:
        conn = get_conn()
        cur = conn.execute("INSERT INTO meeting_tasks (meeting_id, text) VALUES (?, ?)", (meeting_id, text))
        conn.commit()
        tid = cur.lastrowid
        conn.close()
        return tid


def get_meeting_tasks(meeting_id: int) -> list[dict]:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT id, meeting_id, text, done, sort_order FROM meeting_tasks WHERE meeting_id = :m ORDER BY sort_order, id",
                       {"m": meeting_id})
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        rows = conn.execute("SELECT id, meeting_id, text, done, sort_order FROM meeting_tasks WHERE meeting_id = ? ORDER BY sort_order, id",
                            (meeting_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]


def toggle_meeting_task(task_id: int, done: int):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "UPDATE meeting_tasks SET done = :d WHERE id = :id", {"d": done, "id": task_id})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("UPDATE meeting_tasks SET done = ? WHERE id = ?", (done, task_id))
        conn.commit()
        conn.close()


def delete_meeting_task(task_id: int):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM meeting_tasks WHERE id = :id", {"id": task_id})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM meeting_tasks WHERE id = ?", (task_id,))
        conn.commit()
        conn.close()


# ── Licente ──

def get_licente() -> list[dict]:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT id, license_code, casino_name FROM licente ORDER BY casino_name")
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        rows = conn.execute("SELECT id, license_code, casino_name FROM licente ORDER BY casino_name").fetchall()
        conn.close()
        return [dict(r) for r in rows]


def add_licenta(license_code: str, casino_name: str) -> int:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "INSERT INTO licente (license_code, casino_name) VALUES (:c, :n) RETURNING id",
                       {"c": license_code, "n": casino_name})
        lid = rows[0][0]
        conn.close()
        return lid
    else:
        conn = get_conn()
        cur = conn.execute("INSERT INTO licente (license_code, casino_name) VALUES (?, ?)", (license_code, casino_name))
        conn.commit()
        lid = cur.lastrowid
        conn.close()
        return lid


def delete_licenta(lid: int):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM licente WHERE id = :id", {"id": lid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM licente WHERE id = ?", (lid,))
        conn.commit()
        conn.close()


def licente_count() -> int:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT count(*) FROM licente")
        conn.close()
        return rows[0][0]
    else:
        conn = get_conn()
        row = conn.execute("SELECT count(*) FROM licente").fetchone()
        conn.close()
        return row[0]


# ── PaySafes ──

def get_paysafes() -> list[dict]:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT id, username, psf25, psf50, status, platform FROM paysafes ORDER BY id")
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        rows = conn.execute("SELECT id, username, psf25, psf50, status, platform FROM paysafes ORDER BY id").fetchall()
        conn.close()
        return [dict(r) for r in rows]


def add_paysafe(username: str, psf25: int, psf50: int, status: str, platform: str) -> int:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "INSERT INTO paysafes (username, psf25, psf50, status, platform) VALUES (:u, :a, :b, :s, :p) RETURNING id",
                       {"u": username, "a": psf25, "b": psf50, "s": status, "p": platform})
        pid = rows[0][0]
        conn.close()
        return pid
    else:
        conn = get_conn()
        cur = conn.execute("INSERT INTO paysafes (username, psf25, psf50, status, platform) VALUES (?, ?, ?, ?, ?)",
                           (username, psf25, psf50, status, platform))
        conn.commit()
        pid = cur.lastrowid
        conn.close()
        return pid


def update_paysafe(pid: int, psf25: int, psf50: int, status: str):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "UPDATE paysafes SET psf25=:a, psf50=:b, status=:s WHERE id=:id",
                {"a": psf25, "b": psf50, "s": status, "id": pid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("UPDATE paysafes SET psf25=?, psf50=?, status=? WHERE id=?", (psf25, psf50, status, pid))
        conn.commit()
        conn.close()


def delete_paysafe(pid: int):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM paysafes WHERE id = :id", {"id": pid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM paysafes WHERE id = ?", (pid,))
        conn.commit()
        conn.close()


# ── Roata APP ──

def get_roata(category: str) -> list[dict]:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "SELECT id, category, date, rotiri, user_app, username_cazino, status FROM roata WHERE category = :c ORDER BY id DESC",
                       {"c": category})
        result = _pg_to_dicts(conn, rows)
        conn.close()
        return result
    else:
        conn = get_conn()
        rows = conn.execute("SELECT id, category, date, rotiri, user_app, username_cazino, status FROM roata WHERE category = ? ORDER BY id DESC",
                            (category,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]


def add_roata(category: str, date: str, rotiri: str, user_app: str, username_cazino: str, status: str) -> int:
    if DATABASE_URL:
        conn = get_conn()
        rows = _pg_run(conn, "INSERT INTO roata (category, date, rotiri, user_app, username_cazino, status) VALUES (:cat, :d, :r, :ua, :uc, :s) RETURNING id",
                       {"cat": category, "d": date, "r": rotiri, "ua": user_app, "uc": username_cazino, "s": status})
        rid = rows[0][0]
        conn.close()
        return rid
    else:
        conn = get_conn()
        cur = conn.execute("INSERT INTO roata (category, date, rotiri, user_app, username_cazino, status) VALUES (?, ?, ?, ?, ?, ?)",
                           (category, date, rotiri, user_app, username_cazino, status))
        conn.commit()
        rid = cur.lastrowid
        conn.close()
        return rid


def update_roata_status(rid: int, status: str):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "UPDATE roata SET status=:s WHERE id=:id", {"s": status, "id": rid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("UPDATE roata SET status=? WHERE id=?", (status, rid))
        conn.commit()
        conn.close()


def update_roata_entry(rid: int, rotiri: str, user_app: str, username_cazino: str, status: str):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "UPDATE roata SET rotiri=:r, user_app=:ua, username_cazino=:uc, status=:s WHERE id=:id",
                {"r": rotiri, "ua": user_app, "uc": username_cazino, "s": status, "id": rid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("UPDATE roata SET rotiri=?, user_app=?, username_cazino=?, status=? WHERE id=?",
                     (rotiri, user_app, username_cazino, status, rid))
        conn.commit()
        conn.close()


def delete_roata(rid: int):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "DELETE FROM roata WHERE id = :id", {"id": rid})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("DELETE FROM roata WHERE id = ?", (rid,))
        conn.commit()
        conn.close()


# ── Targets ──

def get_target(streamer: str, year: int, month: int) -> int:
    if DATABASE_URL:
        conn = get_conn()
        raw = _pg_run(conn, "SELECT views_target FROM targets WHERE streamer=:s AND year=:y AND month=:m",
                      {"s": streamer, "y": year, "m": month})
        rows = _pg_to_dicts(conn, raw)
        conn.close()
        return rows[0]["views_target"] if rows else 0
    else:
        conn = get_conn()
        row = conn.execute("SELECT views_target FROM targets WHERE streamer=? AND year=? AND month=?",
                           (streamer, year, month)).fetchone()
        conn.close()
        return row[0] if row else 0


def set_target(streamer: str, year: int, month: int, views_target: int, hours_target: int = 0):
    if DATABASE_URL:
        conn = get_conn()
        # Ensure hours_target column exists
        _safe_add_column(conn, "targets", "hours_target", "INTEGER DEFAULT 0")
        _pg_run(conn, """INSERT INTO targets (streamer, year, month, views_target, hours_target)
                         VALUES (:s, :y, :m, :v, :h)
                         ON CONFLICT (streamer, year, month) DO UPDATE SET views_target=EXCLUDED.views_target, hours_target=EXCLUDED.hours_target""",
                {"s": streamer, "y": year, "m": month, "v": views_target, "h": hours_target})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("""INSERT INTO targets (streamer, year, month, views_target, hours_target)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (streamer, year, month) DO UPDATE SET views_target=excluded.views_target, hours_target=excluded.hours_target""",
                     (streamer, year, month, views_target, hours_target))
        conn.commit()
        conn.close()


def _pg_has_column(conn, table, column):
    rows = conn.run("SELECT 1 FROM information_schema.columns WHERE table_name=:t AND column_name=:c", t=table, c=column)
    return len(rows) > 0


def get_all_targets(year: int, month: int) -> dict:
    if DATABASE_URL:
        conn = get_conn()
        has_hours = _pg_has_column(conn, "targets", "hours_target")
        if has_hours:
            raw = _pg_run(conn, "SELECT streamer, views_target, hours_target FROM targets WHERE year=:y AND month=:m",
                           {"y": year, "m": month})
        else:
            raw = _pg_run(conn, "SELECT streamer, views_target FROM targets WHERE year=:y AND month=:m",
                           {"y": year, "m": month})
        rows = _pg_to_dicts(conn, raw)
        conn.close()
        return {r["streamer"]: {"views": r.get("views_target", 0), "hours": r.get("hours_target", 0)} for r in rows}
    else:
        conn = get_conn()
        rows = [dict(r) for r in conn.execute("SELECT streamer, views_target, hours_target FROM targets WHERE year=? AND month=?",
                            (year, month)).fetchall()]
        conn.close()
        return {r["streamer"]: {"views": r.get("views_target", 0), "hours": r.get("hours_target", 0)} for r in rows}


# ── Activity Log ──

def log_activity(username: str, action: str, details: str = ""):
    if DATABASE_URL:
        conn = get_conn()
        _pg_run(conn, "INSERT INTO activity_log (username, action, details) VALUES (:u, :a, :d)",
                {"u": username, "a": action, "d": details})
        conn.close()
    else:
        conn = get_conn()
        conn.execute("INSERT INTO activity_log (username, action, details) VALUES (?, ?, ?)",
                     (username, action, details))
        conn.commit()
        conn.close()


def get_activity_log(limit: int = 100) -> list[dict]:
    if DATABASE_URL:
        conn = get_conn()
        raw = _pg_run(conn, "SELECT id, username, action, details, created_at FROM activity_log ORDER BY created_at DESC LIMIT :n",
                       {"n": limit})
        rows = _pg_to_dicts(conn, raw)
        conn.close()
        return rows
    else:
        conn = get_conn()
        rows = [dict(r) for r in conn.execute(
            "SELECT id, username, action, details, created_at FROM activity_log ORDER BY created_at DESC LIMIT ?",
            (limit,)).fetchall()]
        conn.close()
        return rows
