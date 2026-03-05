"""SQLite database for SevenSlots Dashboard."""
import sqlite3
import json
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "sevenslots.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
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
    conn.close()


# ── Token storage ──

def save_token(token_json: str):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO oauth_tokens (id, token_json, updated_at) VALUES (1, ?, CURRENT_TIMESTAMP)",
        (token_json,),
    )
    conn.commit()
    conn.close()


def get_token() -> str | None:
    conn = get_conn()
    row = conn.execute("SELECT token_json FROM oauth_tokens WHERE id = 1").fetchone()
    conn.close()
    return row["token_json"] if row else None


# ── Sessions ──

def add_session(data: dict) -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO sessions
        (streamer, date, title, link, duration, views, unique_viewers,
         avg_duration, peak_concurrent, likes, avg_viewers, new_subs,
         discord, casino, provider, video_id, note)
        VALUES (:streamer, :date, :title, :link, :duration, :views, :unique_viewers,
                :avg_duration, :peak_concurrent, :likes, :avg_viewers, :new_subs,
                :discord, :casino, :provider, :video_id, :note)""",
        data,
    )
    conn.commit()
    sid = cur.lastrowid
    conn.close()
    return sid


def get_sessions(streamer: str = None) -> list[dict]:
    conn = get_conn()
    if streamer:
        rows = conn.execute(
            "SELECT * FROM sessions WHERE streamer = ? ORDER BY date DESC", (streamer,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM sessions ORDER BY date DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_session(sid: int):
    conn = get_conn()
    conn.execute("DELETE FROM sessions WHERE id = ?", (sid,))
    conn.commit()
    conn.close()


def session_exists_by_video_id(video_id: str) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM sessions WHERE video_id = ?", (video_id,)
    ).fetchone()
    conn.close()
    return row is not None


# ── Programs ──

def save_program_day(year: int, month: int, day: int, streamer: str, casino: str, provider: str):
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO programs (year, month, day, streamer, casino, provider)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (year, month, day, streamer, casino, provider),
    )
    conn.commit()
    conn.close()


def get_program(year: int, month: int, streamer: str = None) -> dict:
    conn = get_conn()
    if streamer:
        rows = conn.execute(
            "SELECT * FROM programs WHERE year = ? AND month = ? AND streamer = ?",
            (year, month, streamer),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM programs WHERE year = ? AND month = ?", (year, month)
        ).fetchall()
    conn.close()
    return {r["day"]: dict(r) for r in rows}
