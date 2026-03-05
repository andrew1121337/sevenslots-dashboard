"""YouTube API integration with persistent token storage."""
import json
import os
import re
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from googleapiclient.discovery import build

import db

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]
CLIENT_SECRET = os.path.join(os.path.dirname(__file__), "client_secret.json")


def _ensure_client_secret():
    """Write client_secret.json from env var if it doesn't exist on disk."""
    if not os.path.exists(CLIENT_SECRET):
        data = os.environ.get("CLIENT_SECRET_JSON")
        if data:
            with open(CLIENT_SECRET, "w") as f:
                f.write(data)


def _refresh_and_save(creds: Credentials) -> Credentials:
    """Refresh token if needed and persist to DB."""
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        db.save_token(creds.to_json())
    return creds


def get_credentials() -> Credentials | None:
    """Load credentials from DB, refresh if needed."""
    token_json = db.get_token()
    if not token_json:
        return None
    creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds = _refresh_and_save(creds)
        else:
            return None
    return creds


def _redirect_uri() -> str:
    base = os.environ.get("BASE_URL", "http://localhost:8080")
    return f"{base}/oauth/callback"


def start_oauth_flow() -> str:
    """Start OAuth flow and return auth URL for user to visit."""
    _ensure_client_secret()
    from google_auth_oauthlib.flow import Flow
    flow = Flow.from_client_secrets_file(CLIENT_SECRET, SCOPES)
    flow.redirect_uri = _redirect_uri()
    auth_url, _ = flow.authorization_url(
        access_type="offline", prompt="consent"
    )
    return auth_url


def complete_oauth_flow(code: str) -> Credentials:
    """Complete OAuth flow with authorization code, save token to DB."""
    _ensure_client_secret()
    from google_auth_oauthlib.flow import Flow
    flow = Flow.from_client_secrets_file(CLIENT_SECRET, SCOPES)
    flow.redirect_uri = _redirect_uri()
    flow.fetch_token(code=code)
    creds = flow.credentials
    db.save_token(creds.to_json())
    return creds


def is_authenticated() -> bool:
    return get_credentials() is not None


def parse_iso_duration(iso: str) -> str:
    """Convert ISO 8601 duration (PT3H45M12S) to H:MM:SS."""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m:
        return iso or ""
    h, mi, s = int(m.group(1) or 0), int(m.group(2) or 0), int(m.group(3) or 0)
    return f"{h}:{mi:02d}:{s:02d}"


def fetch_live_streams(creds: Credentials, year: int = None, month: int = None) -> list[dict]:
    """Fetch completed live streams for a specific month."""
    from datetime import datetime
    youtube = build("youtube", "v3", credentials=creds)
    yt_analytics = build("youtubeAnalytics", "v2", credentials=creds)

    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    # Date range for the target month
    month_start = f"{year}-{month:02d}-01"
    if month == 12:
        month_end = f"{year + 1}-01-01"
    else:
        month_end = f"{year}-{month + 1:02d}-01"

    # Get channel ID and uploads playlist
    ch = youtube.channels().list(part="id,contentDetails", mine=True).execute()
    channel_id = ch["items"][0]["id"]
    uploads_id = ch["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Walk uploads playlist, stop when we pass the target month
    all_video_ids = []
    request = youtube.playlistItems().list(
        part="contentDetails,snippet",
        playlistId=uploads_id,
        maxResults=50,
    )
    done = False
    while request and not done:
        resp = request.execute()
        for item in resp.get("items", []):
            published = item["snippet"]["publishedAt"][:10]
            # Stop if we've gone past the target month (uploads are newest-first)
            if published < month_start:
                done = True
                break
            # Skip if not in target month
            if published >= month_end:
                continue
            vid = item["contentDetails"]["videoId"]
            if not db.session_exists_by_video_id(vid):
                all_video_ids.append(vid)
        if not done:
            request = youtube.playlistItems().list_next(request, resp)

    if not all_video_ids:
        return []

    # Get video details in batches, filter for live streams only
    videos = []
    for i in range(0, len(all_video_ids), 50):
        batch = all_video_ids[i:i+50]
        resp = youtube.videos().list(
            part="snippet,statistics,contentDetails,liveStreamingDetails",
            id=",".join(batch),
        ).execute()
        for item in resp.get("items", []):
            live = item.get("liveStreamingDetails")
            if not live:
                continue
            vid = item["id"]
            stats = item.get("statistics", {})
            videos.append({
                "video_id": vid,
                "title": item["snippet"]["title"],
                "date": item["snippet"]["publishedAt"][:10],
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "duration": parse_iso_duration(item.get("contentDetails", {}).get("duration", "")),
                "peak_concurrent": int(live.get("concurrentViewers", 0)),
                "unique_viewers": 0,
                "avg_duration": "",
                "avg_viewers": 0,
                "new_subs": 0,
            })

    # Get analytics per video
    today = datetime.now().strftime("%Y-%m-%d")
    for v in videos:
        vid = v["video_id"]
        try:
            a = yt_analytics.reports().query(
                ids=f"channel=={channel_id}",
                startDate="2000-01-01",
                endDate=today,
                metrics="views,estimatedMinutesWatched,averageViewDuration,likes,subscribersGained",
                filters=f"video=={vid}",
            ).execute()
            if a.get("rows"):
                row = a["rows"][0]
                analytics_views = int(row[0])
                minutes_watched = float(row[1])
                avg_view_sec = int(row[2])
                analytics_likes = int(row[3])
                v["new_subs"] = int(row[4])
                v["avg_duration"] = f"{avg_view_sec // 60}:{avg_view_sec % 60:02d}"
                # Use analytics views/likes if higher (more accurate)
                if analytics_views > v["views"]:
                    v["views"] = analytics_views
                if analytics_likes > v["likes"]:
                    v["likes"] = analytics_likes
                # Calculate average concurrent viewers from minutes watched / duration
                dur = v.get("duration", "")
                if dur and minutes_watched > 0:
                    parts = dur.split(":")
                    if len(parts) == 3:
                        total_min = int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) / 60
                        if total_min > 0:
                            v["avg_viewers"] = int(minutes_watched / total_min)
                # Estimate unique viewers: views * (avgViewDuration / totalDuration)
                # This gives a rough approximation
                if dur and avg_view_sec > 0 and analytics_views > 0:
                    parts = dur.split(":")
                    if len(parts) == 3:
                        total_sec = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                        if total_sec > 0:
                            # unique ~= minutesWatched / avgViewDuration * 60
                            v["unique_viewers"] = int(minutes_watched * 60 / avg_view_sec) if avg_view_sec > 0 else 0
        except Exception:
            pass

    return videos
