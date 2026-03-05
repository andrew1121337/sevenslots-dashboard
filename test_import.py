#!/usr/bin/env python3
"""Test YouTube import - uses token from DB (already authenticated)."""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import db
import youtube_api as yt

db.init_db()

# Step 1: Check auth
print("=== Step 1: Check Auth ===")
creds = yt.get_credentials()
if not creds:
    print("Not authenticated. Run: python3 test_import.py --login")
    if "--login" in sys.argv:
        from google_auth_oauthlib.flow import InstalledAppFlow
        flow = InstalledAppFlow.from_client_secrets_file(
            os.path.expanduser("~/Downloads/client_secret_936904037043-a20308c78j5nnhuv08q94tq16ecr00fl.apps.googleusercontent.com.json"),
            yt.SCOPES,
        )
        creds = flow.run_local_server(port=9090)
        db.save_token(creds.to_json())
        print("Token saved!")
    else:
        sys.exit(1)
else:
    print("Authenticated! Token found in DB.")

# Step 2: Channel info
print("\n=== Step 2: Channel Info ===")
from googleapiclient.discovery import build
youtube = build("youtube", "v3", credentials=creds)
ch = youtube.channels().list(part="id,snippet,contentDetails", mine=True).execute()
channel = ch["items"][0]
uploads_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]
print(f"Channel: {channel['snippet']['title']}")
print(f"Channel ID: {channel['id']}")
print(f"Uploads playlist: {uploads_id}")

# Step 3: Get uploads and find live streams
print("\n=== Step 3: Scan Uploads for Live Streams ===")
all_ids = []
request = youtube.playlistItems().list(part="contentDetails", playlistId=uploads_id, maxResults=50)
resp = request.execute()
for item in resp.get("items", []):
    all_ids.append(item["contentDetails"]["videoId"])
print(f"Found {resp.get('pageInfo',{}).get('totalResults',0)} total uploads, checking first {len(all_ids)}...")

# Get details for first batch
details_resp = youtube.videos().list(
    part="snippet,statistics,contentDetails,liveStreamingDetails",
    id=",".join(all_ids[:50]),
).execute()

live_count = 0
for item in details_resp.get("items", []):
    live = item.get("liveStreamingDetails")
    if not live:
        continue
    live_count += 1
    stats = item.get("statistics", {})
    dur = yt.parse_iso_duration(item.get("contentDetails", {}).get("duration", ""))
    print(f"\n  [{live_count}] {item['snippet']['title'][:60]}")
    print(f"      Date: {item['snippet']['publishedAt'][:10]}")
    print(f"      Duration: {dur}")
    print(f"      Views: {stats.get('viewCount', 'N/A')}")
    print(f"      Likes: {stats.get('likeCount', 'N/A')}")
    print(f"      Peak: {live.get('concurrentViewers', 'N/A')}")

print(f"\n  Total live streams found: {live_count} / {len(all_ids)} videos")

# Step 4: Test analytics on first live
print("\n=== Step 4: Analytics Test ===")
yt_analytics = build("youtubeAnalytics", "v2", credentials=creds)
from datetime import datetime
# Find first live video
test_vid = None
for item in details_resp.get("items", []):
    if item.get("liveStreamingDetails"):
        test_vid = item["id"]
        break
if test_vid:
    try:
        analytics = yt_analytics.reports().query(
            ids=f"channel=={channel['id']}",
            startDate="2000-01-01",
            endDate=datetime.now().strftime("%Y-%m-%d"),
            metrics="estimatedMinutesWatched,averageViewDuration,subscribersGained",
            filters=f"video=={test_vid}",
        ).execute()
        if analytics.get("rows"):
            row = analytics["rows"][0]
            print(f"  Video: {test_vid}")
            print(f"  Est. Minutes Watched: {row[0]}")
            print(f"  Avg View Duration (sec): {row[1]}")
            print(f"  Subscribers Gained: {row[2]}")
        else:
            print(f"  No analytics data for {test_vid}")
    except Exception as e:
        print(f"  Analytics error: {e}")

# Step 5: Full import test
print("\n=== Step 5: Full Import via fetch_live_streams() ===")
imported = yt.fetch_live_streams(creds)
print(f"Returned {len(imported)} new live streams")
for v in imported[:5]:
    print(f"  {v['date']} | {v['title'][:50]} | Views:{v.get('views',0)} | Peak:{v.get('peak_concurrent',0)}")

print("\n=== DONE ===")
