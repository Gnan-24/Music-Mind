import requests
import json
import sys

# CONFIG

# Paste a FRESH access token here
ACCESS_TOKEN = "BQBJ-Bs_rQmFdUFAxiiAtLNVIFCskI6V-H984tMM4sPblITMy6B33zgulwQPY74Yk0n3f4eelxrtmulw6Q1_h3s1TDQdul9weyWWyzMIrIKTNcA8lMwTyMCybmCb4MjFGPZWS4tk2O6L7FATIO3meZEyUGZNGOo5Db1MoT8w8fOBfrYzU0xbetNVwBDwX5q8FZ_8bmSUXhlXPv_XlFEkHh4QoKnJwDEg8IM1Pr693iKDZjTd6oXrVzxSwNlt23iUSYE-HEM"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

# HELPER FUNCTION

def fetch_json(url, params=None, label="Request"):
    response = requests.get(url, headers=HEADERS, params=params)

    if response.status_code != 200:
        print(f"{label} failed with status {response.status_code}")
        print(response.text)
        sys.exit(1)

    try:
        return response.json()
    except ValueError:
        print(f"{label} returned non-JSON response")
        print(response.text)
        sys.exit(1)


# 1. FETCH TOP TRACKS

top_tracks_url = "https://api.spotify.com/v1/me/top/tracks?limit=50"
top_tracks_data = fetch_json(top_tracks_url, label="Top tracks request")

if "items" not in top_tracks_data:
    print("Unexpected top tracks response:")
    print(top_tracks_data)
    sys.exit(1)

with open("raw_data/top_tracks.json", "w") as f:
    json.dump(top_tracks_data, f, indent=2)


# 2. FETCH TOP ARTISTS


top_artists_url = "https://api.spotify.com/v1/me/top/artists?limit=50"
top_artists_data = fetch_json(top_artists_url, label="Top artists request")

if "items" not in top_artists_data:
    print("Unexpected top artists response:")
    print(top_artists_data)
    sys.exit(1)

with open("raw_data/top_artists.json", "w") as f:
    json.dump(top_artists_data, f, indent=2)


# 3. FETCH AUDIO FEATURES


track_ids = [
    track["id"]
    for track in top_tracks_data["items"]
    if track.get("id") is not None
]

# Spotify allows max 100 IDs (we are safe, but explicit is better)
track_ids = track_ids[:100]

audio_features_url = "https://api.spotify.com/v1/audio-features"
params = {"ids": ",".join(track_ids)}

audio_features_data = fetch_json(
    audio_features_url,
    params=params,
    label="Audio features request"
)

if "audio_features" not in audio_features_data:
    print("Unexpected audio features response:")
    print(audio_features_data)
    sys.exit(1)

with open("raw_data/audio_features.json", "w") as f:
    json.dump(audio_features_data, f, indent=2)


# DONE

print("✅ Spotify data fetched and saved successfully")
