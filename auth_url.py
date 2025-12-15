import os
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI")
SCOPE = "user-read-private user-read-email user-top-read"

params = {
    "client_id": CLIENT_ID,
    "response_type": "code",
    "redirect_uri": REDIRECT_URI,
    "scope": SCOPE
}

query_string = urllib.parse.urlencode(params)
auth_url = f"https://accounts.spotify.com/authorize?{query_string}"

print("\n👉 Open this URL in your browser:\n")
print(auth_url)