import requests

# Spotify app credentials (temporary, do not commit)
CLIENT_ID = "70045b28e7d64b3ebc9d8659f3e305c0"
CLIENT_SECRET = "75237307b5924e48b0aaf0a83871cc44"
REFRESH_TOKEN = "AQDpPoOdJeEY3_dUblaZkwoG5oTE1TlBLDz3gUKUxM70G0Q9gpW3LjKQIkMTJFw5j0f6cjrFcqwpNqxWQN-knE4JGBmJ3iu8d3dxMDi8ozBJbUUJ_8xgEJpxGqVQ2EZXJbY"

# Spotify token endpoint (used to generate access tokens)
TOKEN_URL = "https://accounts.spotify.com/api/token"

# Request payload to exchange refresh token for access token
data = {
    "grant_type": "refresh_token",
    "refresh_token": REFRESH_TOKEN,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

# Send request to Spotify
response = requests.post(TOKEN_URL, data=data)

# Handle failure
if response.status_code != 200:
    print("Error getting access token")
    print(response.text)
    exit()

# Extract and print access token
access_token = response.json()["access_token"]
print(access_token)
