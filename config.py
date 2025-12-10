from dotenv import load_dotenv
import os

load_dotenv()

print("CLIENT ID:", os.getenv("SPOTIFY_CLIENT_ID"))
