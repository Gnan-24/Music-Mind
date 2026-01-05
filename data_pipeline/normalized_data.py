import json

# 1. Load raw Spotify data
def load_raw_data():
    with open("raw_data/top_tracks.json", "r") as f:
        top_tracks = json.load(f)

    with open("raw_data/top_artists.json", "r") as f:
        top_artists = json.load(f)

    return top_tracks, top_artists

# 2. Normalize songs from top_tracks.json
def normalize_songs(top_tracks):
    songs = []
    for track in top_tracks["items"]:
        artist_ids = []
        for artist in track["artists"]:
            artist_ids.append(artist["id"])
        song = {
               "song_id": track["id"],
               "name": track["name"],
               "artist_ids": artist_ids,
               "popularity": track["popularity"]
         }
        songs.append(song)

    return songs


# 3. Normalize artists from top_artists.json
def normalize_artists(top_artists):
    artists = []

    for raw_artist in top_artists["items"]:
        artist = {
            "artist_id": raw_artist["id"],
            "name": raw_artist["name"],
            "genres": raw_artist["genres"],
            "popularity": raw_artist["popularity"]
        }
        artists.append(artist)

    return artists

# 4. Normalize genres from artists data
def normalize_genres(artists):
    genre_map={}
    for artist in artists:
        for genre in artist["genres"]:
            if genre not in genre_map:
                genre_map[genre] = []
            genre_map[genre].append(artist["artist_id"])
    genres = []
    for genre, artist_ids in genre_map.items():
        genres.append({
            "genre": genre,
            "artist_ids": artist_ids
        })

    return genres

# 5. Build user listening history
def build_user_history(top_tracks, user_id="user_001"):
    history = []

    for track in top_tracks["items"]:
        history.append({
            "user_id": user_id,
            "song_id": track["id"]
        })

    return history

# 6. Save normalized output
def save_json(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

def main():
        top_tracks, top_artists = load_raw_data()
        songs = normalize_songs(top_tracks)
        artists = normalize_artists(top_artists)
        genres = normalize_genres(artists)
        user_history = build_user_history(top_tracks)
        save_json(songs, "normalized_data/songs.json")
        save_json(artists, "normalized_data/artists.json")
        save_json(genres, "normalized_data/genres.json")
        save_json(user_history, "normalized_data/user_history.json")

if __name__ == "__main__":
    main()
