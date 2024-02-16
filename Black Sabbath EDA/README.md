# 1. Python API Request
First, the API call needs to be made so that we get the right information. The program sends a get request for artist name, track name, track popularity index, track id, and audio features.

```Python
import requests
import pandas as pd
import base64
from sqlalchemy import create_engine

def get_spotify_access_token():

    client_id = "YourClientIDHere"
    client_secret = "YourClientSecretHere"
    auth_url = "https://accounts.spotify.com/api/token"
    credentials_b64 = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    headers = {"Authorization": f"Basic {credentials_b64}"}
    payload = {"grant_type": "client_credentials"}

    response = requests.post(auth_url, headers=headers, data=payload)
    response.raise_for_status()
    access_token = response.json()['access_token']
    return access_token


def get_artist_id(artist_name, access_token):

    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": artist_name, "type": "artist", "limit": 1}
    response = requests.get(search_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return data['artists']['items'][0]['id'] if data['artists']['items'] else None

def get_artist_popularity(access_token, artist_id):
    artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(artist_url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data['popularity']


def get_top_tracks_for_artists(artist_id, access_token, market):
    top_tracks_url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market={market}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(top_tracks_url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    top_tracks = [{
        "artist_name": track["artists"][0]["name"],
        "track_name": track['name'],
        "track_id": track['id'],
        "popularity": track['popularity'],
        "track_url": track['external_urls']['spotify'],
        "market": market  # Add market to each track for reference
    } for track in data['tracks']]
    return top_tracks

def get_tracks_audio_feats(access_token, track_id):
    audio_feats_url = f"https://api.spotify.com/v1/audio-features/{track_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(audio_feats_url, headers=headers)
    response.raise_for_status()
    data = response.json()

    return {
        "acousticness": data['acousticness'],
        "danceability": data['danceability'],
        "duration_ms": data['duration_ms'],
        "energy": data['energy'],
        "key": data['key'],
        "mode": data['mode'],
        "liveness": data['liveness'],
        "loudness": data['loudness'],
        "speechiness": data['speechiness'],
        "tempo": data['tempo'],
        "time_signature": data['time_signature'],
        "valence": data['valence']
    }
```

We unify the previous snippets through a main() function which sets *Black Sabbath* as the artist name and creates a list with the markets (countries) we want to explore.

```Python
def main():
    artist_name = 'Black Sabbath'
    markets = ["CA", "DE", "FR", "GB", "JP", "MX", "US"]
    access_token = get_spotify_access_token()

    artist_id = get_artist_id(artist_name, access_token)
    if not artist_id:
        print(f"Could not find artist ID for {artist_name}")
        return

    all_tracks_features = []  # List to store features of tracks from all markets

    for market in markets:
        top_tracks = get_top_tracks_for_artists(artist_id, access_token, market)
        
        for track in top_tracks:
            track_features = {
                "artist_name": track["artist_name"],
                "track_name": track["track_name"],
                "track_id": track["track_id"],
                "market": market,
                "popularity": track["popularity"],
                "track_url": track["track_url"]
            }
    
            track_audio_features = get_tracks_audio_feats(access_token, track['track_id'])
            combined_track_info = {**track_features, **track_audio_features}
            all_tracks_features.append(combined_track_info)

    df = pd.DataFrame(all_tracks_features)
    
    return df
```

To get an overwview of how the created dataset looks:
```Python
df_spotify_tracks = main()
df_spotify_tracks.head(10)
```
![SQL_1](https://github.com/zefrios/SQL/assets/83305620/991c1711-6c82-426f-8a31-a09aeccf06b9)

Once the data is correctly extracted and placed into a dataframe, it is transferred to a SQL table for further analysis:

```Python
engine = create_engine('sqlite:///spotify_track_info.db')

df_spotify_tracks.to_sql(name='spotify_track_info', con=engine, if_exists='replace', index=False)
print("Data stored in the 'spotify_track_info' table.")
```
With this, the Python part of the project is concluded and we can proceed to querying the data from SQL to answer some exploratory questions.
***

# 2. SQL Queries

## 1. How many distinct tracks are there in the dataset?

```SQL
SELECT 
COUNT(DISTINCT track_id) AS unique_tracks
FROM spotify_track_info
```
![SQL_Q1](https://github.com/zefrios/SQL/assets/83305620/bf9863e8-eba4-45be-8616-f07c40f02693)


## 2. What are the average values of acousticness, danceability, duration (in minutes), energy, loudness, and tempo for all tracks?

```SQL
SELECT
ROUND(AVG(acousticness), 2) AS avg_acousticness,
ROUND(AVG(danceability), 2) AS avg_danceability,
ROUND(AVG((duration_ms)/60000), 2) AS avg_track_duration,
ROUND(AVG(energy), 2) AS avg_energy,
ROUND(AVG(loudness), 2) AS avg_loudness,
ROUND(AVG(tempo), 2) AS avg_tempo
FROM spotify_track_info
```

| avg_acousticness | avg_danceability | avg_track_duration | avg_energy | avg_loudness | avg_tempo |
| --- | --- | --- | --- | --- | --- |
| 0.23|0.39|4.6|0.54|-13.51|123.45

## 3. What is the average and median popularity of the tracks?
### Median
Since mySQL doesn't really have a function to calculate the median of a column directly, alternative routes which involve session variables and subqueries are necessary:
```SQL
SET @row_index = -1;

SELECT ROUND(AVG(subq.popularity), 2) as median_popularity
FROM (
    SELECT 
      @row_index := @row_index + 1 AS row_index, 
      popularity
    FROM spotify_track_info
    ORDER BY popularity
  ) AS subq
  WHERE subq.row_index 
  IN (FLOOR(@row_index / 2) , CEIL(@row_index / 2));
```
| median_popularity |
| --- |
| 65.00 |

### Average
```SQL
SELECT ROUND(AVG(popularity), 2) AS avg_popularity
FROM spotify_track_info
```
|avg_popularity|
| --- |
|66.14 

## 4. What is the average and median popularity of the tracks, per country?
### Median
Since the process to calculate the median can be simplified by using the *MariaDB* mySQL system, it should be noted that all the answers from this point on are obtained through that variation.
```SQL
SELECT DISTINCT
market,
MEDIAN(popularity) OVER(PARTITION BY market) AS median_popularity
FROM df_spotify_tracks
```
| market | median_popularity |
| -- | -- |
| CA | 60 |
| DE | 66 |
| FR | 66 |
| GB | 66 |
| JP | 66 |
| MX | 66 |
| US | 60 |

### Average
```SQL
SELECT
market,
ROUND(AVG(popularity), 2) AS avg_popularity
FROM df_spotify_tracks
GROUP BY market
```
| market | avg_popularity |
| -- | -- |
| CA | 62.60 |
| DE | 67.70 |
| FR | 67.70 |
| GB | 67.70 |
| JP | 67.70 |
| MX | 67.70 |
| US | 62.60 |

## 5. How many tracks have danceability and energy above the dataset's average?
```SQL
SELECT COUNT(*) AS above_than_average
FROM df_spotify_tracks
WHERE danceability > (SELECT AVG(danceability) FROM df_spotify_tracks)
AND energy > (SELECT AVG(energy) FROM df_spotify_tracks)
```
| above_than_average |
| -- |
| 14 |
