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

## 4. What is the average and median popularity of the tracks, per country?
