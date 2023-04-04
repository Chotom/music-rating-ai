# RYM RAW DATA ----------------------------------------------------------------
ARTIST = 'artist'
ALBUM = 'album'
DATE = 'date'
RATING = 'rating'
RATING_NUMBER = 'rating_number'
GENRES = 'genres'

RYM_COLS = [ARTIST, ALBUM, DATE, RATING, RATING_NUMBER, GENRES]
"""Columns for rate your music data."""

# SPOTIFY RAW DATA ------------------------------------------------------------
ALBUM_ID = 'album_id'
SPOTIFY_ALBUM = 'spotify_album'
SPOTIFY_ARTIST = 'spotify_artist'
PREC_MATCH = 'precision_match'

SPOTIFY_SEARCH_COLS = [ALBUM, ARTIST, ALBUM_ID, SPOTIFY_ALBUM, SPOTIFY_ARTIST, PREC_MATCH]
"""Column names in output spotify search file."""

# SPOTIFY FEATURES ------------------------------------------------------------
DANCEABILITY = 'danceability'
ENERGY = 'energy'
KEY = 'key'
LOUDNESS = 'loudness'
MODE = 'mode'
SPEECHINESS = 'speechiness'
ACOUSTICNESS = 'acousticness'
INSTRUMENTALNESS = 'instrumentalness'
LIVENESS = 'liveness'
VALENCE = 'valence'
TEMPO = 'tempo'
DURATION_MS = 'duration_ms'
TIME_SIGNATURE = 'time_signature'

SPOTIFY_FEATURES = [
    DANCEABILITY, ENERGY, KEY, LOUDNESS, MODE, SPEECHINESS, ACOUSTICNESS,
    INSTRUMENTALNESS, LIVENESS, VALENCE, TEMPO, DURATION_MS, TIME_SIGNATURE
]
"""
Spotify features, documentation: 
https://developer.spotify.com/documentation/web-api/reference/#/operations/get-several-audio-features
"""

# SPOTIFY TRACKS ------------------------------------------------------------
SONG_ID = 'song_id'
SONG_NAME = 'song_name'
SONG_NUMBER = 'song_number'
SONG_ARTISTS_NUMBER = 'song_artists_number'

SPOTIFY_TRACKS_IDS_COLS = [ALBUM_ID, SONG_ID, SONG_NAME, SONG_NUMBER, SONG_ARTISTS_NUMBER]
"""Column names in output spotify track ids file."""

# GENIUS STATS ----------------------------------------------------------------

NUMBER_OF_LYRICS = 'number_of_fetched_lyrics'
GENIUS_STATS_COLS = [ALBUM_ID, SPOTIFY_ALBUM, SPOTIFY_ARTIST, NUMBER_OF_LYRICS]
"""Column names in output genius fetch lyrics stats file."""
