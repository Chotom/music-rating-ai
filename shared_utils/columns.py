# RYM RAW DATA ----------------------------------------------------------------
ARTIST = 'artist'
ALBUM = 'album'
DATE = 'date'
RATING = 'rating'
RATING_NUMBER = 'ratings_number'
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

# SPOTIFY RAW FEATURES --------------------------------------------------------
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

SPOTIFY_RAW_FEATURES = [
    DANCEABILITY, ENERGY, KEY, LOUDNESS, MODE, SPEECHINESS, ACOUSTICNESS,
    INSTRUMENTALNESS, LIVENESS, VALENCE, TEMPO, DURATION_MS, TIME_SIGNATURE
]
"""
Spotify features documentation: 
https://developer.spotify.com/documentation/web-api/reference/#/operations/get-several-audio-features
"""

# SPOTIFY RAW TRACKS ----------------------------------------------------------
SONG_ID = 'song_id'
SONG_NAME = 'song_name'
SONG_NUMBER = 'song_number'
SONG_ARTISTS_NUMBER = 'song_artists_number'

SPOTIFY_TRACKS_IDS_COLS = [ALBUM_ID, SONG_ID, SONG_NAME, SONG_NUMBER, SONG_ARTISTS_NUMBER]
"""Column names in output spotify track ids file."""

# SPOTIFY PROCESSED DATA -----------------------------------------------------
NUM_TRACKS = 'num_tracks'
NUM_FEATURES = 'num_features'

SPOTIFY_ALBUM_PROCESSED_COLS = [
    ALBUM, ARTIST, ALBUM_ID, SPOTIFY_ALBUM, SPOTIFY_ARTIST, PREC_MATCH, NUM_TRACKS, NUM_FEATURES
]
"""Column names in output spotify processed album file."""

SPOTIFY_TRACKS_PROCESSED_COLS = [
    SONG_ID, DANCEABILITY, ENERGY, KEY, LOUDNESS, MODE, SPEECHINESS, ACOUSTICNESS,
    INSTRUMENTALNESS, LIVENESS, VALENCE, TEMPO, DURATION_MS, TIME_SIGNATURE,
    ALBUM_ID, SONG_NAME, SONG_NUMBER, SONG_ARTISTS_NUMBER
]
"""Column names in output spotify processed tracks file."""

# FEATURE SELECTION -----------------------------------------------------------

RATING_CLASS = 'rating_class'
DECADE_CLASS = 'decade'
FEATURE = 'feature'

RYM_CORE_FEATURES = [
    DATE,
    GENRES
]
"""Features for RYM."""

SPOTIFY_CORE_FEATURES = [
    DANCEABILITY,
    ENERGY,
    KEY,
    LOUDNESS,
    MODE,
    SPEECHINESS,
    ACOUSTICNESS,
    INSTRUMENTALNESS,
    LIVENESS,
    VALENCE,
    TEMPO,
    DURATION_MS,
    TIME_SIGNATURE
]
"""Features for Spotify."""
