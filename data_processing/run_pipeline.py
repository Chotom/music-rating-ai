import os

from data_processing.fetch.spotify_api.spotify_search_album_fetcher import SpotifySearchAlbumFetcher
from data_processing.fetch.spotify_api.spotify_track_ids_fetcher import SpotifyTrackIDsFetcher
from shared_utils.utils import PROJECT_DIR
from data_processing.fetch.rym.rym_data_collection import RymFetcher
from data_processing.preprocessing.rym_data_processing import RymDataProcessor

STEPS = {
    'FetchRym': 0,
    'PreprocessRym': 0,
    'SearchSpotifyAlbums': 0,
    'FetchSpotifyTrackIDs': 1,
}
START_YEAR = 1980
END_YEAR = 1980
rym_path = f'{PROJECT_DIR}/data/raw/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv'
rym_processed_path = f'{PROJECT_DIR}/data/processed/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv'
spotify_search_path = f'{PROJECT_DIR}/data/raw/spotify/spotify_search_album_id_{START_YEAR}_{END_YEAR}.csv'
spotify_track_ids_path = f'{PROJECT_DIR}/data/raw/spotify/spotify_tracks_ids_{START_YEAR}_{END_YEAR}.csv'

if STEPS['FetchRym']:
    RymFetcher(rym_path).fetch(START_YEAR, END_YEAR)

if STEPS['PreprocessRym']:
    RymDataProcessor(rym_path, rym_processed_path).process()

if STEPS['SearchSpotifyAlbums']:
    search_fetcher = SpotifySearchAlbumFetcher(
        os.getenv('SPOTIFY_CLIENT_ID'),
        os.getenv('SPOTIFY_CLIENT_SECRET'),
        rym_processed_path,
        spotify_search_path
    )
    search_fetcher.fetch()

if STEPS['FetchSpotifyTrackIDs']:
    search_fetcher = SpotifyTrackIDsFetcher(
        os.getenv('SPOTIFY_CLIENT_ID'),
        os.getenv('SPOTIFY_CLIENT_SECRET'),
        spotify_search_path,
        spotify_track_ids_path
    )
    search_fetcher.fetch()
