import os

from data_processing.fetch.spotify_api.spotify_search_album_fetcher import SpotifySearchAlbumFetcher
from shared_utils.utils import PROJECT_DIR
from data_processing.fetch.rym.rym_data_collection import RymFetcher
from data_processing.preprocessing.rym_data_processing import RymDataProcessor

STEPS = {
    'FetchRym': 1,
    'PreprocessRym': 1,
    'SearchSpotifyAlbums': 1,
}
START_YEAR = 1980
END_YEAR = 1980
rym_path = f'{PROJECT_DIR}/data/raw/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv'
rym_processed_path = f'{PROJECT_DIR}/data/processed/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv'
spotify_search_path = f'{PROJECT_DIR}/data/raw/spotify/spotify_search_album_id_{START_YEAR}_{END_YEAR}.csv'

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
