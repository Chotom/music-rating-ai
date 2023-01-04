import os

from data_processing.fetch.spotify_api.spotify_search_album_fetcher import SpotifySearchAlbumFetcher
from data_processing.fetch.spotify_api.spotify_track_features_fetcher import SpotifyTrackFeaturesFetcher
from data_processing.fetch.spotify_api.spotify_track_ids_fetcher import SpotifyTrackIDsFetcher
from shared_utils.utils import PROJECT_DIR
from data_processing.fetch.rym.rym_data_collection import RymFetcher
from data_processing.preprocessing.rym_data_processing import RymDataProcessor

# Initialize variables before running pipelines.
STEPS = {
    'FetchRym': 0,
    'PreprocessRym': 0,
    'SearchSpotifyAlbums': 0,
    'FetchSpotifyTrackIDs': 0,
    'FetchSpotifyTrackFeatures': 1,
}

START_YEAR = 1980
END_YEAR = 1980

spotify_client_id = os.getenv('SPOTIFY_CLIENT_ID')
spotify_client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')

rym_path = f'{PROJECT_DIR}/data/raw/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv'
rym_processed_path = f'{PROJECT_DIR}/data/processed/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv'
spotify_search_path = f'{PROJECT_DIR}/data/raw/spotify/spotify_search_album_id_{START_YEAR}_{END_YEAR}.csv'
spotify_track_ids_path = f'{PROJECT_DIR}/data/raw/spotify/spotify_tracks_ids_{START_YEAR}_{END_YEAR}.csv'
spotify_track_features_path = f'{PROJECT_DIR}/data/raw/spotify/spotify_tracks_feature_{START_YEAR}_{END_YEAR}.csv'

# Run pipeline based on declared steps.
if __name__ == '__main__':
    if STEPS['FetchRym']:
        rym_fetcher = RymFetcher(
            rym_path
        )
        rym_fetcher.fetch(START_YEAR, END_YEAR)

    if STEPS['PreprocessRym']:
        rym_data_processor = RymDataProcessor(
            rym_path,
            rym_processed_path
        )
        rym_data_processor.process()

    if STEPS['SearchSpotifyAlbums']:
        search_fetcher = SpotifySearchAlbumFetcher(
            spotify_client_id,
            spotify_client_secret,
            rym_processed_path,
            spotify_search_path
        )
        search_fetcher.fetch()

    if STEPS['FetchSpotifyTrackIDs']:
        search_fetcher = SpotifyTrackIDsFetcher(
            spotify_client_id,
            spotify_client_secret,
            spotify_search_path,
            spotify_track_ids_path
        )
        search_fetcher.fetch()

    if STEPS['FetchSpotifyTrackFeatures']:
        search_fetcher = SpotifyTrackFeaturesFetcher(
            spotify_client_id,
            spotify_client_secret,
            spotify_track_ids_path,
            spotify_track_features_path
        )
        search_fetcher.fetch()
