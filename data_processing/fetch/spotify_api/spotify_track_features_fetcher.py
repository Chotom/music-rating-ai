import os
import sys
import pandas as pd
import pyprind
import requests
from typing import List, Dict
from requests import Response

from data_processing.fetch.spotify_api.spotify_data_collection import SpotifyFetcher
from data_processing.fetch.spotify_api.data_models.spotify_track_features_model import TrackFeatureModel
from shared_utils.utils import PROJECT_DIR


class SpotifyTrackFeaturesFetcher(SpotifyFetcher):
    def __init__(
            self,
            client_id: str,
            client_secret: str,
            spotify_track_ids_input_filepath: str,
            spotify_track_features_output_filepath: str
    ):
        super().__init__(client_id, client_secret)

        self.input_filepath = spotify_track_ids_input_filepath
        self.output_filepath = spotify_track_features_output_filepath
        self._prepare_input_ids()

    def _prepare_input_ids(self):
        """Prepare track IDs to fetch from spotify, based on fetched tracks."""
        df_track_ids = pd.read_csv(self.input_filepath)
        self._track_ids: pd.Series = df_track_ids['song_id']

        if os.path.exists(self.output_filepath):
            df_track_ids = pd.read_csv(self.output_filepath)
            self._track_ids = self._track_ids[~self._track_ids.isin(df_track_ids['id'])]
        self._logger.info(f'Number of albums to fetch track ids: {len(self._track_ids)}')

    def fetch(self):
        chunk_size = 100
        progress_bar = pyprind.ProgBar(int(len(self._track_ids) / chunk_size), stream=sys.stdout)
        for track_ids in self._ids_by_chunks(chunk_size):
            resp: Response = self._send_for_tracks_features(track_ids)

            # Handle response
            if resp.status_code == 429 or resp.status_code == 401:
                retry_after = resp.headers.get("Retry-After", "Cannot get value")
                self._raise_too_many_request_error(retry_after)
            elif resp.status_code != 200:
                self._logger.warning(f'Cannot fetch ids {resp.status_code}: {resp.content}.')
            else:
                self._handle_successful_response(resp, track_ids)
            progress_bar.update()

    def _ids_by_chunks(self, chunk_size):
        for i in range(0, len(self._track_ids), chunk_size):
            yield self._track_ids[i:i + chunk_size]

    def _send_for_tracks_features(self, ids: List) -> Response:
        base_url = f'https://api.spotify.com/v1/audio-features?ids={",".join(ids)}'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._token}'
        }
        return requests.get(base_url, headers=headers)

    def _handle_successful_response(self, resp: Response, track_ids: pd.Series):
        batch: List[Dict[str, str]] = []
        tracks_features = TrackFeatureModel(**resp.json()).audio_features
        for i, features in enumerate(tracks_features):
            if features is not None:
                batch.append(features.dict())
            else:
                self._logger.debug(f'Feature not found for {track_ids.values[i]} track.')
        is_file_new = not os.path.exists(self.output_filepath)
        pd.DataFrame(batch).to_csv(self.output_filepath, mode='a', index=False, header=is_file_new)


if __name__ == '__main__':
    search_fetcher = SpotifyTrackFeaturesFetcher(
        os.getenv('SPOTIFY_CLIENT_ID'),
        os.getenv('SPOTIFY_CLIENT_SECRET'),
        f'{PROJECT_DIR}/data/raw/spotify/spotify_tracks_ids.csv',
        f'{PROJECT_DIR}/data/raw/spotify/spotify_tracks_feature.csv'
    )
    search_fetcher.fetch()
