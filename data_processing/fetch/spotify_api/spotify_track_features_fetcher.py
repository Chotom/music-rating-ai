import os
import sys
import pandas as pd
import pyprind
import requests
import shared_utils.columns as c

from typing import List, Dict
from requests import Response

from data_processing.fetch.spotify_api.spotify_data_collection import SpotifyFetcher
from data_processing.fetch.spotify_api.data_models.spotify_track_features_model import TrackFeatureModel


class SpotifyTrackFeaturesFetcher(SpotifyFetcher):
    """
    Class for fetching audio features for tracks from the Spotify API.
    """

    def __init__(
            self,
            client_id: str,
            client_secret: str,
            spotify_track_ids_input_filepath: str,
            spotify_track_features_output_filepath: str
    ):
        """
        Args:
            client_id: Spotify API client ID.
            client_secret: Spotify API client secret.
            spotify_track_ids_input_filepath: Filepath of the input CSV file containing track IDs to fetch data for.
            spotify_track_features_output_filepath: Filepath of the output CSV file to store features.
        """
        super().__init__(client_id, client_secret)

        self.input_filepath = spotify_track_ids_input_filepath
        self.output_filepath = spotify_track_features_output_filepath
        self._prepare_input_ids()

    def _prepare_input_ids(self):
        """Prepare track IDs to fetch from spotify, based on fetched tracks."""
        df_track_ids = pd.read_csv(self.input_filepath)
        self._track_ids: pd.Series = df_track_ids[c.SONG_ID]

        if os.path.exists(self.output_filepath):
            df_track_ids = pd.read_csv(self.output_filepath)
            self._track_ids = self._track_ids[~self._track_ids.isin(df_track_ids[c.SONG_ID])]
        self._logger.info(f'Number of track ids to fetch features: {len(self._track_ids)}')

    def fetch(self):
        """
        Fetch track features for each track ID in self._track_ids.
        Send requests to the Spotify API in chunks and store it in output file.
        """

        chunk_size = 100
        progress_bar = pyprind.ProgBar(int(len(self._track_ids) / chunk_size + 1), stream=sys.stdout)
        for track_ids in self._ids_by_chunks(chunk_size):
            resp: Response = self._send_for_tracks_features(track_ids)

            # Handle response
            if resp.status_code == 200:
                self._handle_successful_response(resp, track_ids)
            elif resp.status_code == 429 or resp.status_code == 401:
                retry_after = resp.headers.get("Retry-After", "Cannot get value")
                self._raise_too_many_request_error(retry_after)
            else:
                self._logger.warning(f'Cannot fetch ids {resp.status_code}: {resp.content}.')
            progress_bar.update()

    def _ids_by_chunks(self, chunk_size):
        """Generate batches of track IDs with given chunk size."""
        for i in range(0, len(self._track_ids), chunk_size):
            yield self._track_ids.iloc[i:i + chunk_size]

    def _send_for_tracks_features(self, ids: List) -> Response:
        """
        Send a request to the Spotify API for track audio features for the provided track ids.
        Returns a `Response` object containing the response from the API.

        Args:
            ids: A list of track ids to get audio features for.

        Returns:
            A `Response` object containing the response from the Spotify API.
        """

        base_url = f'https://api.spotify.com/v1/audio-features?ids={",".join(ids)}'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._token}'
        }
        return requests.get(base_url, headers=headers)

    def _handle_successful_response(self, resp: Response, track_ids: pd.Series):
        """
        Handle a successful response from the Spotify API for track features.

        Args:
            resp: The response from the Spotify API.
            track_ids: The track IDs that the response is for.
        """

        batch: List[Dict[str, str]] = []
        tracks_features = TrackFeatureModel(**resp.json()).audio_features
        for i, features in enumerate(tracks_features):
            if features:
                batch.append(features.dict())
            else:
                self._logger.debug(f'Feature not found for {track_ids.values[i]} track.')
        is_file_new = not os.path.exists(self.output_filepath)
        pd.DataFrame(batch).to_csv(self.output_filepath, mode='a', index=False, header=is_file_new)
