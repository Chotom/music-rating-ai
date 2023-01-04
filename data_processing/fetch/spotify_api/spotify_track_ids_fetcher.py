import os
import sys
import pandas as pd
import pyprind
import requests
from typing import List, Dict
from requests import Response

from data_processing.fetch.spotify_api.data_models.spotify_album_tracks_model import AlbumInfoModel
from data_processing.fetch.spotify_api.spotify_data_collection import SpotifyFetcher
from shared_utils.utils import SPOTIFY_COLS


class SpotifyTrackIDsFetcher(SpotifyFetcher):
    """
    A class for fetching track data for Spotify albums, based on album ID.
    The output file has the following Spotify data columns: 'album_id', 'track_name', 'track_number',
    'artist', 'duration_ms', 'preview_url'.

    Notice! After sending too many requests the token may expire, and You will have to wait some
    time to download data again. This class will always fetch only 'album_id' values that
    does not already exist in the output file.

    Attributes:
        spotify_ids_input_filepath: Filepath for Spotify searched album ids input data.
        spotify_tracks_ids_output_filepath: Filepath for Spotify track ids output data.
        _spotify_ids: Spotify album IDs to fetch.
    """

    def __init__(
            self,
            client_id: str,
            client_secret: str,
            spotify_ids_input_filepath: str,
            spotify_tracks_ids_output_filepath: str
    ):
        """
        Args:
            client_id: Spotify API client ID.
            client_secret: Spotify API client secret.
            spotify_ids_input_filepath: Filepath for the input data of Spotify album data.
            spotify_tracks_ids_output_filepath:  Filepath for the output of tracks ids.
        """
        super().__init__(client_id, client_secret)

        self.spotify_ids_input_filepath = spotify_ids_input_filepath
        self.spotify_tracks_ids_output_filepath = spotify_tracks_ids_output_filepath
        self._prepare_input_ids()

    def _prepare_input_ids(self):
        """
        Prepare album IDs to fetch from Spotify, based on searched albums. Reads
        in the input file of Spotify album data - if the output file already exists,
        removes any 'album_id' values that have already been fetched from self._spotify_ids.
        """

        self._spotify_ids = self._get_loaded_spotify_ids()
        if os.path.exists(self.spotify_tracks_ids_output_filepath):
            df_song_ids = pd.read_csv(self.spotify_tracks_ids_output_filepath)
            fetched_ids = df_song_ids['album_id'].unique()
            self._spotify_ids = self._spotify_ids[~self._spotify_ids.isin(fetched_ids)]
        self._logger.info(f'Number of albums to fetch track ids: {len(self._spotify_ids)}')

    def _get_loaded_spotify_ids(self) -> pd.Series:
        """
        Reads in the input file of Spotify album data and filters for rows with
        a precision_match value greater than 1.

        Returns:
            Spotify album id's.
        """

        df_spotify_ids = pd.read_csv(self.spotify_ids_input_filepath)
        assert (df_spotify_ids.columns.values == SPOTIFY_COLS).all(), 'Invalid input data structure.'

        df_spotify_ids = df_spotify_ids[df_spotify_ids.notna()]
        df_spotify_ids = df_spotify_ids[df_spotify_ids['precision_match'] > 1]

        return df_spotify_ids['spotify_id']

    def fetch(self):
        """
        Fetch track data for each album ID in self._spotify_ids.
        Send requests to the Spotify API in chunks and store it in output file.
        """

        chunk_size = 20
        progress_bar = pyprind.ProgBar(int(len(self._spotify_ids) / chunk_size), stream=sys.stdout)
        for album_ids in self._ids_by_chunks(chunk_size):
            resp: Response = self._send_for_album_tracks(album_ids)

            if resp.status_code == 200:
                self._handle_successful_response(resp)
            elif resp.status_code == 429 or resp.status_code == 401:
                retry_after = resp.headers.get("Retry-After", "Cannot get value")
                self._raise_too_many_request_error(retry_after)
            else:
                self._logger.warning(f'Cannot fetch ids {resp.status_code}: {resp.content}.')

            progress_bar.update()
        self._logger.info(f'Saved data to {self.spotify_tracks_ids_output_filepath}.')

    def _ids_by_chunks(self, chunk_size):
        """Generate batches of IDs with given chunk size."""
        for i in range(0, len(self._spotify_ids), chunk_size):
            yield self._spotify_ids.iloc[i:i + chunk_size]

    def _send_for_album_tracks(self, ids: List) -> Response:
        """
        Send a request to the Spotify API to retrieve track data for the albums
        specified by the given list of album IDs.

        Args:
            ids: List of album IDs.

        Returns:
            Spotify API response.
        """

        base_url = f'https://api.spotify.com/v1/albums?ids={",".join(ids)}&market=US'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._token}'
        }
        return requests.get(base_url, headers=headers)

    def _handle_successful_response(self, resp: Response):
        """
        Processes a successful response from the Spotify API. Extracts track data
        from the response and stores it in the output file.

        Args:
            resp: Spotify API response.
        """

        batch: List[Dict[str, str]] = []
        albums = AlbumInfoModel(**resp.json()).albums
        for album in albums:
            for track in album.tracks.items:
                record = {
                    'album_id': album.id,
                    'song_id': track.id,
                    'song_name': track.name,
                    'song_number': track.track_number,
                    'song_artists_number': len(track.artists)
                }
                batch.append(record)

        is_file_new = not os.path.exists(self.spotify_tracks_ids_output_filepath)
        pd.DataFrame(batch).to_csv(self.spotify_tracks_ids_output_filepath, mode='a', index=False, header=is_file_new)
