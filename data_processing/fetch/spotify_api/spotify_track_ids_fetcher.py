import os
import time
import pandas as pd
import requests
from typing import List, Dict
from requests import Response

from data_processing.fetch.spotify_api.data_models.spotify_album_tracks_model import AlbumInfoModel
from data_processing.fetch.spotify_api.spotify_data_collection import SpotifyFetcher
from shared_utils.utils import PROJECT_DIR


class SpotifyTrackIDsFetcher(SpotifyFetcher):
    spotify_ids_cols = ['album', 'artist', 'spotify_id', 'spotify_album', 'spotify_artist', 'precision_match']
    """Column names in input file."""

    def __init__(
            self,
            client_id: str,
            client_secret: str,
            spotify_ids_input_filepath: str,
            spotify_tracks_ids_output_filepath: str
    ):
        super().__init__(client_id, client_secret)

        self.spotify_ids_input_filepath = spotify_ids_input_filepath
        self.spotify_tracks_ids_output_filepath = spotify_tracks_ids_output_filepath
        self._prepare_input_ids()

    def _prepare_input_ids(self):
        """Prepare album IDs to fetch from spotify, based on searched albums."""
        df_spotify_ids = pd.read_csv(self.spotify_ids_input_filepath)
        df_spotify_ids = df_spotify_ids[df_spotify_ids.notna()]
        df_spotify_ids = df_spotify_ids[df_spotify_ids['precision_match'] > 1]
        self._spotify_ids: pd.Series = df_spotify_ids['spotify_id']

        if os.path.exists(self.spotify_tracks_ids_output_filepath):
            df_song_ids = pd.read_csv(self.spotify_tracks_ids_output_filepath)
            fetched_ids = df_song_ids['album_id'].unique()
            self._spotify_ids = self._spotify_ids[~self._spotify_ids.isin(fetched_ids)]
        self._logger.info(f'Number of albums to fetch track ids: {len(self._spotify_ids)}')

    def fetch(self):
        chunk_size = 20
        for i, album_ids in enumerate(self._ids_by_chunks(chunk_size)):
            print(f'Batch {i}/{int(len(self._spotify_ids) / chunk_size)}')
            resp: Response = self._send_for_album_tracks(album_ids)

            # Handle response
            if resp.status_code == 429 or resp.status_code == 401:
                retry_after = resp.headers.get("Retry-After", "Cannot get value")
                self._raise_too_many_request_error(retry_after)
            elif resp.status_code != 200:
                self._logger.warning(f'Cannot fetch ids {resp.status_code}: {resp.content}.')
            else:
                self._handle_successful_response(resp)

    def _ids_by_chunks(self, chunk_size):
        for i in range(0, len(self._spotify_ids), chunk_size):
            yield self._spotify_ids[i:i + chunk_size]

    def _send_for_album_tracks(self, ids: List) -> Response:
        base_url = f'https://api.spotify.com/v1/albums?ids={",".join(ids)}&market=NA'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._token}'
        }
        return requests.get(base_url, headers=headers)

    def _raise_too_many_request_error(self, retry_after: str):
        try:
            retry_after_date = str(time.strftime('%H:%M:%S', time.gmtime(int(retry_after))))
        except ValueError:
            retry_after_date = retry_after
        err_msg = f'Error during fetching tracks ids: , too many requests - try after: {retry_after_date}'
        self._logger.error(err_msg)
        raise Exception(err_msg)

    def _handle_successful_response(self, resp: Response):
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


if __name__ == '__main__':
    search_fetcher = SpotifyTrackIDsFetcher(
        os.getenv('SPOTIFY_CLIENT_ID'),
        os.getenv('SPOTIFY_CLIENT_SECRET'),
        f'{PROJECT_DIR}/data/raw/spotify/spotify_search_album_id.csv',
        f'{PROJECT_DIR}/data/raw/spotify/spotify_tracks_ids.csv'
    )
    search_fetcher.fetch()
