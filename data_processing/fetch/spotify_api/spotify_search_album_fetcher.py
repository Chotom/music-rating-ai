import math
import os
import time
import requests
import pandas as pd

from dataclasses import dataclass
from typing import Tuple
from requests import Response
from unidecode import unidecode

from data_processing.fetch.spotify_api.spotify_data_collection import SpotifyFetcher
from data_processing.fetch.spotify_api.spotify_search_album_model import Model, Item
from shared_utils.utils import PROJECT_DIR


class SpotifySearchAlbumFetcher(SpotifyFetcher):
    """
    Class for fetching spotify album ids, based on data from RateYourMusic.
    Create output file based on artist name and album data from rym with
    Spotify data columns filled with empty values (spotify columns:
    'spotify_id', 'spotify_album', 'spotify_artist', 'precision_match').
    If 'precision_match' field is empty, the search request will be sent to
    spotify API. Precision_match column show how accurate search was
    (max value is 4 and min is 0). Zero means that normalized name of album or
    artist was not found in spotify. Empty values means that the request wasn't
    sent yet.

    Notice! After sending too many requests the token may expire, and You will
    have to wait some time to download data again. This class will always fetch
    only rows without 'precision_match' value in output file, so don't modify
    this file unless you are sure everything is fetched (this may require to run
    this class few times, due to token expiration).
    """
    spotify_cols = ['album', 'artist', 'spotify_id', 'spotify_album', 'spotify_artist', 'precision_match']
    """Column names in output file."""

    @dataclass
    class SpotifyRecord:
        spotify_id: str = None
        spotify_album: str = None
        spotify_artist: str = None
        precision_match: int = 0

    def __init__(self, client_id: str, client_secret: str, rym_input_filepath: str, spotify_output_filepath: str):
        super().__init__(client_id, client_secret)

        self.rym_input_filepath = rym_input_filepath
        self.spotify_output_filepath = spotify_output_filepath
        self._prepare_output_file()

    def _prepare_output_file(self):
        if not os.path.exists(self.spotify_output_filepath):
            self._create_df()
        self._df_spotify = pd.read_csv(self.spotify_output_filepath)

        assert (self._df_spotify.columns.values == self.spotify_cols).all(), 'Invalid data structure.'
        self._logger.info(f'Output file loaded. Spotify filled data:\n{self._df_spotify.notna().sum()}.')

    def _create_df(self):
        df_rym = pd.read_csv(self.rym_input_filepath)
        self._df_spotify = df_rym[['album', 'artist']].copy()
        self._df_spotify['spotify_id'] = None
        self._df_spotify['spotify_album'] = None
        self._df_spotify['spotify_artist'] = None
        self._df_spotify['precision_match'] = None

        self._logger.info(f'Prepared spotify data filled with empty values. Columns: {self.spotify_cols}.')
        self._save_df()

    def _save_df(self):
        self._df_spotify[self.spotify_cols].to_csv(self.spotify_output_filepath, index=False)
        self._logger.info(f'Saved data to {self.spotify_output_filepath}.')

    def fetch(self):
        self._logger.info(f"{self._df_spotify['precision_match'].isna().sum()} albums id to fetch.")
        self._logger.info(f"{self._df_spotify['precision_match'].notna().sum()} albums already fetched.")

        for index, row in self._df_spotify.iterrows():
            if math.isnan(row['precision_match']):
                if index % 1000 == 0:
                    self._logger.info(f"{index}/{self._df_spotify.shape[0]}: {row['artist']} - {row['album']}")
                    self._save_df()
                record = self._get_album_data(index, row)
                self._df_spotify.loc[index, 'spotify_id'] = record.spotify_id
                self._df_spotify.loc[index, 'spotify_album'] = record.spotify_album
                self._df_spotify.loc[index, 'spotify_artist'] = record.spotify_artist
                self._df_spotify.loc[index, 'precision_match'] = record.precision_match
            # else: album is already fetched and row['precision_match'] is not nan.
        self._save_df()

    def _get_album_data(self, index: int, row: pd.Series) -> SpotifyRecord:
        self._album = row['album']
        self._artist = row['artist']
        record = self.SpotifyRecord()
        resp: Response = self._send_search_request_for_album()

        # Handle response
        if resp.status_code == 429 or resp.status_code == 401:
            retry_after = resp.headers.get("Retry-After", "Cannot get value")
            self._raise_too_many_request_error_for_album(index, retry_after)
        elif resp.status_code != 200:
            self._logger.warning(f'Cannot fetch album {index}: {self._album}, {resp.status_code}: {resp.content}.')
        else:
            record = self._handle_successful_response(resp)

        return record

    def _send_search_request_for_album(self) -> Response:
        base_url = 'https://api.spotify.com/v1/search'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {self._token}'
        }
        data = {
            'q': f'{self._album} {self._artist}',
            'type': 'album',
            'market': 'NA',
            'limit': 10
        }
        return requests.get(base_url, params=data, headers=headers)

    def _raise_too_many_request_error_for_album(self, i: int, retry_after: str):
        try:
            retry_after_date = str(time.strftime('%H:%M:%S', time.gmtime(int(retry_after))))
        except ValueError:
            retry_after_date = retry_after

        err_msg = f'Error during fetching album {i}: {self._album}, too many requests - try after: {retry_after_date}'
        self._logger.error(err_msg)
        self._save_df()
        raise Exception(err_msg)

    def _handle_successful_response(self, resp: Response) -> SpotifyRecord:
        record = self.SpotifyRecord()
        results = Model(**resp.json()).albums.items
        if len(results) == 0:
            self._logger.warning(f'Missing results for: {self._artist} - {self._album}')
            return record

        result, record.precision_match = self._match_best_item(results)
        record.spotify_id = result.id
        record.spotify_album = result.name
        record.spotify_artist = ' / '.join(result.get_artists_name())
        return record

    def _match_best_item(self, results: list[Item]) -> Tuple[Item, int]:
        best_match, best_nr = 0, 0
        for item_nr, item in enumerate(results):
            names = item.get_artists_name()
            m1 = self._exact_name_match(names, self._artist)
            m2 = self._contain_exact_name_match(names, self._artist)

            album = item.name
            m3 = self._exact_name_match([album], self._album)
            m4 = self._contain_exact_name_match([album], self._album)

            if best_match < m1 + m2 + m3 + m4:
                best_match = m1 + m2 + m3 + m4
                best_nr = item_nr
        return results[best_nr], best_match

    def _exact_name_match(self, names: list[str], name_to_match: str) -> bool:
        for name in names:
            if unidecode(name_to_match.lower()) == unidecode(name.lower()):
                return True
        return False

    def _contain_exact_name_match(self, names: list[str], name_to_match: str) -> bool:
        name_to_match = unidecode(name_to_match.lower())
        for name in names:
            name = unidecode(name.lower())
            if name_to_match in name or name in name_to_match:
                return True
        return False


if __name__ == '__main__':
    search_fetcher = SpotifySearchAlbumFetcher(
        os.getenv('SPOTIFY_CLIENT_ID'),
        os.getenv('SPOTIFY_CLIENT_SECRET'),
        f'{PROJECT_DIR}/data/processed/rym_charts.csv',
        f'{PROJECT_DIR}/data/raw/spotify/spotify_search_album_id.csv'
    )
    search_fetcher.fetch()
