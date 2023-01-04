import os
import time
import requests
import pandas as pd
from dataclasses import dataclass
from typing import Tuple, Optional
from requests import Response

from data_processing.fetch.spotify_api.spotify_data_collection import SpotifyFetcher
from data_processing.fetch.spotify_api.data_models.spotify_search_album_model import SearchModel, Item
from shared_utils.utils import SPOTIFY_COLS, clear_album_name, clear_artist_name


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
    sent yet. Search end when all values in 'precision_match' column are filled.

    Notice! After sending too many requests the token may expire, and You will
    have to wait some time to download data again. This class will always fetch
    only rows without 'precision_match' value in output file, so don't modify
    this file unless everything is fetched and whole 'precision_match' column
    is filled (this may require to run this class few times, due to token
    expiration).

    Attributes:
        rym_input_filepath: Filepath for RateYourMusic input data.
        spotify_output_filepath: Filepath for Spotify output data.
        _df_spotify: Output dataframe with Spotify data.
    """

    @dataclass
    class SpotifyRecord:
        spotify_id: Optional[str] = None
        spotify_album: Optional[str] = None
        spotify_artist: Optional[str] = None
        precision_match: int = 0

    def __init__(self, client_id: str, client_secret: str, rym_input_filepath: str, spotify_output_filepath: str):
        """
        Args:
            client_id: Spotify API client ID.
            client_secret: Spotify API client secret.
            rym_input_filepath: Path to file with RateYourMusic data.
            spotify_output_filepath: Path to output file for Spotify data.
        """
        super().__init__(client_id, client_secret)

        self.rym_input_filepath = rym_input_filepath
        self.spotify_output_filepath = spotify_output_filepath
        self._prepare_output_file()

    def _prepare_output_file(self):
        """Prepare output file to search data."""
        if not os.path.exists(self.spotify_output_filepath):
            self._create_output_df()
        self._df_spotify = pd.read_csv(self.spotify_output_filepath)

        assert (self._df_spotify.columns.values == SPOTIFY_COLS).all(), 'Invalid data structure.'
        self._logger.info(f'Output file loaded. Spotify filled data:\n{self._df_spotify.notna().sum()}.')

    def _create_output_df(self):
        """Create output file based on artist name and album data from rym."""
        df_rym = pd.read_csv(self.rym_input_filepath)
        self._df_spotify = df_rym[['album', 'artist']].copy()
        self._df_spotify['spotify_id'] = None
        self._df_spotify['spotify_album'] = None
        self._df_spotify['spotify_artist'] = None
        self._df_spotify['precision_match'] = None

        self._logger.info(f'Prepared spotify data filled with empty values. Columns: {SPOTIFY_COLS}.')
        self._save_df()

    def _save_df(self):
        """Save self._df_spotify in self.spotify_output_filepath CSV file."""
        self._df_spotify[SPOTIFY_COLS].to_csv(self.spotify_output_filepath, index=False)
        self._logger.info(f'Saved data to {self.spotify_output_filepath}.')

    def fetch(self):
        """
        Send search album request to Spotify API for every album with
        empty 'precision_match' value in dataframe. Add Spotify data to output
        file, including 'precision_match' value. Save output file every 100th request
        to spotify_output_filepath.
        """

        self._logger.info(f"{self._df_spotify['precision_match'].isna().sum()} albums id to fetch.")
        self._logger.info(f"{self._df_spotify['precision_match'].notna().sum()} albums already fetched.")

        empty_precision_mask = self._df_spotify['precision_match'].isna()
        for i, row in self._df_spotify[empty_precision_mask].iterrows():
            if i % 100 == 0:
                self._logger.info(f'{i}/{self._df_spotify.shape[0]}')
                self._save_df()

            record = self._get_album_data(i, row)
            self._df_spotify.loc[i, 'spotify_id'] = record.spotify_id
            self._df_spotify.loc[i, 'spotify_album'] = record.spotify_album
            self._df_spotify.loc[i, 'spotify_artist'] = record.spotify_artist
            self._df_spotify.loc[i, 'precision_match'] = record.precision_match

        self._logger.info(f'Processed finished {self._df_spotify.shape[0]}.')
        self._save_df()

    def _get_album_data(self, index: int, row: pd.Series) -> SpotifyRecord:
        """
        Retrieve album data from Spotify API based on album and artist name.

        Args:
            index: Index of album in output dataframe.
            row: Series object with 'album' and 'artist' data for the current album.

        Returns:
            SpotifyRecord obj with album data from Spotify or empty obj if album was not found or an error occurred.
        """

        self._album = row['album']
        self._artist = row['artist']
        record = self.SpotifyRecord()
        resp: Response = self._send_search_request_for_album()

        # Handle response
        if resp.status_code == 429 or resp.status_code == 401:
            retry_after = resp.headers.get('Retry-After', 'Cannot get value')
            self._raise_too_many_request_error_for_album(index, retry_after)
        elif resp.status_code != 200:
            self._logger.warning(f'Cannot fetch album {index}: {self._album}, {resp.status_code}: {resp.content}.')
        else:
            record = self._handle_successful_response(resp)

        return record

    def _send_search_request_for_album(self) -> Response:
        """
        Send get request to search list of matched albums.

        Returns:
            Spotify response of search.
        """
        base_url = 'https://api.spotify.com/v1/search'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {self._token}'
        }
        data = {
            'q': f'{self._album} artist:{self._artist}',
            'type': 'album',
            'market': 'US',
            'limit': 10
        }
        return requests.get(base_url, params=data, headers=headers)

    def _raise_too_many_request_error_for_album(self, i: int, retry_after: str):
        """
        Handle 'too many requests' error when fetching album data from Spotify API.

        Args:
            i: Number of the album being fetched.
            retry_after: Value of the 'Retry-After' header in the response.

        Raises:
            requests.ConnectionError: If 'too many requests' error occurs.
        """

        try:
            # Convert number of seconds to a time string in HH:MM:SS format.
            retry_after_date = str(time.strftime('%H:%M:%S', time.gmtime(int(retry_after))))
        except ValueError:
            # If the 'Retry-After' header is not a number, use the date string as it is.
            retry_after_date = retry_after

        err_msg = f'Error during fetching album {i}: {self._album}, too many requests - try after: {retry_after_date}'
        self._logger.error(err_msg)
        self._save_df()
        raise requests.ConnectionError(err_msg)

    def _handle_successful_response(self, resp: Response) -> SpotifyRecord:
        """
        Args:
            resp: Response from the Spotify API search request.

        Returns:
            A SpotifyRecord object containing the album id, album name, artist name, and the precision match score.
        """

        results = SearchModel(**resp.json()).albums.items
        if len(results) == 0:
            self._logger.debug(f'Missing results for: {self._artist} - {self._album}')
            return self.SpotifyRecord()

        result, precision_match = self._match_best_item(results)
        return self.SpotifyRecord(
            spotify_id=result.id,
            spotify_album=result.name,
            spotify_artist=' / '.join(result.get_artists_name()),
            precision_match=precision_match,
        )

    def _match_best_item(self, results: list[Item]) -> Tuple[Item, int]:
        """
        Find the best match for album and artist among the given search results.

        Args:
           results: List of search results for given album and artist.

        Returns:
           The best matching item and its precision match rate.
        """

        best_item, best_rate = results[0], 0
        for item in results:
            names = item.get_artists_name()
            album = item.name

            match_rate = 0
            match_rate += self._exact_name_match(names, self._artist)
            match_rate += self._contain_exact_name_match(names, self._artist)
            match_rate += self._exact_name_match_album(album, self._album)
            match_rate += self._contain_exact_name_match_album(album, self._album)

            if match_rate > best_rate:
                best_rate = match_rate
                best_item = item

        if best_rate == 0:
            self._logger.debug(f'Precision match = 0 for: {self._artist} - {self._album}')

        return best_item, best_rate

    @staticmethod
    def _exact_name_match(names: list[str], name_to_match: str) -> bool:
        for name in names:
            if clear_artist_name(name_to_match) == clear_artist_name(name):
                return True
        return False

    @staticmethod
    def _contain_exact_name_match(names: list[str], name_to_match: str) -> bool:
        name_to_match = clear_artist_name(name_to_match)
        for name in names:
            name = clear_artist_name(name)
            if name_to_match in name or name in name_to_match:
                return True
        return False

    @staticmethod
    def _exact_name_match_album(name: str, name_to_match: str) -> bool:
        return clear_album_name(name_to_match) == clear_album_name(name)

    @staticmethod
    def _contain_exact_name_match_album(name: str, name_to_match: str) -> bool:
        name_to_match = clear_album_name(name_to_match)
        name = clear_album_name(name)
        return name_to_match in name or name in name_to_match
