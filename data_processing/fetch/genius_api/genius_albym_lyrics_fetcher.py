import json
import os
import re
import time
import pandas as pd
import asyncio

from tenacity import retry, stop_after_attempt
from typing import Optional, List, Dict

from lyricsgenius import Genius
from lyricsgenius.types import Album, Track

from data_processing.fetch.genius_api.data_models.genius_album_lyrics_model import TrackModel, AlbumLyricsModel
from shared_utils.utils import create_logger


def background(f):
    def wrapped(*args, **kwargs):
        # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


class GeniusDataFetcher:
    """
    Class for fetching lyrics data from Genius API.

    Args:
        spotify_search_album_data_path (str): Path to input CSV file with Spotify album data.
        genius_stats_filepath (str): Path to output CSV file with statistics on fetched data.
        genius_lyrics_dir (str): Path to output directory for JSON files with fetched lyrics data.
    """

    spotify_album_id_col = 'album_id'
    genius_stats_cols = [spotify_album_id_col, 'spotify_album', 'spotify_artist', 'number_of_fetched_lyrics']
    """Column names in output stats file."""

    def __init__(
            self,
            spotify_search_album_data_path: str,
            genius_stats_filepath: str,
            genius_lyrics_dir: str,
    ):
        self._logger = create_logger('GeniusLyricFetcher')
        self._genius_api = Genius()
        self._genius_stats_filepath = genius_stats_filepath
        self._genius_lyrics_dir = genius_lyrics_dir
        self._spotify_search_album_data_path = spotify_search_album_data_path
        self._prepare_input()

    def _prepare_input(self):
        """
        Prepares input data for fetching.
        """

        # Prepare ids to fetch.
        df_spotify = self._load_spotify_albums()
        self._df_albums = self._prepare_tracks_id_to_fetch(df_spotify)

        # Prepare output dir for json files.
        if not os.path.exists(self._genius_lyrics_dir):
            os.makedirs(self._genius_lyrics_dir)
            self._logger.info(f'Created {self._genius_lyrics_dir} directory to store lyrics.')

    def _load_spotify_albums(self) -> pd.DataFrame:
        """
        Loads Spotify album data from input CSV file.

        Returns:
            pd.DataFrame: DataFrame with Spotify album data.
        """

        df_spotify_ids = pd.read_csv(self._spotify_search_album_data_path)
        df_spotify_ids = df_spotify_ids[df_spotify_ids.notna()]
        df_spotify_ids = df_spotify_ids[df_spotify_ids['precision_match'] > 2]
        df_spotify_ids = df_spotify_ids.drop_duplicates(subset=[self.spotify_album_id_col])

        expected_cols = [self.spotify_album_id_col, 'spotify_album', 'spotify_artist']
        assert all(col in df_spotify_ids.columns for col in expected_cols), 'Invalid data structure for input file.'

        return df_spotify_ids[expected_cols]

    def _prepare_tracks_id_to_fetch(self, df: pd.DataFrame) -> pd.DataFrame:
        if os.path.exists(self._genius_stats_filepath):
            df_genius_stats = pd.read_csv(self._genius_stats_filepath)
            assert (df_genius_stats.columns.values == self.genius_stats_cols).all(), 'Invalid data structure in output.'

            ids_already_fetched = df[self.spotify_album_id_col].isin(df_genius_stats[self.spotify_album_id_col])
            df = df.drop(df[ids_already_fetched].index)

        self._logger.info(f'Number of album to fetch: {len(df)}')
        return df

    def fetch(self):
        # Parallel(n_jobs=10)(delayed(self._try_handle_album)(row) for _, row in self._df_albums.iterrows())

        for i, row in self._df_albums.iterrows():
            self._try_handle_album(row)

    # @background
    @retry(stop=stop_after_attempt(5))
    def _try_handle_album(self, record: pd.Series):
        try:
            self._genius_api = Genius()
            self._handle_album(record)
        except Exception as e:
            # Retry 5 times and then continue.
            if (reattempt := self._try_handle_album.retry.statistics['attempt_number']) > 5:
                self._logger.error(f'Error {e} occurred - skip album {record[self.spotify_album_id_col]}')
            else:
                self._logger.error(f'Error occurred: {e}')
                time.sleep(10)
                self._logger.info(f'Trying to fetch lyrics again {reattempt}/5...')
                self._genius_api = Genius()
                raise e

    def _handle_album(self, record: pd.Series):
        spotify_id = record[self.spotify_album_id_col]

        album_name = record['spotify_album']
        if album_name[-1] == ')':
            album_name = re.sub(r"\([^()]*\)", "", album_name).strip()

        artist_name = record['spotify_artist']
        if ' / ' in artist_name:
            artist_name = artist_name.replace(' / ', ' & ')

        stats = {
            self.spotify_album_id_col: spotify_id,
            'spotify_album': album_name,
            'spotify_artist': artist_name,
            'number_of_fetched_lyrics': 0
        }

        genius_album: Optional[Album]
        if genius_album := self._genius_api.search_album(album_name, artist_name):
            genius_model_tracks = self._prepare_track_list(genius_album)
            stats['number_of_fetched_lyrics'] = len(genius_model_tracks)
            album_model = AlbumLyricsModel(
                spotify_id=spotify_id,
                genius_id=genius_album.id,
                artist_name=artist_name,
                album_name=album_name,
                tracks=genius_model_tracks
            )
            self._save_album(album_model, spotify_id)
        self._save_stats(stats)

    def _prepare_track_list(self, genius_album: Album) -> List[TrackModel]:
        track: Track
        genius_tracks: List[TrackModel] = []
        for i, track in enumerate(genius_album.tracks):
            if track.song.lyrics or track.song.lyrics_state == 'complete':
                # 'language' field may be None or empty.
                lang = track.to_dict().get('song').get('language', 'unknown')
                lang = lang if lang else 'unknown'

                song_number = track.number if track.number else i
                fetched_track = TrackModel(
                    song_id=track.id,
                    song_name=track.song.full_title,
                    song_number=song_number,
                    lyrics=self.clean_lyrics(track.song.lyrics),
                    lyrics_state=track.song.lyrics_state,
                    language=lang
                )
                genius_tracks.append(fetched_track)
        return genius_tracks

    def _save_album(self, album_model: AlbumLyricsModel, filename: str):
        if len(album_model.tracks) != 0:
            album_lyrics_file = open(f'{self._genius_lyrics_dir}/{filename}.json', 'w', encoding='utf-8')
            json.dump(album_model.dict(), album_lyrics_file, default=str)
            album_lyrics_file.close()

    def _save_stats(self, stats: Dict):
        is_file_new = not os.path.exists(self._genius_stats_filepath)
        pd.DataFrame([stats]).to_csv(self._genius_stats_filepath, mode='a', index=False, header=is_file_new)

    @staticmethod
    def clean_lyrics(text: str):
        if len(text) == 0 or text is None:
            return ''

        if text.endswith('Embed'):
            text = text[:-5]

        while text[-1].isdigit():
            text = text[:-1]

        if text.endswith('You might also like'):
            text = text[:-19]

        return text
