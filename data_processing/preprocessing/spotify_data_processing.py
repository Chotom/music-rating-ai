import pandas as pd
import shared_utils.columns as c

from shared_utils.utils import create_logger
from shared_utils.columns import SPOTIFY_SEARCH_COLS, SPOTIFY_FEATURES


class SpotifyDataProcessor:
    """
    A class to process data fetched from Spotify.

    Args:
        search_result_filepath (str): The file path to the csv file containing the search results for albums.
        track_ids_filepath (str): The file path to the csv file containing the track ids and corresponding album ids.
        track_features_filepath (str): The file path to the csv file containing the track processing.
        search_result_output_filepath (str): The file path to save the processed search results.
        track_features_output_filepath (str): The file path to save the processed track processing.
    """

    def __init__(
            self,
            search_result_filepath: str,
            track_ids_filepath: str,
            track_features_filepath: str,
            search_result_output_filepath: str,
            track_features_output_filepath: str
    ):
        self._logger = create_logger('SpotifyDataProcessor')
        self._search_result_output_filepath = search_result_output_filepath
        self._track_features_output_filepath = track_features_output_filepath

        self._df_search = pd.read_csv(search_result_filepath)
        self._df_track_ids = pd.read_csv(track_ids_filepath)
        self._df_features = pd.read_csv(track_features_filepath)

    def process_and_save(self):
        """
        Calls process_features_and_search_results() in class to clean and merge the input data,
        and then saves the cleaned data to output files.

        1. Process input data from given filepaths in class
        2. Save processed search result and merged tracks ids with processing.
        """

        self._logger.info(f'Features size before clean: {self._df_features.shape}')
        self._logger.info(f'Track ids size before clean: {self._df_track_ids.shape}')
        self._logger.info(f'Album search size before clean: {self._df_search.shape}')

        self._df_search, self._df_features = self.process_features_and_search_results(
            self._df_search,
            self._df_track_ids,
            self._df_features
        )

        self._logger.info(f'Album search size after processing: {self._df_search.shape}')
        self._logger.info(f'Features size after processing: {self._df_features.shape}')

        self._df_search.to_csv(self._search_result_output_filepath, index=False)
        self._df_features.to_csv(self._track_features_output_filepath, index=False)

    @staticmethod
    def process_features_and_search_results(
            df_search: pd.DataFrame,
            df_tracks: pd.DataFrame,
            df_features: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        1. Clean processing and cut outliers.
        2. Merge processing with tracks.
        3. Clean albums search data.
        4. Select first 16 processing for each album.
        5. Select albums with enough amount of processing.

        Args:
            df_search: The input dataframe with search results for albums.
            df_tracks: The input dataframe with track ids and corresponding album ids.
            df_features: The input dataframe with track processing.

        Returns:
            A tuple of cleaned search results and merged track processing dataframes.
        """

        df_tracks = df_tracks.drop_duplicates(subset=[c.SONG_ID])
        df_features = SpotifyDataProcessor.clear_tracks_features(df_features)
        df_features = df_features.merge(df_tracks, on=c.SONG_ID)
        df_features = SpotifyDataProcessor.remove_albums_with_not_enough_features(df_features, 4)
        df_features = SpotifyDataProcessor.select_top_n_features(df_features, 16, c.SONG_NUMBER)
        df_search = SpotifyDataProcessor.clear_search_results(df_search, df_tracks, df_features)

        return df_search, df_features

    @staticmethod
    def clear_tracks_features(df_features: pd.DataFrame) -> pd.DataFrame:
        """
        Remove outliers from the track processing dataframe.

        Args:
            df_features: The input dataframe with Spotify track processing.

        Returns:
            Copy of input dataframe with removed outliers.
        """
        assert all(col in df_features.columns for col in SPOTIFY_FEATURES), 'Input is missing processing columns.'

        df = SpotifyDataProcessor._prepare_df(df_features)
        df = SpotifyDataProcessor._replace_outliers(df)
        df = SpotifyDataProcessor._cut_outliers(df)

        return df

    @staticmethod
    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the input feature dataframe for outlier removal.

        Args:
            df: The input dataframe with Spotify track processing.

        Returns:
            Copy of input dataframe with removed null values and duplicates.
        """
        return df.dropna().drop_duplicates(subset=[c.SONG_ID]).round(4)

    @staticmethod
    def _replace_outliers(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace outlier values in the dataframe with certain min and max.

        Args:
            df: The input dataframe with Spotify track processing.

        Returns:
            Copy of input dataframe with outlier values replaced.
        """
        df.loc[df[c.TEMPO] < 45, c.TEMPO] = 45
        df.loc[df[c.TEMPO] > 220, c.TEMPO] = 220
        df.loc[df[c.DURATION_MS] > 600000, c.DURATION_MS] = 600000
        df.loc[df[c.TIME_SIGNATURE] < 3, c.TIME_SIGNATURE] = 4
        return df

    @staticmethod
    def _cut_outliers(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove outliers from the dataframe based on certain min and max values.

        Args:
            df: The input dataframe with Spotify track processing.

        Returns:
            Copy of input dataframe with outliers removed.
        """
        df = df[df[c.DANCEABILITY].between(0, 1, inclusive='neither')]
        df = df[df[c.ENERGY].between(0, 1)]
        df = df[df[c.KEY].between(0, 11)]
        df = df[df[c.LOUDNESS].between(-35, 0, inclusive='left')]
        df = df[df[c.MODE].isin([0, 1])]
        df = df[df[c.SPEECHINESS].between(0, 0.66, inclusive='neither')]
        df = df[df[c.ACOUSTICNESS].between(0, 1)]
        df = df[df[c.INSTRUMENTALNESS].between(0, 1)]
        df = df[df[c.LIVENESS].between(0, 1)]
        df = df[df[c.VALENCE].between(0, 1)]
        df = df[df[c.DURATION_MS] >= 20000]
        df = df[df[c.TIME_SIGNATURE].between(3, 7)]
        return df

    @staticmethod
    def remove_albums_with_not_enough_features(df_features: pd.DataFrame, min_features: int) -> pd.DataFrame:
        """
        Remove album_id's with less than min_features from input dataframe.

        Args:
            df_features: The input dataframe with Spotify track processing.
            min_features: Minimum number of processing for an album_id.

        Returns:
            Copy of input dataframe with removed album_id's that have less than min_features.
        """
        assert all(col in df_features.columns for col in SPOTIFY_FEATURES), 'Input is missing processing columns.'

        # Get album count.
        df_album_count = df_features.groupby(c.ALBUM_ID).size().reset_index(name='count')

        # Get album with count greater or equal to min_features.
        df_album_id = df_album_count[df_album_count['count'] >= min_features][c.ALBUM_ID]

        # Filter the input dataframe with album's that have at least min_features.
        df_filtered = df_features[df_features[c.ALBUM_ID].isin(df_album_id)]

        return df_filtered

    @staticmethod
    def select_top_n_features(df: pd.DataFrame, n: int, criterion: str) -> pd.DataFrame:
        """
        Select top n processing for each group in a dataframe based on a specified criterion.

        Args:
            df: The input dataframe.
            n: The number of processing to select for each group.
            criterion: The name of the column to use for selecting top n processing.

        Returns:
            A new dataframe with the top n processing selected for each group.
        """

        return df.groupby(c.ALBUM_ID, group_keys=False).apply(lambda x: x.nlargest(n, criterion))

    @staticmethod
    def clear_search_results(
            df_search: pd.DataFrame,
            df_track_ids: pd.DataFrame,
            df_features: pd.DataFrame,
    ) -> pd.DataFrame:
        assert all(col in df_track_ids for col in [c.ALBUM_ID, c.SONG_ID]), 'Input is missing id columns.'
        assert all(col in df_features for col in [c.ALBUM_ID, c.SONG_ID]), 'Input is missing id columns.'
        assert all(col in df_search.columns for col in SPOTIFY_SEARCH_COLS), 'Input is missing some columns.'

        # Prepare df.
        df = df_search.copy()
        df.dropna(inplace=True)
        df.drop_duplicates(subset=[c.ALBUM_ID], inplace=True)
        df.drop_duplicates(subset=[c.ALBUM, c.ARTIST], inplace=True)
        df = df[df[c.PREC_MATCH] >= 3.]

        # Number of tracks per album found in spotify.
        df_tracks_num = df_track_ids.groupby(c.ALBUM_ID).size().reset_index(name=c.NUM_TRACKS)
        df = df.merge(df_tracks_num, how='left', on=[c.ALBUM_ID])

        # Number of tracks per album after processing found in spotify.
        df_features_num = df_features.groupby(c.ALBUM_ID).size().reset_index(name=c.NUM_FEATURES)
        df = df.merge(df_features_num, how='left', on=[c.ALBUM_ID])

        return df.dropna()
