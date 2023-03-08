import pandas as pd

from shared_utils.utils import SPOTIFY_FEATURES, create_logger, SPOTIFY_COLS


class SpotifyDataProcessor:
    """
    A class to process data fetched from Spotify.

    Args:
        search_result_filepath (str): The file path to the csv file containing the search results for albums.
        track_ids_filepath (str): The file path to the csv file containing the track ids and corresponding album ids.
        track_features_filepath (str): The file path to the csv file containing the track features.
        search_result_output_filepath (str): The file path to save the processed search results.
        track_features_output_filepath (str): The file path to save the processed track features.
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
        2. Save processed search result and merged tracks ids with features.
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
        1. Clean features and cut outliers.
        2. Merge features with tracks.
        3. Clean albums search data.
        4. Select first 16 features for each album.
        5. Select albums with enough amount of features.

        Args:
            df_search: The input dataframe with search results for albums.
            df_tracks: The input dataframe with track ids and corresponding album ids.
            df_features: The input dataframe with track features.

        Returns:
            A tuple of cleaned search results and merged track features dataframes.
        """

        df_tracks = df_tracks.drop_duplicates(subset=['song_id'])
        df_features = SpotifyDataProcessor.clear_tracks_features(df_features).rename(columns={'id': 'song_id'})
        df_features = df_features.merge(df_tracks, on='song_id')
        df_features = SpotifyDataProcessor.remove_albums_with_not_enough_features(df_features, 4)
        df_features = SpotifyDataProcessor.select_top_n_features(df_features, 16, 'song_number')
        df_search = SpotifyDataProcessor.clear_search_results(df_search, df_tracks, df_features)

        return df_search, df_features

    @staticmethod
    def clear_tracks_features(df_features: pd.DataFrame) -> pd.DataFrame:
        """
        Remove outliers from the track features dataframe.

        Args:
            df_features: The input dataframe with Spotify track features.

        Returns:
            Copy of input dataframe with removed outliers.
        """
        assert all(col in df_features.columns for col in SPOTIFY_FEATURES), 'Input is missing features columns.'

        df = SpotifyDataProcessor._prepare_df(df_features)
        df = SpotifyDataProcessor._replace_outliers(df)
        df = SpotifyDataProcessor._cut_outliers(df)

        return df

    @staticmethod
    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the input feature dataframe for outlier removal.

        Args:
            df: The input dataframe with Spotify track features.

        Returns:
            Copy of input dataframe with removed null values and duplicates.
        """
        return df.dropna().drop_duplicates(subset=['id']).round(4)

    @staticmethod
    def _replace_outliers(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace outlier values in the dataframe with certain min and max.

        Args:
            df: The input dataframe with Spotify track features.

        Returns:
            Copy of input dataframe with outlier values replaced.
        """
        df.loc[df['tempo'] < 45, 'tempo'] = 45
        df.loc[df['tempo'] > 220, 'tempo'] = 220
        df.loc[df['duration_ms'] > 600000, 'duration_ms'] = 600000
        df.loc[df['time_signature'] < 3, 'time_signature'] = 4
        return df

    @staticmethod
    def _cut_outliers(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove outliers from the dataframe based on certain min and max values.

        Args:
            df: The input dataframe with Spotify track features.

        Returns:
            Copy of input dataframe with outliers removed.
        """
        df = df[df['danceability'].between(0, 1, inclusive='neither')]
        df = df[df['energy'].between(0, 1)]
        df = df[df['key'].between(0, 11)]
        df = df[df['loudness'].between(-35, 0, inclusive='left')]
        df = df[df['mode'].isin([0, 1])]
        df = df[df['speechiness'].between(0, 0.66, inclusive='neither')]
        df = df[df['acousticness'].between(0, 1)]
        df = df[df['instrumentalness'].between(0, 1)]
        df = df[df['liveness'].between(0, 1)]
        df = df[df['valence'].between(0, 1)]
        df = df[df['duration_ms'] >= 20000]
        df = df[df['time_signature'].between(3, 7)]
        return df

    @staticmethod
    def remove_albums_with_not_enough_features(df_features: pd.DataFrame, min_features: int) -> pd.DataFrame:
        """
        Remove album_id's with less than min_features from input dataframe.

        Args:
            df_features: The input dataframe with Spotify track features.
            min_features: Minimum number of features for an album_id.

        Returns:
            Copy of input dataframe with removed album_id's that have less than min_features.
        """
        assert all(col in df_features.columns for col in SPOTIFY_FEATURES), 'Input is missing features columns.'

        # Get album count.
        df_album_count = df_features.groupby('album_id').size().reset_index(name='count')

        # Get album with count greater or equal to min_features.
        df_album_id = df_album_count[df_album_count['count'] >= min_features]['album_id']

        # Filter the input dataframe with album's that have at least min_features.
        df_filtered = df_features[df_features['album_id'].isin(df_album_id)]

        return df_filtered

    @staticmethod
    def select_top_n_features(df: pd.DataFrame, n: int, criterion: str) -> pd.DataFrame:
        """
        Select top n features for each group in a dataframe based on a specified criterion.

        Args:
            df: The input dataframe.
            n: The number of features to select for each group.
            criterion: The name of the column to use for selecting top n features.

        Returns:
            A new dataframe with the top n features selected for each group.
        """

        return df.groupby('album_id', group_keys=False).apply(lambda x: x.nlargest(n, criterion))

    @staticmethod
    def clear_search_results(
            df_search: pd.DataFrame,
            df_track_ids: pd.DataFrame,
            df_features: pd.DataFrame,
    ) -> pd.DataFrame:
        assert all(col in df_track_ids for col in ['album_id', 'song_id']), 'Input is missing id columns.'
        assert all(col in df_features for col in ['album_id', 'song_id']), 'Input is missing id columns.'
        assert all(col in df_search.columns for col in SPOTIFY_COLS), 'Input is missing some columns.'

        # Prepare df.
        df = df_search.copy()
        df.dropna(inplace=True)
        df.drop_duplicates(subset=['spotify_id'], inplace=True)
        df.rename(columns={'spotify_id': 'album_id'}, inplace=True)
        df = df[df['precision_match'] >= 3.]

        # Number of tracks per album found in spotify.
        df_tracks_num = df_track_ids.groupby('album_id').size().reset_index(name='num_tracks')
        df = df.merge(df_tracks_num, how='left', on=['album_id'])

        # Number of tracks per album after processing found in spotify.
        df_features_num = df_features.groupby('album_id').size().reset_index(name='num_features')
        df = df.merge(df_features_num, how='left', on=['album_id'])

        return df.dropna()
