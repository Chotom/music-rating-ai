import numpy as np
import pandas as pd
from tqdm import tqdm

from shared_utils import columns as c
from shared_utils.utils import PROJECT_DIR, MAX_KEY, MIN_LOUDNESS, MAX_SPEECHINESS, MIN_TEMPO, MAX_TEMPO, \
    MIN_DURATION_MS, MAX_DURATION_MS


class SpotifyFeatureSelection:
    """
       This class contains methods to merge processed data to various variants
       of final dataset.
       """

    def __init__(
            self,
            spotify_features_processed_path: str,
            spotify_output_path: str,
    ):
        """
        Args:
            spotify_features_processed_path: Input file for Spotify features.
            spotify_output_path: Output path.
        """

        self.output_path = spotify_output_path
        self._df_features = pd.read_csv(spotify_features_processed_path)

    def run_and_save(self):
        df = self._df_features.copy()
        df = self._select_features(df)
        df = self._transform_features(df)
        df = self._set_to_max_length(df, 16)

        self._save(df)

    @staticmethod
    def _select_features(df: pd.DataFrame) -> pd.DataFrame:
        """Returns: Dataframe with features, ALBUM_ID columns."""

        return df[[c.ALBUM_ID, c.SONG_NUMBER] + c.SPOTIFY_CORE_FEATURES]

    def _transform_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform every feature if is in the selected columns."""

        if c.DANCEABILITY in df.columns:
            self._transform_danceability(df)

        if c.ENERGY in df.columns:
            self._transform_energy(df)

        if c.KEY in df.columns:
            self._transform_key(df)

        if c.LOUDNESS in df.columns:
            self._transform_loudness(df)

        if c.MODE in df.columns:
            self._transform_mode(df)

        if c.SPEECHINESS in df.columns:
            self._transform_speechiness(df)

        if c.ACOUSTICNESS in df.columns:
            self._transform_acousticness(df)

        if c.INSTRUMENTALNESS in df.columns:
            self._transform_instrumentalness(df)

        if c.LIVENESS in df.columns:
            self._transform_liveness(df)

        if c.VALENCE in df.columns:
            self._transform_valence(df)

        if c.TEMPO in df.columns:
            self._transform_tempo(df)

        if c.DURATION_MS in df.columns:
            self._transform_duration_ms(df)

        if c.TIME_SIGNATURE in df.columns:
            self._transform_time_signature(df)

        df[c.SPOTIFY_CORE_FEATURES] = df[c.SPOTIFY_CORE_FEATURES].round(4)

        return df

    @staticmethod
    def _transform_danceability(df):
        """Danceability for now doesn't need to be transformed."""
        pass

    @staticmethod
    def _transform_energy(df):
        """Energy for now doesn't need to be transformed."""
        pass

    @staticmethod
    def _transform_key(df):
        df[c.KEY] /= MAX_KEY

    @staticmethod
    def _transform_loudness(df):
        min_lin_loudness = 10 ** (MIN_LOUDNESS / 20)
        df[c.LOUDNESS] = 10 ** (df[c.LOUDNESS] / 20)
        df[c.LOUDNESS] = (df[c.LOUDNESS] - min_lin_loudness) / (1 - min_lin_loudness)

    @staticmethod
    def _transform_mode(df):
        """Mode for now doesn't need to be transformed."""
        pass

    @staticmethod
    def _transform_speechiness(df):
        df[c.SPEECHINESS] /= MAX_SPEECHINESS

    @staticmethod
    def _transform_acousticness(df):
        """Acousticness for now doesn't need to be transformed."""
        pass

    @staticmethod
    def _transform_instrumentalness(df):
        """Instrumentalness for now doesn't need to be transformed."""
        pass

    @staticmethod
    def _transform_liveness(df):
        """Liveness for now doesn't need to be transformed."""
        pass

    @staticmethod
    def _transform_valence(df):
        """Valence for now doesn't need to be transformed."""
        pass

    @staticmethod
    def _transform_tempo(df):
        df[c.TEMPO] = (df[c.TEMPO] - MIN_TEMPO) / (MAX_TEMPO - MIN_TEMPO)

    @staticmethod
    def _transform_duration_ms(df):
        df[c.DURATION_MS] = (df[c.DURATION_MS] - MIN_DURATION_MS) / (MAX_DURATION_MS - MIN_DURATION_MS)

    @staticmethod
    def _transform_time_signature(df):
        df[c.TIME_SIGNATURE] = np.where(df[c.TIME_SIGNATURE] == 4, 1., 0.)

    @staticmethod
    def _set_to_max_length(df: pd.DataFrame, max_len: int, fill_type: str = 'repeat'):
        track_counts = df[c.ALBUM_ID].value_counts()
        track_counts = track_counts[track_counts != 16]

        albums = df.groupby(c.ALBUM_ID).apply(np.array)

        filled_rows = []
        for album_id, count in track_counts.items():
            missing_records_count = max_len - count
            features = albums[album_id][:, 1:]

            if fill_type == 'mean':
                features = features.mean(axis=0)
                album_data = [(album_id, count + i + 1, *features[1:]) for i in range(missing_records_count)]
                filled_rows.extend(album_data)
            elif fill_type == 'repeat':
                features = features[features[:, 0].argsort()][:, 1:]
                album_data = [(album_id, count + i + 1, *features[i % count]) for i in range(missing_records_count)]
                filled_rows.extend(album_data)

        filled_df = pd.DataFrame(filled_rows, columns=df.columns)
        df = pd.concat([df, filled_df], ignore_index=True)

        return df

    def _save(self, df: pd.DataFrame):
        df.sort_values(by=[c.ALBUM_ID, c.SONG_NUMBER]).to_csv(self.output_path, index=False)


if __name__ == "__main__":
    START_YEAR = 1965
    END_YEAR = 2022

    feature_selector = SpotifyFeatureSelection(
        spotify_features_processed_path=f'{PROJECT_DIR}/data/processed/spotify/spotify_tracks_feature_{START_YEAR}_{END_YEAR}.csv',
        spotify_output_path=f'{PROJECT_DIR}/data/feature/spotify_{START_YEAR}_{END_YEAR}.csv'
    )

    feature_selector.run_and_save()
