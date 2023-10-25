import pandas as pd

from shared_utils import columns as c
from shared_utils.utils import PROJECT_DIR


class RymFeatureSelection:
    """
       This class contains methods to merge processed data to various variants
       of final dataset.
       """

    def __init__(
            self,
            rym_processed_path: str,
            spotify_search_processed_path: str,
            rym_rating_output_path: str,
    ):
        """
        Args:
            rym_processed_path: Input file for RYM features.
            spotify_search_processed_path: Input file to match album_ids
            rym_rating_output_path: Output path.
        """

        self.output_path = rym_rating_output_path
        self._df_features = pd.merge(
            pd.read_csv(rym_processed_path),
            pd.read_csv(spotify_search_processed_path),
            on=[c.ARTIST, c.ALBUM],
            how='inner'
        )

    def run_and_save(self):
        df = self._df_features.copy()
        df = self._select_features(df)
        df = self._transform_features(df)

        self._save(df)

    @staticmethod
    def _select_features(df: pd.DataFrame) -> pd.DataFrame:
        """Returns: Dataframe with features, ALBUM_ID AND RATING columns."""

        return df[[c.ALBUM_ID] + c.RYM_CORE_FEATURES + [c.RATING]]

    def _transform_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform every feature if is in the selected columns."""

        if c.DATE in df.columns:
            self._transform_date(df)
        if c.GENRES in df.columns:
            self._transform_genres(df)

        self._transform_target(df)

        return df

    @staticmethod
    def _transform_date(df):
        df[c.DATE] = pd.to_datetime(df[c.DATE])
        df[c.DECADE_CLASS] = (df[c.DATE].dt.year - 1959) // 10 / 6

        df.drop(c.DATE, inplace=True, axis=1)

    @staticmethod
    def _transform_genres(df):
        genres = df[c.GENRES].str.get_dummies(sep=',')
        df[genres.columns] = genres

        df.drop(c.GENRES, inplace=True, axis=1)

    @staticmethod
    def _transform_target(df):
        # Based on notebook 01_analysis_rym.
        range0 = 2.77
        range1 = 3.17
        range2 = 3.35
        range3 = 3.52
        range4 = 3.73

        df.loc[df[c.RATING].between(0., range0), c.RATING_CLASS] = 0
        df.loc[df[c.RATING].between(range0, range1), c.RATING_CLASS] = 1
        df.loc[df[c.RATING].between(range1, range2), c.RATING_CLASS] = 2
        df.loc[df[c.RATING].between(range2, range3), c.RATING_CLASS] = 3
        df.loc[df[c.RATING].between(range3, range4), c.RATING_CLASS] = 4
        df.loc[df[c.RATING].between(range4, 5.0), c.RATING_CLASS] = 5
        df[c.RATING] = df[c.RATING_CLASS]

        df.drop(c.RATING_CLASS, inplace=True, axis=1)

    def _save(self, df: pd.DataFrame):
        df.to_csv(self.output_path, index=False)


if __name__ == "__main__":
    START_YEAR = 1965
    END_YEAR = 2022

    feature_selector = RymFeatureSelection(
        rym_processed_path=f'{PROJECT_DIR}/data/processed/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv',
        spotify_search_processed_path=f'{PROJECT_DIR}/data/processed/spotify/spotify_search_album_id_{START_YEAR}_{END_YEAR}.csv',
        rym_rating_output_path=f'{PROJECT_DIR}/data/feature/rym_{START_YEAR}_{END_YEAR}.csv'
    )

    feature_selector.run_and_save()
