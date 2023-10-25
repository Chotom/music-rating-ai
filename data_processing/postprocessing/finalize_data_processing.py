import ast
import os
import pandas as pd
import json

from shared_utils import columns as c
from shared_utils.utils import PROJECT_DIR


class FinalizeDataProcessor:
    """
    This class contains methods to merge processed data to various variants 
    of final dataset.
    """

    def __init__(
            self,
            rym_rating_path: str,
            spotify_features: str,
            output_dir: str
    ):
        self.df_rym_ratings = pd.read_csv(rym_rating_path)
        self.df_spotify_features = pd.read_csv(spotify_features)

        self._spotify_cols = self.df_spotify_features.columns.drop([c.ALBUM_ID, c.SONG_NUMBER]).tolist()
        self._rym_cols = self.df_rym_ratings.columns.drop([c.ALBUM_ID, c.RATING]).tolist()

        self.output_dir = output_dir
        for subdir in ['flatten', 'agg_flatten', 'single']:
            subdir_path = os.path.join(output_dir, subdir)
            if not os.path.exists(subdir_path):
                os.makedirs(subdir_path)

    def finalize_to_flatten(self):
        """
        ...
        """

        # Flatten spotify
        grouped_spotify = self.df_spotify_features.groupby(c.ALBUM_ID).apply(
            lambda row: row[self._spotify_cols].values.flatten().tolist()
        ).reset_index()
        grouped_spotify['features_spotify'] = grouped_spotify[0]
        print('flatten: spotify grouped.')

        # Flatten rym
        grouped_rym = self.df_rym_ratings.groupby(c.ALBUM_ID).apply(
            lambda row: row[self._rym_cols].values.flatten().tolist()
        ).reset_index()
        grouped_rym['features_rym'] = grouped_rym[0]
        print('flatten: rym grouped.')

        # Finalize
        flatten_df = (
            self.df_rym_ratings[[c.ALBUM_ID, c.RATING]]
            .merge(grouped_rym, on=c.ALBUM_ID)
            .merge(grouped_spotify, on=c.ALBUM_ID))
        flatten_df[c.FEATURE] = flatten_df['features_spotify'] + flatten_df['features_rym']

        spotify_cols = [f'{name}{i // len(self._spotify_cols)}' for i, name in enumerate(self._spotify_cols * 16)]
        feature_names = spotify_cols + self._rym_cols

        self._save(flatten_df[[c.ALBUM_ID, c.FEATURE, c.RATING]], feature_names, 'flatten')

    def finalize_to_aggregated(self):
        df = self.df_spotify_features
        df_agg_mean = df.groupby(c.ALBUM_ID).mean()
        df_agg_min = df.groupby(c.ALBUM_ID).min()
        df_agg_max = df.groupby(c.ALBUM_ID).max()
        df_agg_std = df.groupby(c.ALBUM_ID).std()

        df1 = df_agg_mean
        df2 = df1.merge(df_agg_min, suffixes=['_mean', '_min'], on=c.ALBUM_ID)
        df3 = df2.merge(df_agg_max, suffixes=['', '_max'], on=c.ALBUM_ID)
        df4 = df3.merge(df_agg_std, suffixes=['_max', '_std'], on=c.ALBUM_ID).reset_index()
        spotify_cols = df4.columns
        spotify_cols.drop([c.ALBUM_ID])
        df4['features_spotify'] = df4.apply(lambda row: [row[col] for col in df4.columns if col != c.ALBUM_ID], axis=1)


        # Flatten rym
        grouped_rym = self.df_rym_ratings.groupby(c.ALBUM_ID).apply(
            lambda row: row[self._rym_cols].values.flatten().tolist()
        ).reset_index()
        grouped_rym['features_rym'] = grouped_rym[0]
        print('flatten: rym grouped.')

        # Finalize
        flatten_df = (
            self.df_rym_ratings[[c.ALBUM_ID, c.RATING]]
            .merge(grouped_rym, on=c.ALBUM_ID)
            .merge(df4, on=c.ALBUM_ID))
        flatten_df[c.FEATURE] = flatten_df['features_spotify'] + flatten_df['features_rym']
        # spotify_cols = flatten_df.columns

        feature_names = list(spotify_cols) + list(self._rym_cols)

        self._save(flatten_df[[c.ALBUM_ID, c.FEATURE, c.RATING]], feature_names, 'agg_flatten')

    def finalize_to_single(self):
        """
        ...
        """

        # Flatten spotify
        grouped_spotify = self.df_spotify_features[self._spotify_cols].apply(
            lambda row: row.tolist(), axis=1
        ).reset_index()
        grouped_spotify['features_spotify'] = grouped_spotify[0]
        grouped_spotify[c.ALBUM_ID] = self.df_spotify_features[c.ALBUM_ID]
        print('flatten: spotify grouped.')

        # Flatten rym
        grouped_rym = self.df_rym_ratings[self._rym_cols].apply(
            lambda row: row.tolist(), axis=1
        ).reset_index()
        grouped_rym['features_rym'] = grouped_rym[0]
        grouped_rym[c.ALBUM_ID] = self.df_rym_ratings[c.ALBUM_ID]
        print('flatten: rym grouped.')

        # Finalize
        flatten_df = (
            self.df_rym_ratings[[c.ALBUM_ID, c.RATING]]
            .merge(grouped_rym, on=c.ALBUM_ID)
            .merge(grouped_spotify, on=c.ALBUM_ID, how='right')
        ).dropna()
        flatten_df[c.FEATURE] = flatten_df['features_spotify'] + flatten_df['features_rym']
        spotify_cols = [f'{name}{i // len(self._spotify_cols)}' for i, name in enumerate(self._spotify_cols * 1)]
        feature_names = spotify_cols + self._rym_cols

        self._save(flatten_df[[c.ALBUM_ID, c.FEATURE, c.RATING]], feature_names, 'single')

    def _save(self, df: pd.DataFrame, feature_names: list[str], subdir: str):
        path = os.path.join(self.output_dir, subdir)

        df.to_csv(path + '/album_rating.csv', index=False)
        df.to_parquet(path + '/album_rating.parquet', index=False)

        with open(path + '/album_rating_feature_names.json', 'w') as json_file:
            json.dump(feature_names, json_file)


    # def merge_to_features_in_list(self, path: str):
    #     """Aggregate feature to list to one column called 'feature'."""
    #
    #     df_merged_features = self.df_rym_ratings.merge(self.df_spotify_features, on='album_id', how='inner')
    #
    #     # Cast features in string to the list
    #     df_merged_features['feature'] = df_merged_features['feature'].apply(lambda x: ast.literal_eval(x))
    #
    #     # Save to csv
    #     df_merged_features[['feature', 'rating']].to_csv(path + '.csv', index=False)
    #     df_merged_features[['feature', 'rating']].to_parquet(path + '.parquet', index=False)


if __name__ == "__main__":
    START_YEAR = 1965
    END_YEAR = 2022

    finalizer = FinalizeDataProcessor(
        rym_rating_path=f'{PROJECT_DIR}/data/feature/rym_{START_YEAR}_{END_YEAR}.csv',
        spotify_features=f'{PROJECT_DIR}/data/feature/spotify_{START_YEAR}_{END_YEAR}.csv',
        output_dir=f'{PROJECT_DIR}/data/final/'
    )

    finalizer.finalize_to_aggregated()
