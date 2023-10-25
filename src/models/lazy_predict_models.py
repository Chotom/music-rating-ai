import json
import numpy as np
import pandas as pd

from lazypredict.Supervised import LazyClassifier
from sklearn.model_selection import train_test_split

import shared_utils.columns as c
from data_processing.feature.rym_feature_selection import RymFeatureSelection
from data_processing.feature.spotify_feature_selection import SpotifyFeatureSelection
from data_processing.postprocessing.finalize_data_processing import FinalizeDataProcessor
from shared_utils.utils import PROJECT_DIR

# Prepare data
START_YEAR = 1965
END_YEAR = 2022
PROCESS_DATA = True

if PROCESS_DATA:
    feature_selector = SpotifyFeatureSelection(
        spotify_features_processed_path=f'{PROJECT_DIR}/data/processed/spotify/spotify_tracks_feature_{START_YEAR}_{END_YEAR}.csv',
        spotify_output_path=f'{PROJECT_DIR}/data/feature/spotify_{START_YEAR}_{END_YEAR}.csv'
    )

    feature_selector.run_and_save()

    feature_selector = RymFeatureSelection(
        rym_processed_path=f'{PROJECT_DIR}/data/processed/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv',
        spotify_search_processed_path=f'{PROJECT_DIR}/data/processed/spotify/spotify_search_album_id_{START_YEAR}_{END_YEAR}.csv',
        rym_rating_output_path=f'{PROJECT_DIR}/data/feature/rym_{START_YEAR}_{END_YEAR}.csv'
    )

    feature_selector.run_and_save()

    finalizer = FinalizeDataProcessor(
        rym_rating_path=f'{PROJECT_DIR}/data/feature/rym_{START_YEAR}_{END_YEAR}.csv',
        spotify_features=f'{PROJECT_DIR}/data/feature/spotify_{START_YEAR}_{END_YEAR}.csv',
        output_dir=f'{PROJECT_DIR}/data/final/'
    )

    finalizer.finalize_to_flatten()

# Generate synthetic dataset
np.random.seed(42)

df = pd.read_parquet(f'{PROJECT_DIR}/data/final/flatten/album_rating.parquet')
with open(f'{PROJECT_DIR}/data/final/flatten/album_rating_feature_names.json', 'r') as json_file:
    feature_names = json.load(json_file)

# Extract the 'feature' column into a NumPy matrix
X = np.vstack(df[c.FEATURE].values)
assert type(X[0]) == np.ndarray

# Extract the 'rating' column into a NumPy array
y = df[c.RATING].values
num_classes = len(df[c.RATING].unique())

# Split dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f'train size: {X_train.shape}, test size: {X_test.shape}')

clf = LazyClassifier(verbose=0, ignore_warnings=True, custom_metric=None)
models, predictions = clf.fit(X_train, X_test, y_train, y_test)

print(models)
