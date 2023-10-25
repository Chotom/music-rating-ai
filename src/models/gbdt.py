import json
import random

import numpy as np
import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix

import shared_utils.columns as c
from data_processing.feature.rym_feature_selection import RymFeatureSelection
from data_processing.feature.spotify_feature_selection import SpotifyFeatureSelection
from data_processing.postprocessing.finalize_data_processing import FinalizeDataProcessor
from shared_utils.utils import PROJECT_DIR

# Prepare data
START_YEAR = 1965
END_YEAR = 2022
PROCESS_DATA = 0

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

df = pd.read_parquet(f'{PROJECT_DIR}/data/final/agg_flatten/album_rating.parquet')
with open(f'{PROJECT_DIR}/data/final/agg_flatten/album_rating_feature_names.json', 'r') as json_file:
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

# Below code extract and prepare names (now it doesnt work as the feature may differ)
# all_names = SPOTIFY_RAW_FEATURES*16
# feature_names = [all_names[i] + str(int(i/13)) for i in range(len(all_names))]

# Convert data to DMatrix format
dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)

# Define XGBoost parameters for multi-class classification
params = {
    'objective': 'multi:softmax',  # Use softmax for multi-class classification
    'num_class': num_classes,  # Number of classes
    'max_depth': 5,
    'learning_rate': 0.1,
    'eval_metric': 'merror'  # Multi-class log loss metric
}

# Train the XGBoost model
num_round = 200  # Number of boosting rounds
bst = xgb.train(params, dtrain, num_round)

# Make predictions on the test set
y_pred = bst.predict(dtest)

# Evaluate the model
y_pred = [random.randint(3, 4) for _ in y_test]
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)

# Calculate the confusion matrix
conf_matrix = confusion_matrix(y_test, y_pred)

# Plot the confusion matrix using seaborn
plt.figure(figsize=(8, 6))
labels = [i for i in range(num_classes)]
sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues', xticklabels=labels, yticklabels=labels)
plt.xlabel('Predykcja')
plt.ylabel('Wartość prawdziwa')
plt.title('Macierz pomyłek')
# plt.savefig(r"D:\repo\music-rating-ai\docs\master_thesis\xgb_best_flatten_agg.svg")
plt.show()


# Gain
bst_dict = bst.get_score(importance_type='gain')
my_dict = {}
for i, (k, v) in enumerate(bst_dict.items()):
    my_dict[feature_names[i]] = v
sorted_dict = dict(sorted(my_dict.items(), key=lambda item: item[1]))
print(sorted_dict)
