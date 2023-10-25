import json
import numpy as np
import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix
from torch.utils.data import DataLoader, TensorDataset

import shared_utils.columns as c
from data_processing.feature.rym_feature_selection import RymFeatureSelection
from data_processing.feature.spotify_feature_selection import SpotifyFeatureSelection
from data_processing.postprocessing.finalize_data_processing import FinalizeDataProcessor
from shared_utils.utils import PROJECT_DIR

# Prepare data
START_YEAR = 1965
END_YEAR = 2022
PROCESS_DATA = False
data_type = "flatten"

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
print(device)

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
torch.manual_seed(42)


df = pd.read_parquet(f'{PROJECT_DIR}/data/final/{data_type}/album_rating.parquet')
with open(f'{PROJECT_DIR}/data/final/{data_type}/album_rating_feature_names.json', 'r') as json_file:
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


# Define a custom neural network model
class FeedForwardNN(nn.Module):
    def __init__(self, input_size, hidden_sizes, num_classes):
        super(FeedForwardNN, self).__init__()
        self.input_size = input_size
        self.hidden_sizes = hidden_sizes

        # Create hidden layers
        _input_size = input_size
        self.hidden_layers = nn.ModuleList()
        for hidden_size in hidden_sizes:
            self.hidden_layers.append(nn.Linear(_input_size, hidden_size))
            # self.hidden_layers.append(nn.LeakyReLU())
            # self.hidden_layers.append(nn.Dropout(p=0.1))
            _input_size = hidden_size

        # Output layer
        self.output_layer = nn.Linear(_input_size, num_classes)

    def forward(self, x):
        for layer in self.hidden_layers:
            x = layer(x)
        x = self.output_layer(x)
        return x


# Define hyperparameters
input_size = X_train.shape[1]
hidden_sizes = [256, 128, 64, 32]
learning_rate = 0.001
num_epochs = 100
batch_size = 8196  # Adjust this as needed

# Create model, loss function, and optimizer, and move them to the device
model = FeedForwardNN(input_size, hidden_sizes, num_classes)
model = model.to(device)
model = model.to(dtype=torch.float32)

criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr=learning_rate)

# Convert NumPy arrays to PyTorch tensors
X_train_tensor = torch.Tensor(X_train)
y_train_tensor = torch.LongTensor(y_train)
# Move the training and testing data to the device

X_train_tensor = X_train_tensor.to(device)
y_train_tensor = y_train_tensor.to(device)

# Create DataLoader for batch training
train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

# Training loop
for epoch in range(num_epochs):
    for batch_X, batch_y in train_loader:
        batch_X = batch_X.to(device)
        batch_y = batch_y.to(device)

        # Forward pass
        outputs = model(batch_X)
        loss = criterion(outputs, batch_y)

        # Backpropagation and optimization
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

    # Print loss at each epoch (optional)
    print(f'Epoch [{epoch + 1}/{num_epochs}], Loss: {loss.item()}')

# Testing the model
X_test_tensor = torch.Tensor(X_test)
X_test_tensor = X_test_tensor.to(device)
with torch.no_grad():
    outputs = model(X_test_tensor)
    _, y_pred = torch.max(outputs, 1)

# Move y_pred to CPU to convert it to a NumPy array
y_pred = y_pred.cpu().numpy()

# Move y_test to CPU to convert it to a NumPy array
# y_test = y_test.cpu().numpy()

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)

# Calculate the confusion matrix
conf_matrix = confusion_matrix(y_test, y_pred)

# Plot the confusion matrix using seaborn
plt.figure(figsize=(8, 6))
labels = [i for i in range(num_classes)]
sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues', xticklabels=labels, yticklabels=labels)
plt.xlabel('Predicted')
plt.ylabel('True')
plt.title('Confusion Matrix')
# plt.savefig(r"D:\repo\music-rating-ai\docs\master_thesis\ff_best_flatten_repeated.svg")

plt.show()

# # Gain
# bst_dict = bst.get_score(importance_type='gain')
# my_dict = {}
# for i, (k, v) in enumerate(bst_dict.items()):
#     my_dict[feature_names[i]] = v
# sorted_dict = dict(sorted(my_dict.items(), key=lambda item: item[1]))
# print(sorted_dict)
