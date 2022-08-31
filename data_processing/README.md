# Data processing

## Setup

Run `data_processing/fetch/rym/rymscraper/setup.py` to prepare for rym data fetching.
This will clone repository (rymscraper), which is required to fetch music rating data.
Remember to install rymscraper pip packages and prepare selenium webdrivers.

## Data pipeline

0. Setup dependencies and requirements.

---

1. Fetch album rating data from rateyourmusic.com.
    1. Set years to fetch in `data_processing/fetch/rym/rym_data_collection.py` and run it.
   ```python
   # Example
   START_YEAR = 1980
   END_YEAR = 1990
   path = f'{PROJECT_DIR}/data/raw/rym/rym_charts.csv'
   RymFetcher(path).fetch(START_YEAR, END_YEAR)
   ```
   Fetching this many years may take some time. Also, an excessive usage of
   rymscraper can make your IP address banned by rateyourmusic for a few days,
   so don't download too much data at once.

---

2. Clean RYM data (remove non-ascii characters, normalise date etc.).
3. Search for an album in spotify API and save album ID.
4. Search for album and artist on Genius API and save album ID.
5. Fetch Spotify audio features data to directory by ID.
6. Fetch song lyrics from Genius.