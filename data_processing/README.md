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
    1. Set filepaths and run `data_processing/preprocessing/rym_data_processing.py`.
       ```python
       # Example
       path = f'{PROJECT_DIR}/data/raw/rym/rym_charts.csv'
       output_path = f'{PROJECT_DIR}/data/processed/rym_charts.csv'
       RymDataProcessor(path, output_path).process()
       ```

---

3. Search for an album in spotify API and save album ID.
    1. Go to https://developer.spotify.com/dashboard/applications and login to get API credentials.
    2. Set `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` envs in system.
    3. Run `data_processing/fetch/spotify_api/spotify_search_album_fetcher.py`. Based on fetched RYM data (in previous
       step), this class will search for each album in spotify to find its ID. This ID is required to fetch albums songs
       data.
       ```python
       # Example
       search_fetcher = SpotifySearchAlbumFetcher(
           os.getenv('SPOTIFY_CLIENT_ID'),
           os.getenv('SPOTIFY_CLIENT_SECRET'),
           f'{PROJECT_DIR}/data/processed/rym_charts.csv',
           f'{PROJECT_DIR}/data/raw/spotify/spotify.csv'
       )
       search_fetcher.fetch()
       ```
       *You may repeat this step a few times due to spotify token expiration.*

---

4. Fetch Spotify audio features data to directory by ID.
    1. Fetch Spotify tracks IDs for fetched albums in previous step.
---

5. Search for album and artist on Genius API and save album ID.
6. Fetch song lyrics from Genius.