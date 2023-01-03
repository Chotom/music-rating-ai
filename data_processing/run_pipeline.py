from shared_utils.utils import PROJECT_DIR
from data_processing.fetch.rym.rym_data_collection import RymFetcher

# Fetch album rating data from rateyourmusic.com
START_YEAR = 1980
END_YEAR = 1980
path = f'{PROJECT_DIR}/data/raw/rym/rym_charts_{START_YEAR}_{END_YEAR}.csv'
RymFetcher(path).fetch(START_YEAR, END_YEAR)
