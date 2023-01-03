import os
import pandas as pd

from rymscraper.rymscraper import RymNetwork, RymUrl

from shared_utils.utils import PROJECT_DIR, RYM_COLS
from shared_utils.utils import create_logger


class RymFetcher:
    MAX_PAGE = 25
    """Maximum number of pages in rym chart"""

    COLS = ['Artist', 'Album', 'Date', 'RYM Rating', 'Ratings', 'Genres']
    """Column names for data fetched from rym (must same as in rymscraper package)."""

    URL = 'https://rateyourmusic.com/charts/popular'
    """Url of rateyourmusic chart to fetch data from."""

    def __init__(self, filepath: str):
        self.logger = create_logger('RymFetcher')
        self.network = RymNetwork()
        self.filepath = filepath

    def fetch(self, start_year: int, end_year: int, ):
        """Download and append albums chart data from every year to file at RYM_FILE_PATH"""
        for year in range(start_year, end_year):
            self._download_chart_data_from_rym(year)

    def _download_chart_data_from_rym(self, year: int):
        """
        Download and append albums chart data from given year to file at RYM_FILE_PATH

        :param year: year of chart to download
        """
        rym_url = RymUrl.RymUrl()
        rym_url.url_base = self.URL
        rym_url.year = year
        rym_url.language = 'en'

        data = pd.DataFrame(self._get_chart_data(rym_url))[self.COLS]

        is_file_new = RYM_COLS if not os.path.exists(self.filepath) else False
        data.to_csv(self.filepath, mode='a', header=is_file_new, index=False)
        self.logger.info(f'{year} saved to csv.')

    def _get_chart_data(self, rym_url: RymUrl) -> list[dict]:
        """Get albums info from RYM website"""
        try:
            self.logger.info(f'Get: {rym_url}')
            chart_info = self.network.get_chart_infos(url=rym_url, max_page=self.MAX_PAGE)
        except Exception as e:
            self.logger.info(f'Error while getting chart data: {e}.\n',
                             f'Trying again...\n',
                             f'Get: {rym_url}')
            self.network = RymNetwork()
            chart_info = self.network.get_chart_infos(url=rym_url, max_page=self.MAX_PAGE)
        return chart_info


if __name__ == '__main__':
    START_YEAR = 1980
    END_YEAR = 1990
    path = f'{PROJECT_DIR}/data/raw/rym/rym_charts.csv'
    RymFetcher(path).fetch(START_YEAR, END_YEAR)
