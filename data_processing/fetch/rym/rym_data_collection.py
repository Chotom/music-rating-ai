import os
import pandas as pd

from rymscraper.rymscraper import RymNetwork, RymUrl

from shared_utils.utils import RYM_COLS
from shared_utils.utils import create_logger


class RymFetcher:
    """
    Class for fetching and saving album chart data from rateyourmusic.com.

    Attributes:
        MAX_PAGE: Maximum number of pages in the rym chart.
        COLS: Column names for data fetched from rym (must be the same as in the rymscraper package).
        URL: URL of the rym chart to fetch data from.
        _logger: Logger object for logging messages.
        _network: RymNetwork object for making requests to rateyourmusic.com.
        _filepath: Filepath of the CSV file to save the fetched data to.
    """

    MAX_PAGE = 25
    COLS = ['Artist', 'Album', 'Date', 'RYM Rating', 'Ratings', 'Genres']
    URL = 'https://rateyourmusic.com/charts/popular'

    def __init__(self, filepath: str):
        """
        Args:
            filepath: Filepath of the CSV file to save the fetched data to.
        """

        self._logger = create_logger('RymFetcher')
        self._network = RymNetwork()
        self._filepath = filepath

    def fetch(self, start_year: int, end_year: int):
        """
        Fetch album chart data from rym for the given range of years and stores it in file.

        Args:
            start_year: Starting year of the range.
            end_year: Ending year of the range.
        """

        for year in range(start_year, end_year + 1):
            self._download_chart_data_from_rym(year)

    def _download_chart_data_from_rym(self, year: int):
        """
        Download and append albums chart data from given year to file at self._filepath.

        Args:
            year: Year of chart to download.
        """
        rym_url = RymUrl.RymUrl()
        rym_url.url_base = self.URL
        rym_url.year = year
        rym_url.language = 'en'

        data = pd.DataFrame(self._get_chart_data(rym_url))[self.COLS]

        is_file_new = RYM_COLS if not os.path.exists(self._filepath) else False
        data.to_csv(self._filepath, mode='a', header=is_file_new, index=False)
        self._logger.info(f'{year} saved to csv.')

    def _get_chart_data(self, rym_url: RymUrl) -> list[dict]:
        """
        Get albums info from RYM website.

        Args:
            rym_url: RymUrl object containing the URL of the chart to fetch data from.

        Returns:
            A list of dictionaries containing album information.
        """
        try:
            self._logger.info(f'Fetching album chart data from: {rym_url}')
            chart_data = self._network.get_chart_infos(url=rym_url, max_page=self.MAX_PAGE)
        except Exception as e:
            self._logger.info(f'Error while getting chart data: {e}. Retrying...')
            self._network = RymNetwork()
            chart_data = self._network.get_chart_infos(url=rym_url, max_page=self.MAX_PAGE)
        return chart_data
