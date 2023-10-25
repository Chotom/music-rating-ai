import datetime
import pandas as pd
import ast

import shared_utils.columns as c

from shared_utils.columns import RYM_COLS
from shared_utils.utils import PROJECT_DIR
from data_processing.preprocessing.genre_mapper import GenreMapper


class RymDataProcessor:
    """A class to process data fetched from RYM charts.

    Attributes:
        MINIMUM_RATE_NUMBER: Minimum number of ratings required to save record.
        _input_df: Read dataframe from input_path with fetched data.
        _output_path: Filepath of the output CSV file to save the processed data to.
    """

    MINIMUM_RATE_NUMBER = 50

    def __init__(self, input_path: str, output_path: str):
        """
        Args:
            input_path: Filepath of the input CSV file containing the fetched rym data.
            output_path: Filepath of the output CSV file to save the processed data to.
        """

        self._input_df = pd.read_csv(input_path)
        self._input_df.dropna(inplace=True)
        self._output_path = output_path

        assert (self._input_df.columns.values == RYM_COLS).all(), 'Invalid input data structure.'

    def process(self) -> None:
        """
        Process read data from input_path and stores in CSV at output_path.
        1. Convert the date column to YYYY-mm-dd format.
        2. Drop duplicates and keep the last entry for each artist and album.
        3. Convert the ratings_number column to integer.
        4. Sort the dataframe by date.
        5. Save the processed dataframe to self.output_path.
        """
        genre_mapper = GenreMapper(f'{PROJECT_DIR}/data/all_genre_map.json')

        df: pd.DataFrame = self._date_convert(self._input_df.copy())
        df.drop_duplicates(subset=[c.ARTIST, c.ALBUM], keep='last', inplace=True)
        df[c.RATING_NUMBER] = df[c.RATING_NUMBER].str.replace(',', '').astype(int)
        df = df.loc[df[c.RATING_NUMBER] >= self.MINIMUM_RATE_NUMBER, :]
        df.sort_values(by=[c.DATE], inplace=True)
        df[c.GENRES] = df[c.GENRES].apply(
            lambda genres: ','.join(
                genre_mapper.map_many_genres(
                    genres.split(", ")
                )
            )
        )

        df.to_csv(self._output_path, index=False)

    def _date_convert(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert c.DATE column to YYYY-mm-dd format with YYYY-01-01 as default if day
        or month data is missing (e.g. from 'September 1990' to '1990-09-01').

        Args:
            df: DataFrame with literal date in c.DATE columns to convert.

        Returns:
            DataFrame with converted c.DATE in YYYY-mm-dd format.
        """

        # Split the date column into year, month, and day columns.
        df[['year', 'month', 'day']] = self._input_df[c.DATE].apply(
            lambda date_in_text: pd.Series(date_in_text.split(' ', 2)[::-1])
        )

        # Fill missing day and month data with default values.
        df['day'] = df['day'].fillna(1).astype(str)
        df['month'] = (
            df['month']
            .fillna('january')
            .replace('', 'january')
            .apply(lambda x: datetime.datetime.strptime(x, '%B').strftime('%m'))
        )

        # Save date in YYYY-mm-dd format
        df[c.DATE] = df['year'] + '-' + df['month'] + '-' + df['day']
        df[c.DATE] = pd.to_datetime(df[c.DATE], format='%Y-%m-%d')

        return df.drop(columns=['year', 'month', 'day'])
