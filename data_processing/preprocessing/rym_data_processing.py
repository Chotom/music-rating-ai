import datetime
import re
import pandas as pd
from unidecode import unidecode

from shared_utils.utils import PROJECT_DIR


class RymDataProcessor:
    data_header = ['artist', 'album', 'date', 'rating', 'ratings_number', 'genres']
    date_col = 'date'

    def __init__(self, input_path: str, output_path: str):
        self.df = pd.read_csv(input_path, names=self.data_header)
        self.output_path = output_path

    def process(self):
        self._remove_duplicates()
        self._date_convert()
        self._clear_names()
        self.df.sort_values(by=[self.date_col]).to_csv(output_path, index=False)

    def _remove_duplicates(self):
        """Remove duplicated albums from DataFrame."""
        self.df.drop_duplicates(subset=['artist', 'album'], keep='last', inplace=True)

    def _clear_names(self):
        """
        Clean names in Dataframe. Extract names from brackets and check is name in ascii.
        """
        self.df['clean_artist'] = self.df['artist'].str.strip()
        self.df['clean_artist'] = self.df['clean_artist'].apply(lambda x: self._get_str_from_brackets(x))
        self.df['clean_artist'] = self.df['clean_artist'].apply(lambda x: unidecode(x))
        self.df['is_artist_ascii'] = self.df['artist'].apply(lambda x: x.isascii())

        # self.df['clean_album'] = self.df['album'].apply(lambda x: self._get_str_from_brackets(x))
        self.df['clean_album'] = self.df['album'].str.strip()
        self.df['clean_album'] = self.df['clean_album'].apply(lambda x: unidecode(x))
        self.df['is_album_ascii'] = self.df['album'].apply(lambda x: x.isascii())

    def _get_str_from_brackets(self, name: str) -> str:
        """
        Extract string from brackets ('[', '〈') from given name.
        Each part of name separated with '/' will be extracted separately and joined in result.

        Examples:
        ---------
        >>> self._get_str_from_brackets('[name 1] / 〈name_2〉 / name3')
        'name 1 / name_2 / name3'

        >>> self._get_str_from_brackets('some name [name 1]')
        'name 1'

        >>> self._get_str_from_brackets('just name')
        'just name'

        :param name: names join with '/' to extract
        :return: extracted names join with '/'
        """

        # Init
        brackets = ['[', '〈']
        regex = r'[\[〈](.+)[〉\]]'
        if not any(x in name for x in brackets):
            return name

        # Split various names to list
        names = name.split(' / ')

        # Extract names between brackets to list
        results: list[str] = []
        for n in names:
            if any(x in n for x in brackets):
                print(n)
                results.append(re.search(regex, n).group(1).strip())
            else:
                results.append(n.strip())
        return ' / '.join(results)

    def _date_convert(self):
        """
        Convert DATE_COL column to YYYY-mm-dd format with YYYY-01-01 as default if day
        or month data is missing (e.g. from 'September 1990' to '1990-09-01').
        """

        # Prepare columns to fill missing data
        df_convert = self.df.copy()
        df_convert[['year', 'month', 'day']] = self.df[self.date_col].apply(
            lambda x: pd.Series(x.split(' ', 2)[::-1]))
        df_convert['day'] = df_convert['day'].fillna(1).astype(str)

        # Cast month to numeric value
        df_convert['month'] = (df_convert['month']
                               .fillna('january')
                               .replace('', 'january')
                               .apply(lambda x: datetime.datetime.strptime(x, '%B').strftime('%m')))

        # Save date in YYYY-mm-dd format
        df_convert[self.date_col] = df_convert['year'] + '-' + df_convert['month'] + '-' + df_convert['day']
        df_convert[self.date_col] = pd.to_datetime(df_convert[self.date_col], format='%Y-%m-%d')
        self.df = df_convert.drop(columns=['year', 'month', 'day'])


if __name__ == '__main__':
    path = f'{PROJECT_DIR}/data/raw/rym/rym_charts.csv'
    output_path = f'{PROJECT_DIR}/data/processed/rym_charts.csv'
    RymDataProcessor(path, output_path).process()
