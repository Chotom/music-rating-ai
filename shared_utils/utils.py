import logging
import os
import re
import sys
from unidecode import unidecode

# Consts

PROJECT_DIR = f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}'
"""Absolute path to project directory."""

LOG_LEVEL = logging.DEBUG
"""Message logging level."""

# Spotify features consts
MIN_TEMPO = 45
MAX_TEMPO = 220
MAX_DURATION_MS = 600000
MAX_DANCEABILITY = 1
MAX_ENERGY = 1
MAX_KEY = 11
MIN_LOUDNESS = -35
VALID_MODES = [0, 1]
MAX_SPEECHINESS = 0.66
MAX_ACOUSTICNESS = 1
MAX_INSTRUMENTALNESS = 1
MAX_LIVENESS = 1
MAX_VALENCE = 1
MIN_DURATION_MS = 20000
MIN_TIME_SIGNATURE = 3
MEDIAN_TIME_SIGNATURE = 4
MAX_TIME_SIGNATURE = 7

# Functions


def create_logger(name: str) -> logging.Logger:
    """
    Create logger for given name
    :param name: name of logger
    :return: logger
    """

    log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
    formatter = logging.Formatter(log_format)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    if not len(logger.handlers):
        logger.addHandler(console_handler)
    return logger


def assert_columns(cols: list[str], reference_cols: list[str]):
    # Check that the length of the two lists is the same
    assert len(cols) == len(reference_cols), 'Number of columns does not match'

    # Check that the columns in the `cols` list are the same as the `reference_cols` list
    for col in cols:
        assert col in reference_cols, f'Column {col} does not match any columns from expected: {reference_cols}.'


def clear_album_name(name: str) -> str:
    name = _remove_additional_info_in_parenthesis(name)
    name = unidecode(name)
    name = name.lower()
    name = (
        name.replace(' ', '')
        .replace('&', '')
        .replace('the', '')
        .replace('and', '')
        .replace(',', '')
        .replace('-', '')
        .replace('~', '')
        .replace(';', '')
        .replace('"', '')
        .replace("'", '')
        .replace(">", '')
        .replace("<", '')
        .replace("`", '')
        .replace('!', '')
        .replace('(', '')
        .replace(')', '')
        .replace('[', '')
        .replace(']', '')
        .replace('{', '')
        .replace('}', '')
        .replace('.', '')
        .replace(':', '')
        .replace('?', '')
    )
    return name


def _remove_additional_info_in_parenthesis(album_name: str):
    album_name = re.sub(r"\([^()]*\)", "", album_name).strip()
    album_name = re.sub(r"\[[^()]*\]", "", album_name).strip()
    return album_name


def clear_artist_name(name: str) -> str:
    name = _get_str_from_brackets(name)
    name = unidecode(name)
    name = name.lower()
    name = (
        name.replace('the ', '')
        .replace('and ', '')
        .replace(' ', '')
        .replace('&', '')
        .replace(',', '')
        .replace('-', '')
        .replace('+', '')
        .replace('~', '')
        .replace(';', '')
        .replace('"', '')
        .replace("'", '')
        .replace(">", '')
        .replace("<", '')
        .replace("`", '')
        .replace('!', '')
        .replace('(', '')
        .replace(')', '')
        .replace('[', '')
        .replace(']', '')
        .replace('{', '')
        .replace('}', '')
        .replace('.', '')
        .replace(':', '')
        .replace('?', '')
    )
    return name


def _get_str_from_brackets(name: str) -> str:
    """
    Extract string from brackets ('[', '〈') from given name.
    Each part of name separated with '/' will be extracted separately and joined in result.

    Examples:
    ---------
    >>> _get_str_from_brackets('[name 1] / 〈name_2〉 / name3')
    'name 1 / name_2 / name3'

    >>> _get_str_from_brackets('some name [name 1]')
    'name 1'

    >>> _get_str_from_brackets('just name')
    'just name'

    :param name: names join with '/' to extract
    :return: extracted names join with '/'
    """

    brackets = ['[', '〈']
    regex = r'[\[〈](.+)[〉\]]'
    if not any(x in name for x in brackets):
        return name
    various_names = name.split(' / ')

    # Extract names between brackets to list
    results: list[str] = []
    for n in various_names:
        if any(x in n for x in brackets):
            results.append(re.search(regex, n).group(1).strip())
        else:
            results.append(n.strip())
    return ' / '.join(results)
