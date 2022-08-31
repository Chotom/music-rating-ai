import logging
import os
import sys

# Consts

PROJECT_DIR = f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}'
"""Absolute path to project directory"""

LOG_LEVEL = logging.DEBUG
"""Message logging level"""


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
