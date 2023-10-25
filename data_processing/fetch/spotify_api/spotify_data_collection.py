import base64
import time
import requests as requests
from abc import ABC, abstractmethod

from shared_utils.utils import create_logger


class SpotifyFetcher(ABC):
    """
    Abstract representation of class for fetching data from spotify service.
    Provides token and logger.
    """

    def __init__(self, client_id: str, client_secret: str):
        self._logger = create_logger('SpotifyFetcher')
        self._set_spotify_token(client_id, client_secret)

    def _set_spotify_token(self, client_id: str, client_secret: str):
        """
        Authorize client by post request in spotify api and set _token field in class.

        :param client_id: Spotify client id - required in authorization.
        :param client_secret: Spotify api secret - required in authorization.
        """

        client_b64 = base64.urlsafe_b64encode(f'{client_id}:{client_secret}'.encode()).decode()
        r = requests.post('https://accounts.spotify.com/api/token',
                          data={'grant_type': 'client_credentials'},
                          headers={'Authorization': f'Basic {client_b64}'})

        if r.status_code != 200:
            raise requests.RequestException(f'Status code not success: {r.json()}')
        self._token = r.json()['access_token']

    @abstractmethod
    def fetch(self):
        """Fetch data from spotify to output file."""
        raise NotImplementedError

    def _raise_too_many_request_error(self, retry_after: str):
        """
        Handle 'too many requests' error when fetching data from Spotify API.

        Args:
            retry_after: Value of the 'Retry-After' header in the response.

        Raises:
            requests.ConnectionError: If 'too many requests' error occurs.
        """

        try:
            # Convert number of seconds to a time string in HH:MM:SS format.
            retry_after_date = str(time.strftime('%H:%M:%S', time.gmtime(int(retry_after))))
        except ValueError:
            # If the 'Retry-After' header is not a number, use the date string as it is.
            retry_after_date = retry_after

        err_msg = f'Error during fetching spotify data: too many requests - try after: {retry_after_date}'
        self._logger.error(err_msg)
        raise requests.ConnectionError(err_msg)
