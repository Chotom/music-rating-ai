import base64
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
