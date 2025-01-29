import os
import requests
from abc import ABC, abstractmethod
class Extract(ABC):
    def __init__(self, api_base : str, key_name : str):
        self.api_base = api_base
        self.key_name = key_name

    def get_api_key(self) -> str:
        api_key = os.environ.get(self.key_name)
        if not api_key:
            raise ValueError(f"The API key {self.key_name}is missing, please set it as an environment variable.")
        return api_key
    
    def get_url_base(self) -> str:
        """Get the url base"""

        return self.api_base
    
    def get_header(self, api_key : str) -> dict[str, str]:
        return {'X-Api-Key' : api_key}
    
    def make_request(self, endpoint : str, country : str | None) -> dict:
        if type(country) is str:
            url = f"{self.api_base}{endpoint}?country={country}"
        else:
            url = f'{self.api_base}{endpoint}'
        api_key = self.get_api_key()
        headers = self.get_header(api_key)
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise requests.exceptions.HTTPError('API request failed.')
        return response.json()

    @abstractmethod
    def extract(self, **kwargs) -> dict:
        pass

class NewsApiExtract(Extract):

    def __init__(self):
        super().__init__(api_base='https://newsapi.org/v2/', key_name='NEWS_API_KEY')

    def extract(self, endpoint = 'top-headlines', country = 'us') -> dict:
        return self.make_request(endpoint, country)