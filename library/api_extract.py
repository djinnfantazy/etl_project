import os
import requests
from typing import Any

class NewsApiExtract:

    def get_api_key(self) -> str:
        """
        Retrieve the News API key 
        from the environment variable.
        """
        api_key = os.environ.get("NEWS_API_KEY")
        if not api_key:
            raise ValueError(f"The 'NEWS_API_KEY' environment variable is missing, make sure it is set and try again.")
        return api_key
    
    def get_url_base(self) -> str:
        """
        Get the url base.
        """

        return "https://newsapi.org/v2/"
    
    def get_header(self) -> dict[str, str]:
        """Get the header required for the API request."""

        return {"X-Api-Key" : self.get_api_key()}
    
    def get_data(self, endpoint: str) -> Any:
        """Retrieve endpoint data from News API"""
        url = f"{self.get_url_base()}{endpoint}?country=us&apiKey={self.get_api_key()}"
        response = requests.get(url).json()
        if response["status"] == "error":
            raise requests.exceptions.HTTPError(response["message"])

        return response