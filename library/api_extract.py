import os
import requests
class NewsApiExtract():

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
    
    def get_header(self, api_key : str) -> dict[str, str]:
        """
        Get the header required for the API request.
        """
        return {'X-Api-Key' : api_key}
    
    def get_data(self, endpoint : str = 'top-headlines', country : str = 'us') -> dict[str, str]:
        """
        Retrieve top headlines from News API
        by the 'articles' key as a JSON object.
        """
        url_base = self.get_url_base()
        url = f"{url_base}{endpoint}?country={country}"
        api_key = self.get_api_key()
        headers = self.get_header(api_key)
        response = requests.get(url, headers=headers)
        if response.json()["status"] == "error":
            raise requests.exceptions.HTTPError(response["message"])
        articles = response.json()['articles']
        return articles