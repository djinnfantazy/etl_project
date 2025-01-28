class NewsApiExtract(Extract):

    def get_api_key() -> str:
        ...

    def get_url_base() -> str:
        """Get the url base"""

        return "https://newsapi.org/v2"

    def get_header() -> dict[str, str]:
        ...

    def extract(table_name: str) -> dict:
        ...

