from urllib.parse import urljoin, urlparse


SPIDER_DATE_PARSED_CATEGORY_PATH = '{spider_name}_parsed_category_stats.json'


def get_url_without_query(url: str) -> str:
    return urljoin(url, urlparse(url).path)
