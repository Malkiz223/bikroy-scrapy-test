from urllib.parse import urljoin, urlparse


def get_full_link(domain: str, url: str) -> str:
    return urljoin(domain, url)


def get_url_without_query(url: str) -> str:
    return urljoin(url, urlparse(url).path)
