from urllib.parse import urljoin


def get_full_link(domain: str, url: str) -> str:
    return urljoin(domain, url)
