import json
import logging
from json import JSONDecodeError
from pathlib import Path
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

CACHE_CATEGORY_PATH = Path('./spiders/cache')
CATEGORY_STATS_FILENAME = '{spider_name}_parsed_category_stats.json'


def get_url_without_query(url: str) -> str:
    return urljoin(url, urlparse(url).path)


def get_latest_category_stats(spider_name: str) -> dict:
    """
    Получить файл, содержащий данные о самом раннем спаршенном
    продукте по всем ранее собранным категориям.
    """
    latest_category_stats = dict()
    filename = CATEGORY_STATS_FILENAME.format(spider_name=spider_name)
    filepath = CACHE_CATEGORY_PATH / filename
    try:
        with open(filepath) as file:
            latest_category_stats = json.load(file)

    except OSError as e:
        logger.debug(f"Can't open {filepath}: {e}")
    except JSONDecodeError as e:
        logger.debug(f"Can't load data from {filepath}: {e}")

    return latest_category_stats


def save_current_category_stats(spider_name: str, category_stats: dict):
    filename = CATEGORY_STATS_FILENAME.format(spider_name=spider_name)
    filepath = CACHE_CATEGORY_PATH / filename
    with open(filepath, 'w') as file:
        file.write(json.dumps(category_stats))
