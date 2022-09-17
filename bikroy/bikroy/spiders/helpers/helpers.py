import json
import logging
from json import JSONDecodeError
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

SPIDER_DATE_PARSED_CATEGORY_PATH = '{spider_name}_parsed_category_stats.json'


def get_url_without_query(url: str) -> str:
    return urljoin(url, urlparse(url).path)


def get_previous_parsed_category_dates(spider_name: str) -> dict:
    """
    Получить файл, содержащий данные о самом раннем спаршенном
    продукте по всем ранее собранным категориям.
    """
    previous_parsed_category_dates = dict()
    try:
        filename = SPIDER_DATE_PARSED_CATEGORY_PATH.format(spider_name=spider_name)
        with open(filename) as file:
            previous_parsed_category_dates = json.load(file)

    except OSError as e:
        logger.debug(f"Can't open {SPIDER_DATE_PARSED_CATEGORY_PATH}: {e}")
    except JSONDecodeError as e:
        logger.debug(f"Can't load data from {SPIDER_DATE_PARSED_CATEGORY_PATH}: {e}")

    return previous_parsed_category_dates


def save_parsed_category_dates(spider_name: str, parsed_category_dates: dict):
    with open(SPIDER_DATE_PARSED_CATEGORY_PATH.format(spider_name=spider_name), 'w') as file:
        file.write(json.dumps(parsed_category_dates))
