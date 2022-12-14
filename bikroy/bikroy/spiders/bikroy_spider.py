import json
import logging
import re
from contextlib import suppress
from functools import wraps
from typing import Any, Optional
from urllib.parse import parse_qsl, urlparse, urljoin

import dateparser
from price_parser import Price
from pydispatch import dispatcher
from scrapy import Request, Spider, signals
from w3lib.url import url_query_parameter, add_or_replace_parameter, add_or_replace_parameters

from .constants.bikroy_com import *
from .helpers.helpers import get_url_without_query, get_latest_category_stats, \
    save_current_category_stats
from ..items import ProductItem

logger = logging.getLogger(__name__)


def optional_field(default: Any = None):
    """Возвращает дефолтное значение, если метод вызвал исключение."""

    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                return default

        return wrapped

    return decorator


class BikroyComParser:

    def get_item_id(self, prod_data: dict) -> str:
        return prod_data['id']

    def get_title(self, prod_data: dict) -> str:
        return prod_data['title']

    @optional_field()
    def get_description(self, prod_data: dict) -> str:
        return prod_data['description']

    @optional_field(default='')
    def get_author_name(self, prod_data: dict) -> str:
        return prod_data['contactCard']['name']

    @optional_field()
    def get_author_phone(self, prod_data: dict) -> Optional[str]:
        phone_number = prod_data['contactCard']['phoneNumbers'][0]['number']
        if phone_number.startswith('0'):
            phone_number = f'+88{phone_number}'
        return phone_number

    def get_creation_timestamp(self, prod_data: dict) -> int:
        creation_timestamp = prod_data['adDate']
        creation_timestamp = dateparser.parse(creation_timestamp)
        return int(creation_timestamp.timestamp())

    @optional_field()
    def get_images(self, prod_data: dict) -> Optional[list]:
        images_data = prod_data['images']['meta']
        image_urls = [image_data['src'] for image_data in images_data]
        # у фотографий на сайте можно указать любое желаемое
        # разрешение, по умолчанию указывается 780x585
        # есть форматы cropped.jpg и fitted.jpg (оригинальный)
        image_urls = [f'{image_url}/780/585/fitted.jpg' for image_url in image_urls]
        return image_urls

    @optional_field()
    def get_metadata(self, prod_data: dict) -> Optional[dict]:
        props = prod_data['properties']
        metadata = dict()

        for prop in props:
            with suppress(KeyError):
                prop_key = prop['label']
                prop_value = prop['value']
                metadata[prop_key] = prop_value

        return metadata

    @optional_field(default=0.0)
    def get_price(self, prod_data: dict) -> float:
        price = Price.fromstring(prod_data['money']['amount'])
        price = price.amount_float or 0.0
        return price

    @optional_field()
    def get_address(self, item: ProductItem) -> Optional[str]:
        return item['metadata']['Address']

    def get_next_page(self, response):
        current_page = int(url_query_parameter(response.url, 'page', '1'))
        next_page_url = add_or_replace_parameter(response.url, 'page', str(current_page + 1))
        return next_page_url

    def get_non_redirect_category_urls(self, category_urls: list) -> list:
        """
        Сайт редиректит при любом удобном случае, замедляя паука
        в несколько раз. Нужно ставить query-params в алфавитном
        порядке и удалять "www.", хотя сайт сам его даёт.
        """
        result_urls = []
        for url in category_urls:
            parsed_url = urlparse(url)
            captured_value = parse_qsl(parsed_url.query)
            sorted_params = sorted(captured_value)
            sorted_query = {key: value for key, value in sorted_params}

            url_without_query = get_url_without_query(url)
            url = add_or_replace_parameters(url_without_query, sorted_query)
            result_urls.append(url)

        result_urls = [url.replace('://www.', '://') for url in result_urls]
        return result_urls

    def get_category_urls(self, response):
        category_urls = response.xpath(CATEGORY_URLS_XPATH).getall()
        category_urls = [urljoin(DOMAIN, url) for url in category_urls]

        return self.get_non_redirect_category_urls(category_urls)

    def get_location_urls(self, response):
        locations_url = response.xpath(REGION_URLS_XPATH).getall()
        locations_url = [urljoin(DOMAIN, url) for url in locations_url]

        return self.get_non_redirect_category_urls(locations_url)

    def has_next_page(self, json_data: dict) -> bool:
        pagination_data = json_data['serp']['ads']['data']['paginationData']
        current_page = pagination_data['activePage']
        total_products = pagination_data['total']
        page_size = pagination_data['pageSize']
        return total_products > page_size * current_page

    def get_product_updated_dates(self, products: list) -> list:
        """
        Возвращает время обновления продуктов в категории.
        Подставляет 0, если не смог собрать время обновления (товар поднят в выдаче).
        """
        result = []
        for product in products:
            try:
                creation_date = dateparser.parse(product['timeStamp'])
                result.append(int(creation_date.timestamp()))
            except (AttributeError, KeyError):  # поднятые объявления не имеют даты обновления
                result.append(0)
        return result

    def parsed_before(self, product_updated_timestamp: int, category_url: str) -> bool:
        previous_parsed_timestamp = self.latest_category_stats.get(category_url)
        if previous_parsed_timestamp:
            return previous_parsed_timestamp > product_updated_timestamp
        return False

    def update_parsed_category_date(self, response, item: ProductItem):
        category_url = response.meta.get('category_url')
        if not category_url:
            return

        creation_timestamp = item['creation_timestamp']

        min_parsed_category_timestamp = self.current_category_stats.get(category_url)
        if not min_parsed_category_timestamp or creation_timestamp > min_parsed_category_timestamp:
            self.current_category_stats[category_url] = creation_timestamp

    def get_json_data(self, response) -> dict:
        json_data = re.search(r'window.initialData = ({.*})', response.text).group(1)
        return json.loads(json_data)

    def get_cleared_category_urls(self, response, category_urls: list) -> list:
        """Возвращает ссылки на все категории ниже текущей."""
        current_category_url = get_url_without_query(response.url)
        if current_category_url in category_urls:
            right_index = category_urls.index(current_category_url) + 1
            return category_urls[right_index:]
        return category_urls

    def is_product_url(self, url: str) -> bool:
        return '/ad/' in url


class BikroySpiderSpider(Spider, BikroyComParser):
    name = 'bikroy.com'

    def __init__(self, start_urls=None, **kwargs):
        super().__init__(**kwargs)
        self.start_urls = start_urls.split('|') if start_urls else [
            'https://bikroy.com/en/ads',  # все категории сайта
        ]
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.latest_category_stats = get_latest_category_stats(self.name)
        self.current_category_stats = dict()

    def spider_closed(self, spider):
        """
        При повторном запуске для ускорения работы паука сохраняем данные
        о времени создания объявления, с которого начали парсить категорию.
        """
        self.latest_category_stats.update(self.current_category_stats)
        save_current_category_stats(self.name, self.latest_category_stats)

    def start_requests(self):
        for url in self.start_urls:
            if self.is_product_url(url):
                yield Request(url=url, callback=self.parse_product)
            else:
                yield Request(url=url, callback=self.parse_categories)

    def parse_categories(self, response):

        category_urls = self.get_category_urls(response)
        category_urls = self.get_cleared_category_urls(response, category_urls)
        if category_urls:
            for url in category_urls:
                yield Request(url=url, callback=self.parse_categories, dont_filter=True)
            return

        city_urls = self.get_location_urls(response)
        city_urls = self.get_cleared_category_urls(response, city_urls)
        if city_urls:
            for url in city_urls:
                yield Request(url=url, callback=self.parse_categories, dont_filter=True)
            return

        yield Request(url=response.url, callback=self.parse, dont_filter=True, meta={'category_url': response.url})

    def parse(self, response):
        json_data = self.get_json_data(response)
        try:
            products = json_data['serp']['ads']['data']['ads']
        except KeyError:
            products = []

        if not products:
            logger.debug(f'Empty category page: {response.url}')
            return

        category_url = response.meta['category_url']
        products_updated_dates = self.get_product_updated_dates(products)

        for product, product_updated_timestamp in zip(products, products_updated_dates):
            if self.parsed_before(product_updated_timestamp, category_url):
                logger.debug(f'Parsed before: {response.url}')
                return

            slug = product['slug']
            url = urljoin(PART_PRODUCT_PAGE_URL, slug)
            yield Request(url=url, callback=self.parse_product, meta={'category_url': category_url})

        if self.has_next_page(json_data):
            next_page_url = self.get_next_page(response)
            yield Request(url=next_page_url, callback=self.parse,
                          meta={'category_url': category_url})
        else:
            logger.debug(f'Last category page: {response.url}')

    def parse_product(self, response):
        json_data = self.get_json_data(response)
        product_data = json_data['adDetail']['data']['ad']

        item = ProductItem()
        item['url'] = response.url
        item['item_id'] = self.get_item_id(product_data)
        item['title'] = self.get_title(product_data)
        item['description'] = self.get_description(product_data)
        item['creation_timestamp'] = self.get_creation_timestamp(product_data)
        item['author_name'] = self.get_author_name(product_data)
        item['author_phone'] = self.get_author_phone(product_data)
        item['price'] = self.get_price(product_data)
        item['images'] = self.get_images(product_data)
        item['metadata'] = self.get_metadata(product_data)
        item['address'] = self.get_address(item)
        self.update_parsed_category_date(response, item)
        yield item
