import json
import logging
import re
from contextlib import suppress
from functools import wraps
from typing import Any, Optional
from urllib.parse import parse_qsl, urlparse

import dateparser
from price_parser import Price
from scrapy import Request, Spider
from w3lib.url import url_query_parameter, add_or_replace_parameter, add_or_replace_parameters

from .constants.bikroy_com import *
from .helpers.helpers import get_full_link, get_url_without_query
from ..items import ProductItem

logger = logging.getLogger(__name__)


def optional_field(default: Any = None):
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
        return prod_data['contactCard']['phoneNumbers'][0]['number']

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
        # есть форматы cropped.jpg и fitted.jpg (оригинальные)
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

    def get_valid_category_urls(self, urls: list) -> list:
        result_urls = []
        for url in urls:
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
        category_urls = [get_full_link(DOMAIN, url) for url in category_urls]

        return self.get_valid_category_urls(category_urls)

    def get_location_urls(self, response):
        locations_url = response.xpath(REGION_URLS_XPATH).getall()
        locations_url = [get_full_link(DOMAIN, url) for url in locations_url]

        return self.get_valid_category_urls(locations_url)


class BikroySpiderSpider(Spider, BikroyComParser):
    name = 'bikroy.com'
    start_urls = [
        'https://bikroy.com/en/ads/bangladesh',  # TODO:
        # 'https://bikroy.com/en/ads/bangladesh/property',
    ]
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 60,
        'CONCURRENT_REQUESTS': 12,  # 290 тысяч за 23 часа
        'DOWNLOAD_DELAY': 0.2,
        'RETRY_TIMES': 10,
        'DOWNLOADER_MIDDLEWARES': {
            'bikroy.spiders.extensions.proxy_rotator.ProxyRotator': 100,
        },
        'ITEM_PIPELINES': {
            'bikroy.pipelines.DuplicatesPipeline': 50,
        },
        'USER_AGENT': CHROME_USERAGENT,
    }

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_categories)

    def parse_categories(self, response):
        category_urls = self.get_category_urls(response)
        for url in category_urls[1:]:
            yield Request(url=url, callback=self.parse_subcategories, dont_filter=True)

    def parse_subcategories(self, response):
        subcategory_urls = self.get_category_urls(response)
        for url in subcategory_urls[2:]:
            yield Request(url=url, callback=self.parse_cities, dont_filter=True)

    def parse_cities(self, response):
        city_urls = self.get_location_urls(response)
        for url in city_urls[1:]:
            yield Request(url=url, callback=self.parse_city_regions)

    def parse_city_regions(self, response):
        city_region_urls = self.get_location_urls(response)
        for url in city_region_urls[2:]:
            category_url = get_url_without_query(url)
            yield Request(url=url, callback=self.parse,
                          meta={'category_url': category_url})

    def parse(self, response):
        json_data = re.search(r'window.initialData = ({.*})', response.text).group(1)
        json_data = json.loads(json_data)
        try:
            products = json_data['serp']['ads']['data']['ads']
        except KeyError:
            products = []

        for product in products:
            slug = product['slug']
            url = get_full_link(PART_PRODUCT_PAGE_URL, slug)
            yield Request(url=url, callback=self.parse_product)

        if products:
            next_page_url = self.get_next_page(response)
            category_url = response.meta['category_url']
            yield Request(url=next_page_url, callback=self.parse,
                          meta={'category_url': category_url})
        else:
            logger.debug(f'Empty category page: {response.url}')

    def parse_product(self, response):
        json_data = re.search(r'window.initialData = ({.*})', response.text).group(1)
        json_data = json.loads(json_data)

        product_data = json_data.get('adDetail').get('data', {}).get('ad', {})

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
        yield item
