import json
import logging
import re
from contextlib import suppress
from urllib.parse import urljoin, parse_qsl, urlparse

import dateparser
from price_parser import Price
from scrapy import Request, Spider
from w3lib.url import url_query_parameter, add_or_replace_parameter, add_or_replace_parameters

from .constants.bikroy_com import *
from .exceptions.exceptions import DataOutException
from .helpers.helpers import get_full_link
from ..items import ProductItem

logger = logging.getLogger(__name__)


class BikroyComParser:

    def get_item_id(self, prod_data: dict) -> dict:
        item_id = prod_data['id']
        return item_id

    def get_author_name(self, prod_data: dict) -> dict:
        author_name = prod_data['contactCard']['name']
        return {'author_name': author_name}

    def get_author_phone(self, prod_data: dict) -> dict:
        try:
            author_phone = prod_data['contactCard']['phoneNumbers'][0]['number']
        except LookupError:
            author_phone = ''
        return {'author_phone': author_phone}

    def get_creation_timestamp(self, prod_data: dict) -> dict:
        creation_timestamp = prod_data['adDate']
        creation_timestamp = dateparser.parse(creation_timestamp)
        creation_timestamp = int(creation_timestamp.timestamp())
        return {'creation_timestamp': creation_timestamp}

    def get_images(self, prod_data: dict) -> dict:
        images_data = prod_data['images']['meta']
        images_url = [image_data['src'] for image_data in images_data]
        # у фотографий на сайте можно указать любое желаемое
        # разрешение, по умолчанию указывается 780x585
        # TODO: добавляется fitted.jpg и cropped.jpg
        images_url = [f'{image_url}/780/585/fitted.jpg' for image_url in images_url]
        return {'images': images_url}

    def get_metadata(self, prod_data: dict) -> dict:  # TODO: 3.9+?
        props = prod_data['properties']
        metadata = dict()

        for prop in props:
            with suppress(KeyError):
                prop_key = prop['label']
                prop_value = prop['value']
                metadata[prop_key] = prop_value

        return {"metadata": metadata}

    def get_price(self, prod_data: dict) -> dict:
        try:
            price = Price.fromstring(prod_data['money']['amount'])
            price = price.amount_float
        except KeyError:
            price = 0.0
        return {'price': price}

    def get_title(self, prod_data: dict) -> dict:
        try:
            title = prod_data['title']
        except KeyError:
            raise DataOutException("Can't parse title")
        return {'title': title}

    def get_address(self, result: ProductItem) -> dict:
        address = result.get('metadata', {}).get('Address', '')
        return {'address': address}

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

            url_without_query = self.get_url_without_query(url)
            url = add_or_replace_parameters(url_without_query, sorted_query)
            result_urls.append(url)
        result_urls = [url.replace('://www.', '://') for url in result_urls]
        return result_urls

    def get_categories_url(self, response):  # TODO: category_urls
        category_urls = response.xpath(
            "(//div[contains(@id, 'collapsible-content')])[1]//a/@href").getall()  # TODO: constants
        category_urls = [get_full_link(DOMAIN, url) for url in category_urls]

        category_urls = self.get_valid_category_urls(category_urls)

        return category_urls

    def get_locations_url(self, response):
        locations_url = response.xpath("(//div[contains(@id, 'collapsible-content')])[2]//a/@href").getall()
        locations_url = [get_full_link(DOMAIN, url) for url in locations_url]

        locations_url = self.get_valid_category_urls(locations_url)

        return locations_url

    def get_url_without_query(self, url: str) -> str:
        return urljoin(url, urlparse(url).path)


class BikroySpiderSpider(Spider, BikroyComParser):
    name = 'bikroy.com'
    start_urls = [
        'https://bikroy.com/en/ads/bangladesh?buy_now=0&order=desc&page=1&sort=date&urgent=0',
    ]
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 60,
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 0.3,
        'RETRY_TIMES': 10,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapyproject.extensions.proxy_rotator.ProxyRotator': 50,
        },
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'ITEM_PIPELINES': {
            # 'scrapyproject.extensions.pipelines.DropDublicateRPC': 100,
            # 'scrapyproject.pipelines.BaseSpiderFormatChecker': 200
        }
    }

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_categories)

    def parse_categories(self, response):  # TODO: sort query by bey
        categories_url = self.get_categories_url(response)
        for url in categories_url[1:]:
            yield Request(url=url, callback=self.parse_subcategories)

    def parse_subcategories(self, response):
        subcategories_url = self.get_categories_url(response)
        for url in subcategories_url[2:]:  # TODO: объяснить зачем обрезаем (и выше)
            yield Request(url=url, callback=self.parse_locations)

    def parse_locations(self, response):
        locations_url = self.get_locations_url(response)
        for url in locations_url[1:]:
            yield Request(url=url, callback=self.parse_sublocations)

    def parse_sublocations(self, response):
        sublocations_url = self.get_locations_url(response)
        for url in sublocations_url[2:]:
            category_url = self.get_url_without_query(url)
            yield Request(url=url, callback=self.parse,
                          meta={'category_url': category_url})

    def parse(self, response):
        json_data = re.search(r'window.initialData = ({.*})', response.text)
        if not json_data:  # TODO: next_page? retry?
            return  # FIXME
        json_data = json_data.group(1)
        json_data = json.loads(json_data)
        products = json_data['serp']['ads']['data']['ads']

        # TODO: смотрим ссылку на категорию,
        PART_PRODUCT_PAGE_URL = 'https://bikroy.com/en/ad/'
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

    # TODO: if products > 10_000: parse_subcategory; if subcategory_urls > 10_000: parse_locations

    def parse_product(self, response):
        json_data = response.xpath("//script[contains(text(), 'window.initialData')]/text()").get()
        json_data = re.search(r'window.initialData = ({.*})', json_data).group(1)
        json_data = json.loads(json_data)

        product_data = json_data.get('adDetail').get('data', {}).get('ad', {})

        item = ProductItem()
        item['url'] = response.url
        item.update(self.get_item_id(product_data))
        item.update(self.get_creation_timestamp(product_data))
        item.update(self.get_title(product_data))
        item.update(self.get_author_name(product_data))
        item.update(self.get_author_phone(product_data))
        item.update(self.get_price(product_data))
        item.update(self.get_images(product_data))
        item.update(self.get_metadata(product_data))
        item.update(self.get_address(item))
        yield item
