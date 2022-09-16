from scrapy.crawler import Crawler


class ProxyPool:
    pass


class Proxy6(ProxyPool):
    pass


class ProxyRotator:

    def __init__(self):
        self.init_proxy_pools()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls()

    def process_request(self, request, spider):
        return
