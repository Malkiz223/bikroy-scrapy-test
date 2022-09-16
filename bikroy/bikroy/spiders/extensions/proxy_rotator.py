from contextlib import suppress
from itertools import cycle

from scrapy.crawler import Crawler


class ProxyPool:
    def __init__(self):
        self._pool_name = self.__class__.__name__
        self.proxies: list[str] = []
        self._proxies_iter: cycle[str] = None
        self._load_proxies()
        self._update_proxies_iter()

    def _load_proxies(self):
        raise NotImplemented

    def get_proxy(self) -> str:
        """При каждом вызове подставляет новую прокси"""
        with suppress(StopIteration):
            return next(self._proxies_iter)

    def _update_proxies_iter(self):
        self._proxies_iter = cycle(self.proxies)

    def __bool__(self):
        return bool(self.proxies)

    def __len__(self):
        return len(self.proxies)

    def __repr__(self):
        return f'<{self._pool_name}: {len(self.proxies)}>'


class ProxyLine(ProxyPool):
    """В рамках тестового задания прокси просто захардкожены."""

    def _load_proxies(self):
        self.proxies = [
            'http://KjSzTTV6:nHdF2DrJ@193.8.175.43:47949',
            'http://KjSzTTV6:nHdF2DrJ@193.150.170.243:57311',
            'http://KjSzTTV6:nHdF2DrJ@45.138.212.252:64825',
            'http://KjSzTTV6:nHdF2DrJ@91.209.31.50:55833',
            'http://KjSzTTV6:nHdF2DrJ@185.103.62.192:61172',
        ]


class ProxyRotator:
    def __init__(self):
        self.proxy_pool = ProxyLine()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls()

    def process_request(self, request, spider):
        self.request_fill_proxy(request)
        return

    def request_fill_proxy(self, request):
        proxy = self.proxy_pool.get_proxy()
        request.meta['proxy'] = proxy
