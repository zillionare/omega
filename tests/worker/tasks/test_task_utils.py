import unittest

import cfg4py
import omicron
from omicron import cache

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.tasks.task_utils import cache_init
from tests import init_test_env


class TaskUtilsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()
        await self.create_quotes_fetcher()

    async def create_quotes_fetcher(self):
        cfg = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def test_cache_init(self):
        await cache.security.delete("calendar:1d")
        await cache.security.delete("security:all")
        await cache_init()
        self.assertTrue(cache.security.exists("calendar:1d"))
        self.assertTrue(cache.security.exists("security:all"))
