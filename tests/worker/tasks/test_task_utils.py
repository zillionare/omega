import unittest
from unittest import mock

import omicron
from omicron import cache

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.worker.tasks.task_utils import cache_init
from tests import init_test_env, mock_jq_data
from tests.demo_fetcher.demo_fetcher import DemoFetcher


class TaskUtilsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()
        await self.create_quotes_fetcher()

    async def create_quotes_fetcher(self):
        self.aq = AbstractQuotesFetcher()
        instance = DemoFetcher()
        self.aq._instances.append(instance)

    async def asyncTearDown(self) -> None:
        await omicron.close()

    @mock.patch("tests.demo_fetcher.demo_fetcher.DemoFetcher.get_all_trade_days")
    @mock.patch("tests.demo_fetcher.demo_fetcher.DemoFetcher.get_security_list")
    async def test_cache_init(self, _sec_list, _calendar):
        mock_data1 = mock_jq_data("securitylist_0223.pik")
        _sec_list.side_effect = [mock_data1]

        mock_data2 = mock_jq_data("trade_days_0223.pik")
        _calendar.side_effect = [mock_data2]

        await cache.security.delete("calendar:1d")
        await cache.security.delete("security:all")
        await cache_init()
        rc = await cache.security.exists("calendar:1d")
        self.assertTrue(rc)
        rc = await cache.security.exists("security:all")
        self.assertTrue(rc)
