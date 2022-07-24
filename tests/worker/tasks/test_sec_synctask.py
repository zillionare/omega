import unittest
from tests import init_test_env
import omicron
from omega.worker.tasks.sec_synctask import sync_xrxd_report_list
import datetime
from omicron import cache
import cfg4py
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq


class SecSyncTaskTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()
        await self.create_quotes_fetcher()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def create_quotes_fetcher(self):
        cfg = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def test_sync_xrxd_report_list(self):
        # fixme: 先清除掉influxdb和缓存
        await cache.sys.hmset("ut:sec:xrxd:report", "worker_count", "0")
        await sync_xrxd_report_list(
            {
                "end": datetime.date(2022, 6, 30),
                "state": "start",
                "timeout": 20,
                "name": "ut_sync_xrxd",
            }
        )

        # fixme: how to check result? 这里应该检查influxdb和缓存中的数据是否如期望
