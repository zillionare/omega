import datetime
import unittest
from unittest import mock

import cfg4py
import omicron
from omicron import cache
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.security import Security

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.worker.tasks.sec_synctask import sync_xrxd_report_list
from omega.worker.tasks.task_utils import cache_init
from tests import init_test_env, mock_jq_data
from tests.demo_fetcher.demo_fetcher import DemoFetcher

cfg = cfg4py.get_instance()


class SecSyncTaskTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await omicron.cache.init()
        await cache_init()
        await omicron.init()

        await self.create_quotes_fetcher()

        url, token, bucket, org = (
            cfg.influxdb.url,
            cfg.influxdb.token,
            cfg.influxdb.bucket_name,
            cfg.influxdb.org,
        )
        self.client = InfluxClient(url, token, bucket, org)
        name = "security_xrxd_reports"
        await self.client.drop_measurement(name)

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def create_quotes_fetcher(self):
        self.aq = AbstractQuotesFetcher()
        instance = DemoFetcher()
        self.aq._instances.append(instance)

    @mock.patch("tests.demo_fetcher.demo_fetcher.DemoFetcher.get_finance_xrxd_info")
    async def test_sync_xrxd_report_list(self, _xrxd):
        mock_data2 = mock_jq_data("xrxd_210601_220908.pik")
        _xrxd.side_effect = [mock_data2]

        await sync_xrxd_report_list(
            {
                "end": datetime.date(2022, 6, 30),
                "state": "start",
                "timeout": 20,
                "name": "ut_sync_xrxd",
            }
        )
        rc = await Security.get_xrxd_info(datetime.date(2022, 6, 21), "000006.XSHE")
        self.assertEqual(rc[0]["code"], "000006.XSHE")
