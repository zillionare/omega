import datetime
import logging
import os
import unittest
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
from coretypes import FrameType
from omicron import cache

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from tests import init_test_env

logger = logging.getLogger(__name__)

cfg = cfg4py.get_instance()


class TestAbstractQuotesFetcher(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await self.create_quotes_fetcher()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    def get_config_path(self):
        src_dir = os.path.dirname(__file__)
        return os.path.join(src_dir, "../omega/config")

    async def create_quotes_fetcher(self):
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        account = fetcher_info["account"]
        password = fetcher_info["password"]
        await aq.create_instance(impl, account=account, password=password)

    async def clear_cache(self, sec: str, frame_type: FrameType):
        await cache.security.delete(f"{sec}:{frame_type.value}")

    async def test_get_security_list(self):
        # 2020-11-01
        end_dt = arrow.get("2005-12-31").date()
        secs = await aq.get_security_list(end_dt)
        self.assertEqual("000001.XSHE", secs[0][0])

    async def test_get_bars_batch(self):
        secs = ["000001.XSHE", "000001.XSHG"]
        end_dt = arrow.get("2020-11-01").date()
        frame_type = FrameType.DAY

        bars = await aq.get_bars_batch(secs, end_dt, 5, frame_type)
        self.assertSetEqual(set(secs), set(bars.keys()))
        self.assertEqual(5, len(bars["000001.XSHE"]))
        self.assertAlmostEqual(18.2, bars["000001.XSHE"]["open"][0], places=2)

    async def test_get_all_trade_days(self):
        days = await aq.get_all_trade_days()
        self.assertIn(datetime.date(2020, 12, 31), days)
