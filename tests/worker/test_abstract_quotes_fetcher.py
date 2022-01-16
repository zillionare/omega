import datetime
import logging
import os
import unittest

import arrow
import cfg4py
import numpy as np
import omicron
from omicron import cache
from omicron.models.calendar import Calendar as cal
from omicron.core.types import FrameType

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
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def clear_cache(self, sec: str, frame_type: FrameType):
        await cache.security.delete(f"{sec}:{frame_type.value}")

    async def test_get_security_list(self):
        secs = await aq.get_security_list()
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

    async def test_foo(self):
        start = arrow.get("2020-11-02 15:00:00", tzinfo=cfg.tz).datetime
        stop = arrow.get("2020-11-06 14:30:00", tzinfo=cfg.tz).datetime
        frame_type = FrameType.MIN30
        code = "000001.XSHE"

        n = cal.count_frames(start, stop, frame_type)
        bars = await aq.get_bars(code, stop, n, frame_type)
        print(bars)

    async def test_get_fund_list(self):
        code = ["512690"]
        vals = await aq.get_fund_list(code=code)
        self.assertEqual(len(code), len(vals))

        vals = await aq.get_fund_list()
        self.assertTrue(len(vals))

        vals = await aq.get_fund_list(code=code, fields="code")
        self.assertEqual(len(code), len(vals))
        self.assertSequenceEqual(vals.dtype.names, ["code"])

        code = ["233333"]
        vals = await aq.get_fund_list(code=code)
        self.assertFalse(len(vals))

    async def test_get_fund_net_value(self):
        code = ["512690"]
        vals = await aq.get_fund_list(code=code)
        self.assertEqual(len(code), len(vals))
        vals = await aq.get_fund_list(code=code, fields="code")
        self.assertEqual(len(code), len(vals))
        self.assertSequenceEqual(vals.dtype.names, ["code"])
        vals = await aq.get_fund_net_value(day=arrow.get("2021-01-09").date())
        self.assertTrue(len(vals))

    async def test_get_fund_share_daily(self):
        code = ["512690"]
        day = arrow.get("2021-10-26").date()
        vals = await aq.get_fund_share_daily(code, day=day)
        self.assertTrue(len(vals))
        vals = await aq.get_fund_share_daily(code, day=day, fields="code")
        self.assertTrue(len(vals))
        self.assertSequenceEqual(vals.dtype.names, ["code"])

    async def test_get_fund_portfolio_stock(self):
        vals = await aq.get_fund_portfolio_stock(pub_date="2021-12-21")
        self.assertEqual(len(vals), 0)

        vals = await aq.get_fund_portfolio_stock(pub_date="2021-01-22")
        self.assertTrue(len(vals))

        vals = await aq.get_fund_portfolio_stock(code=["150016"])
        self.assertTrue(len(vals))

        vals = await aq.get_fund_portfolio_stock(code=["150016"], fields="code")
        self.assertTrue(len(vals))
        self.assertSequenceEqual(vals.dtype.names, ["code"])
