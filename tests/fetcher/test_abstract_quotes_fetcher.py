import datetime
import logging
import os
import unittest

import arrow
import cfg4py
import numpy as np
import omicron
from omicron import cache
from omicron.core.timeframe import tf
from omicron.core.types import FrameType

from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from tests import init_test_env

logger = logging.getLogger(__name__)

cfg = cfg4py.get_instance()


class TestAbstractQuotesFetcher(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        init_test_env()

        await self.create_quotes_fetcher()
        await omicron.init(aq)

    async def asyncTearDown(self) -> None:
        await omicron.shutdown()

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

    async def test_get_bars_010(self):
        """日线级别, 无停牌"""
        sec = "000001.XSHE"
        frame_type = FrameType.DAY
        # 2020-4-3 Friday
        end = arrow.get("2020-04-03").date()

        # without cache
        await self.clear_cache(sec, frame_type)
        bars = await aq.get_bars(sec, end, 10, frame_type)

        self.assertEqual(bars[0]["frame"], arrow.get("2020-03-23").date())
        self.assertEqual(bars[-1]["frame"], arrow.get("2020-04-03").date())
        self.assertAlmostEqual(12.0, bars[0]["open"], places=2)
        self.assertAlmostEqual(12.82, bars[-1]["open"], places=2)

        # 检查cache
        cache_len = await cache.security.hlen(f"{sec}:{frame_type.value}")
        self.assertEqual(12, cache_len)

        # 日线级别，停牌期间，数据应该置为np.nan
        sec = "000029.XSHE"
        end = arrow.get("2020-8-18").date()
        frame_type = FrameType.DAY
        bars = await aq.get_bars(sec, end, 10, frame_type)
        self.assertEqual(10, len(bars))
        self.assertEqual(end, bars[-1]["frame"])
        self.assertEqual(arrow.get("2020-08-05").date(), bars[0]["frame"])
        self.assertTrue(np.all(np.isnan(bars["close"])))

    async def test_get_bars_011(self):
        """分钟级别，中间有停牌，end指定时间未对齐的情况"""
        # 600721, ST百花， 2020-4-29停牌一天
        sec = "600721.XSHG"
        frame_type = FrameType.MIN60
        end = arrow.get("2020-04-30 10:32", tzinfo="Asia/Shanghai").datetime

        await self.clear_cache(sec, frame_type)
        bars = await aq.get_bars(sec, end, 7, frame_type)
        print(bars)

        self.assertEqual(7, len(bars))
        self.assertEqual(
            arrow.get("2020-04-28 15:00", tzinfo="Asia/Shanghai"), bars["frame"][0]
        )
        self.assertEqual(
            arrow.get("2020-04-30 10:30", tzinfo="Asia/Shanghai"), bars["frame"][-2]
        )
        self.assertEqual(
            arrow.get("2020-4-30 10:32", tzinfo="Asia/Shanghai"), bars["frame"][-1]
        )

        self.assertAlmostEqual(5.37, bars["open"][0], places=2)
        self.assertAlmostEqual(5.26, bars["open"][-2], places=2)
        self.assertAlmostEqual(5.33, bars["open"][-1], places=2)

        # 检查cache,10：32未存入cache
        cache_len = await cache.security.hlen(f"{sec}:{frame_type.value}")
        self.assertEqual(8, cache_len)
        bars_2 = await cache.get_bars(sec, tf.floor(end, frame_type), 6, frame_type)
        np.array_equal(bars[:-1], bars_2)

    async def test_get_bars_012(self):
        """分钟级别，中间有一天停牌，end指定时间正在交易"""
        # 600721, ST百花， 2020-4-29停牌一天
        sec = "600721.XSHG"
        frame_type = FrameType.MIN60
        end = arrow.get("2020-04-30 10:30", tzinfo=cfg.tz).datetime

        bars = await aq.get_bars(sec, end, 6, frame_type)
        print(bars)
        self.assertEqual(6, len(bars))
        self.assertEqual(
            arrow.get("2020-04-28 15:00", tzinfo="Asia/Shanghai"), bars["frame"][0]
        )
        self.assertEqual(
            arrow.get("2020-04-30 10:30", tzinfo="Asia/Shanghai"), bars["frame"][-1]
        )
        self.assertAlmostEqual(5.37, bars["open"][0], places=2)
        self.assertAlmostEqual(5.26, bars["open"][-1], places=2)
        self.assertTrue(np.isnan(bars["open"][1]))

        # 结束时间帧未结束
        end = arrow.get("2020-04-30 10:32:00.13", tzinfo=cfg.tz).datetime
        frame_type = FrameType.MIN30
        bars = await aq.get_bars(sec, end, 6, frame_type)
        print(bars)
        self.assertAlmostEqual(5.33, bars[-1]["close"], places=2)
        self.assertEqual(end.replace(second=0, microsecond=0), bars[-1]["frame"])

    async def test_get_bars_013(self):
        """分钟级别，end指定时间正处在停牌中"""
        # 600721, ST百花， 2020-4-29停牌一天
        sec = "600721.XSHG"
        frame_type = FrameType.MIN60
        end = arrow.get("2020-04-29 10:30", tzinfo="Asia/Chongqing").datetime

        await self.clear_cache(sec, frame_type)

        bars = await aq.get_bars(sec, end, 6, frame_type)
        print(bars)
        self.assertEqual(6, len(bars))
        self.assertEqual(
            arrow.get("2020-04-27 15:00", tzinfo="Asia/Shanghai"), bars["frame"][0]
        )
        self.assertEqual(
            arrow.get("2020-04-29 10:30", tzinfo="Asia/Shanghai"), bars["frame"][-1]
        )
        self.assertAlmostEqual(5.47, bars["open"][0], places=2)
        self.assertAlmostEqual(5.37, bars["open"][-2], places=2)
        self.assertTrue(np.isnan(bars["open"][-1]))

        # 检查cache,10：30 已存入cache
        cache_len = await cache.security.hlen(f"{sec}:{frame_type.value}")
        self.assertEqual(8, cache_len)
        bars_2 = await cache.get_bars(sec, tf.floor(end, frame_type), 6, frame_type)
        np.array_equal(bars, bars_2)

    async def test_get_bars_014(self):
        """测试周线级别未结束的frame能否对齐"""
        # 4/29停牌，4/30复牌, 4/30周线结束
        sec = "600721.XSHG"
        frame_type = FrameType.WEEK

        end = arrow.get("2020-4-29 15:00").datetime  # 周三，该周周四结束
        bars = await aq.get_bars(sec, end, 3, FrameType.WEEK)
        print(bars)
        """
        [(datetime.date(2020, 4, 17), 6.02, 6.69, 5.84, 6.58, 22407., 1.407e+08, 1.455)
         (datetime.date(2020, 4, 24), 6.51, 6.57, 5.68, 5.72, 25911., 1.92e+08, 1.455)
         (datetime.date(2020, 4, 28), 5.7, 5.71, 5.17, 5.36, 11879667,6.39341393e+07, 1.455)]
        """
        self.assertEqual(datetime.date(2020, 4, 17), bars[0]["frame"])
        self.assertEqual(datetime.date(2020, 4, 24), bars[1]["frame"])
        self.assertEqual(datetime.date(2020, 4, 28), bars[-1]["frame"])

        self.assertAlmostEqual(6.58, bars[0]["close"], places=2)
        self.assertAlmostEqual(5.72, bars[1]["close"], places=2)
        self.assertAlmostEqual(5.36, bars[-1]["close"], places=2)

        end = arrow.get("2020-04-30 15:00").datetime
        bars = await aq.get_bars(sec, end, 3, frame_type)
        print(bars)
        """
        [(datetime.date(2020, 4, 17), 6.02, 6.69, 5.84, 6.58, 2241., 1.4e+08, 1.455)
         (datetime.date(2020, 4, 24), 6.51, 6.57, 5.68, 5.72, 2511., 1.55e+08, 1.455)
         (datetime.date(2020, 4, 30), 5.7, 5.71, 5.17, 5.39, 15645495, 84086903, 1.455)]
        """

        self.assertEqual(datetime.date(2020, 4, 17), bars[0]["frame"])
        self.assertEqual(datetime.date(2020, 4, 24), bars[1]["frame"])
        self.assertEqual(datetime.date(2020, 4, 30), bars[-1]["frame"])

        self.assertAlmostEqual(6.58, bars[0]["close"], places=2)
        self.assertAlmostEqual(5.72, bars[1]["close"], places=2)
        self.assertAlmostEqual(5.39, bars[-1]["close"], places=2)

    async def test_get_bars_015(self):
        """test when include_unclosed is False"""
        sec = "000001.XSHG"
        frame_type = FrameType.WEEK
        end = datetime.date(2021, 2, 9)

        # without cache
        await self.clear_cache(sec, frame_type)
        bars = await aq.get_bars(sec, end, 1, frame_type, include_unclosed=False)
        self.assertEqual(datetime.date(2021, 2, 5), bars["frame"][0])

    async def test_get_bars_016(self):
        """test when bars is empty"""
        sec = "605060.XSHG"
        frame_type = FrameType.MONTH
        end = datetime.date(2021, 2, 1)
        bars = await aq.get_bars(sec, end, 1, frame_type, include_unclosed=False)
        self.assertIsNone(bars)

    async def test_get_valuation(self):
        secs = ["000001.XSHE", "600000.XSHG"]
        date = arrow.get("2020-10-26").date()

        # return two records, one for each
        vals = await aq.get_valuation(secs, date)
        self.assertSetEqual(set(secs), set(vals["code"].tolist()))
        self.assertEqual(len(secs), len(vals))

        # return two records, only two fields
        vals = await aq.get_valuation(secs, date, fields=["frame", "code"])
        self.assertEqual(set(secs), set(vals["code"].tolist()))
        self.assertSequenceEqual(vals.dtype.names, ["frame", "code"])

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

        n = tf.count_frames(start, stop, frame_type)
        bars = await aq.get_bars(code, stop, n, frame_type)
        print(bars)
