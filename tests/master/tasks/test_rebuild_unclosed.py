import unittest
from tests import init_test_env, test_dir
import omicron
from omicron.models.stock import Stock
from coretypes import FrameType
import pickle
from unittest import mock
from omega.master.tasks.rebuild_unclosed import (
    _rebuild_min_level_unclosed_bars,
    _rebuild_day_level_unclosed_bars,
)
import arrow
from omicron import cache


class RebuildUnclosedTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()
        await Stock.reset_cache()

        file = test_dir() / "data" / "test_rebuild_unclosed" / "days.pkl"
        with open(file, "rb") as f:
            barss = pickle.load(f)
            self.secs = list(barss.keys())
            await Stock.persist_bars(FrameType.DAY, barss)

        file = test_dir() / "data" / "test_rebuild_unclosed" / "1m.pkl"
        with open(file, "rb") as f:
            barss = pickle.load(f)
            for code, bars in barss.items():
                await Stock.cache_bars(code, FrameType.MIN1, bars)

    async def asyncTearDown(self) -> None:
        await omicron.close()

    @mock.patch("arrow.now", return_value=arrow.get(2022, 7, 21, 11, 14))
    async def test_rebuild_min_level_unclosed_bars(self, _):
        # ensure there's no cache-related data in the database
        keys = await cache.security.keys("bars:5m:*")
        self.assertEqual(len(keys), 0)

        await _rebuild_min_level_unclosed_bars()

        # check the result
        keys = await cache.security.keys("bars:5m:*")
        self.assertSetEqual(
            set(keys),
            set([f"bars:5m:{code}" for code in self.secs] + [f"bars:5m:unclosed"]),
        )

        unclosed = await cache.security.hgetall("bars:5m:unclosed")
        self.assertSetEqual(set(unclosed.keys()), set(self.secs))
        bar_04 = unclosed["000004.XSHE"]
        self.assertEqual(
            "202207211114,8.95,9.0,8.95,9.0,18100.0,162371.0,7.446", bar_04
        )

        closed = await cache.security.hgetall("bars:30m:000004.XSHE")
        self.assertSetEqual(
            set(closed.keys()), set(["202207211000", "202207211030", "202207211100"])
        )

        unclosed_day_bar = await cache.security.hgetall("bars:1d:unclosed")
        self.assertSetEqual(set(unclosed_day_bar.keys()), set(self.secs))
        bar_01 = unclosed_day_bar["000001.XSHE"]
        exp_bar_01 = "20220721,13.32,13.34,13.22,13.23,55886660.0,740959140.0,121.71913"
        self.assertEqual(exp_bar_01, bar_01)

        # should have no bars:1d:000004.XSHE
        keys = await cache.security.keys("bars:1d:*")
        self.assertEqual(keys, ["bars:1d:unclosed"])

        # test error handling
        with mock.patch(
            "omicron.models.stock.Stock._get_cached_bars", side_effect=Exception
        ):
            await _rebuild_min_level_unclosed_bars()

        with mock.patch("omicron.models.stock.Stock.resample", side_effect=Exception):
            await _rebuild_min_level_unclosed_bars()

    @mock.patch("arrow.now", return_value=arrow.get(2022, 7, 21, 11, 14))
    async def test_rebuild_day_level_unclosed_bars(self, _):
        await _rebuild_min_level_unclosed_bars()

        # ensure there's no cache-related data in the database
        keys = await cache.security.keys("bars:1w:*")
        self.assertEqual(len(keys), 0)

        keys = await cache.security.keys("bars:1M:*")
        self.assertEqual(len(keys), 0)

        with mock.patch("omicron.models.security.Query.eval", return_value=self.secs):
            await _rebuild_day_level_unclosed_bars()

        # check the result
        unclosed_week_bar = await cache.security.hgetall("bars:1w:unclosed")
        self.assertSetEqual(set(unclosed_week_bar.keys()), set(self.secs))
        bar_02 = unclosed_week_bar["300001.XSHE"]
        self.assertEqual(
            "20220721,18.81,21.4,18.65,19.95,146464648.0,2899841442.0,6.944613", bar_02
        )

        unclosed_month_bar = await cache.security.hgetall("bars:1M:unclosed")
        self.assertSetEqual(set(unclosed_month_bar.keys()), set(self.secs))
        bar_03 = unclosed_month_bar["000004.XSHE"]
        exp_bar_03 = "20220721,8.91,10.58,8.14,9.0,58976343.0,543641584.0,7.446"
        self.assertEqual(exp_bar_03, bar_03)

        # should have no bars:1w:000004.XSHE
        keys = await cache.security.keys("bars:1w:*")
        self.assertEqual(keys, ["bars:1w:unclosed"])

        # test error handling: should raise no exception
        with mock.patch(
            "omicron.models.stock.Stock._get_persisted_bars", side_effect=Exception
        ):
            await _rebuild_day_level_unclosed_bars()

        with mock.patch("omicron.models.stock.Stock.resample", side_effect=Exception):
            await _rebuild_day_level_unclosed_bars()
