import logging
import os
import unittest
from asyncio import Future
from unittest import mock

import arrow
import cfg4py
import omicron
from omicron.dal import cache
from omicron.dal import security_cache
from omicron.core.types import FrameType
from omicron.core.lang import async_run
from omicron.core.timeframe import tf

import omega.jobs.synccalendar as sc
import omega.jobs.syncquotes as sq
from omega.main import Application as app
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class MyTestCase(unittest.TestCase):
    def get_config_path(self):
        src_dir = os.path.dirname(__file__)
        return os.path.join(src_dir, '../omega/config')

    @async_run
    async def setUp(self) -> None:
        os.environ[cfg4py.envar] = 'TEST'

        logger.info("starting solo quotes server...")
        cfg4py.init(self.get_config_path(), False)

        await omicron.init(cfg)
        app.is_leader = True

        for fetcher in cfg.quotes_fetchers:
            await AbstractQuotesFetcher.create_instance(fetcher)

    async def sync_and_check(self, code, end, frame_type, n_fetched, expected_head,
                             n_cached):
        fut1 = Future()
        fut1.set_result(code)

        fut2 = Future()
        fut2.set_result(None)

        with mock.patch('omicron.cache.sys.lpop', side_effect=[fut1, fut2]):
            await sq.sync_bars(frame_type, sync_to=end, max_bars=n_fetched)
            bars = await security_cache.get_bars(code, end, n_fetched, frame_type)
            hlen = await cache.security.hlen(f"{code}:{frame_type.value}")
            head, tail = await security_cache.get_bars_range(code, frame_type)

            # key space include head, tail
            self.assertEqual(hlen, n_cached + 2)
            self.assertEqual(len(bars), n_fetched)
            bars_start = tf.shift(end, -n_fetched + 1, frame_type)
            self.assertEqual(bars_start, bars[0]['frame'])
            self.assertEqual(end, bars[-1]['frame'])
            self.assertEqual(expected_head, head)
            self.assertEqual(end, tail)

    @async_run
    async def test_000_sync_day_bars(self):
        code = '000001.XSHE'
        frame_type = FrameType.DAY
        bars_to_fetch = 500
        bars_available = 500

        # day 0 sync
        # assume the cache is totally empty, then we'll fetch up to
        # cfg.omega.max_bars bars data, and the cache should
        # have that many records as well
        end = arrow.get('2020-05-08').date()
        expected_head = tf.shift(end, -bars_to_fetch + 1, frame_type)
        self.assertEqual(tf.count_day_frames(expected_head, end), bars_to_fetch)
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, end, frame_type, bars_to_fetch, expected_head,
                                  bars_available)

        head = arrow.get('2020-01-23').date()
        tail = arrow.get('2020-04-08').date()
        # There're several records in the cache and all in sync scope. So after the
        # sync, the cache should have bars_to_fetch bars
        await security_cache.set_bars_range(code, frame_type, head, tail)
        await self.sync_and_check(code, end, frame_type, bars_to_fetch, expected_head,
                                  bars_available)

        # Day 1 sync
        # The cache already has bars_available records: [expected_head, ..., end]
        # After the sync, the cache should contains [expected_head, ..., end,
        # end + 1] bars in the cache
        end = arrow.get('2020-05-11').date()
        await self.sync_and_check(code, end, frame_type, bars_to_fetch, expected_head,
                                  bars_available + 1)

    @async_run
    async def test_001_sync_min30_bars(self):
        code = '000001.XSHE'
        frame_type = FrameType.MIN30
        bars_to_fetch = 200
        bars_available = bars_to_fetch

        # T0 sync
        # assume the cache is totally empty, then we'll fetch up to
        # cfg.omega.max_bars bars data, and the cache should
        # have that many records as well
        end = arrow.get('2020-04-30 15:00', tzinfo=cfg.tz)
        expected_head = tf.shift(end, -bars_to_fetch + 1, frame_type)
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, end, frame_type, bars_to_fetch, expected_head,
                                  bars_available)

        head = arrow.get('2020-03-26 11:00', tzinfo=cfg.tz)
        tail = arrow.get('2020-04-27 15:00', tzinfo=cfg.tz)
        await security_cache.set_bars_range(code, frame_type, head, tail)
        await self.sync_and_check(code, end, frame_type, bars_to_fetch, expected_head,
                                  bars_available)

        # T + 1 day sync
        end = arrow.get('2020-05-06 15:00', tzinfo=cfg.tz)
        await self.sync_and_check(code, end, frame_type, bars_to_fetch, expected_head,
                                  bars_available + 8)

    @async_run
    async def test_002_sync_week_bars(self):
        code = '000001.XSHE'
        frame_type = FrameType.WEEK

        # T0 sync
        # assume the cache is totally empty, there're 778 bars since 2005-01-07,
        # and valid bars for 000001.XSHE is 761
        end = arrow.get('2020-04-24').date()
        start = arrow.get('2005-01-07').date()
        bars_to_fetch = 778
        bars_available = 761

        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, end, frame_type, bars_to_fetch, start,
                                  bars_available)

        # T1 sync
        # Now assume we're at 2020-05-01, week bars of 20200430 is ready
        end = arrow.get('2020-04-30').date()
        start = arrow.get('2005-01-07').date()

        await self.sync_and_check(code, end, frame_type, bars_to_fetch, start,
                                  bars_available + 1)

    @async_run
    async def test_100_sync_calendar(self):
        await sc.sync_calendar()

        fut = Future()
        fut.set_result(None)
        with mock.patch('omega.fetcher.abstract_quotes_fetcher.AbstractQuotesFetcher'
                        '.get_all_trade_days', side_effects=[fut]):
            await sc.sync_calendar()

    @async_run
    async def test_003_init_sync_scope(self):
        await sq.init_sync_scope(['stock', 'index'], FrameType.DAY)


if __name__ == '__main__':
    unittest.main()
