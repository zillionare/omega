import functools
import logging
import os
import unittest
from asyncio import Future
from pathlib import Path
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
import sh
from omicron.core.events import Events
from omicron.core.lang import async_run
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.dal import cache
from omicron.dal import security_cache
from omicron.models.securities import Securities
from pyemit import emit

import omega.jobs.synccalendar as sc
import omega.jobs.syncquotes as sq
from omega.config.cfg4py_auto_gen import Config
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as fetcher

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class MyTestCase(unittest.TestCase):
    def get_config_path(self):
        src_dir = os.path.dirname(__file__)
        return os.path.join(src_dir, '../omega/config')

    @async_run
    async def setUp(self) -> None:
        os.environ[cfg4py.envar] = 'TEST'

        logger.info("starting solo quotes server...")
        cfg4py.init(self.get_config_path(), False)

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)

        await omicron.dal.cache.init(cfg)
        home = Path(cfg.omega.home).expanduser()
        os.makedirs(str(home / "data/chksum"), exist_ok=True)

        for fetcher_info in cfg.quotes_fetchers:
            module = fetcher_info.get('module')
            for params in fetcher_info.get('workers'):
                await fetcher.create_instance(module, **params)

    async def sync_and_check(self, code, end, frame_type, n_fetched, expected_head,
                             n_cached):
        fut1 = Future()
        fut1.set_result(code)

        fut2 = Future()
        fut2.set_result(None)

        with mock.patch('omicron.dal.cache.sys.lpop', side_effect=[fut1, fut2]):
            await sq.do_sync(sync_to=end)
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

        cfg.omega.sync.frames = [{'1d': bars_to_fetch}]
        # day 0 sync
        # assume the cache is totally empty, then we'll fetch up to
        # cfg.omega.max_bars bars data, and the cache should
        # have that many records as well
        end = arrow.get('2020-05-08').date()
        expected_head = tf.shift(end, -bars_to_fetch + 1, frame_type)
        self.assertEqual(tf.count_day_frames(expected_head, end), bars_to_fetch)
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, end, frame_type, bars_to_fetch,
                                  expected_head,
                                  bars_available)

        head = arrow.get('2020-01-23').date()
        tail = arrow.get('2020-04-08').date()
        # There're several records in the cache and all in sync scope. So after the
        # sync, the cache should have bars_to_fetch bars
        await security_cache.set_bars_range(code, frame_type, head, tail)
        await self.sync_and_check(code, end, frame_type, bars_to_fetch,
                                  expected_head,
                                  bars_available)

        # Day 1 sync
        # The cache already has bars_available records: [expected_head, ..., end]
        # After the sync, the cache should contains [expected_head, ..., end,
        # end + 1] bars in the cache
        end = arrow.get('2020-05-11').date()
        await self.sync_and_check(code, end, frame_type, bars_to_fetch,
                                  expected_head,
                                  bars_available + 1)

    @async_run
    async def test_001_sync_min30_bars(self):
        code = '000001.XSHE'
        frame_type = FrameType.MIN30
        bars_to_fetch = 200
        bars_available = bars_to_fetch
        cfg.omega.sync.frames = [{'30m': bars_to_fetch}]

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

        cfg.omega.sync.frames = [{'1w': bars_to_fetch}]

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

        with mock.patch('omega.fetcher.abstract_quotes_fetcher.AbstractQuotesFetcher'
                        '.get_all_trade_days', side_effect=[None]):
            await sc.sync_calendar()

    @async_run
    async def test_200_validation(self):
        await self.prepare_checksum_data()

        errors = set()

        def collect_error(errors: set, msg):
            errors.add(msg)

        emit.register(Events.OMEGA_VALIDATION_ERROR,
                      functools.partial(collect_error, errors))

        codes = ['000001.XSHE']
        await cache.sys.set('jobs.bars_validation.range.start', '20200511')
        await cache.sys.set('jobs.bars_validation.range.stop', '20200513')
        await sq.do_validation(codes, 0)
        self.assertSetEqual(set(), set(errors))

        mock_checksum = ['000001.XSHE,1d,7918c38d', '000001.XSHE,1m,15311c54',
                         '000001.XSHE,5m,54f9ac0a', '000001.XSHE,15m,3c2cd435',
                         '000001.XSHE,30m,0cfbf775', '000001.XSHE,60m,d21018b3',
                         '000001.XSHG,1d,3a24582f', '000001.XSHG,1m,d37d8742',
                         '000001.XSHG,5m,7bb329ad', '000001.XSHG,15m,7c4e48a5',
                         '000001.XSHG,30m,e5db84ef', '000001.XSHG,60m,af4be47d'
                         ]

        await cache.sys.set('jobs.bars_validation.range.start', '20200511')
        await cache.sys.set('jobs.bars_validation.range.stop', '20200513')
        with mock.patch('omega.jobs.syncquotes.calc_checksums',
                        side_effect=[mock_checksum]):
            tf.day_frames = np.array([20200512])
            await sq.do_validation(codes, 0)
            self.assertSetEqual({'20200512,000001.XSHE:1d,7918c38d,7918c38c',
                                 '20200512,000001.XSHG:1m,d37d8742,d37d8741'},
                                errors)

    @async_run
    async def test_201_start_job_validation(self):
        await cache.sys.set('jobs.bars_validation.range.start', '20200511')
        await cache.sys.set('jobs.bars_validation.range.stop', '20200513')
        secs = Securities()
        with mock.patch.object(secs, 'choose', return_value=['000001.XSHE']):
            await sq.start_validation()

    async def prepare_checksum_data(self):
        end = arrow.get('2020-05-12 15:00')

        await cache.sys.set('checksum.cursor', 20200511)

        for frame_type in tf.minute_level_frames:
            n = len(tf.ticks[frame_type])
            await security_cache.clear_bars_range('000001.XSHE', frame_type)
            await security_cache.clear_bars_range('000001.XSHG', frame_type)

            await fetcher.get_bars('000001.XSHE', end, n, frame_type)
            await fetcher.get_bars('000001.XSHG', end, n, frame_type)

        await fetcher.get_bars('000001.XSHE', end.date(), 1, FrameType.DAY)
        await fetcher.get_bars('000001.XSHG', end.date(), 1, FrameType.DAY)

        expected = {'000001.XSHE,1d,7918c38c', '000001.XSHE,1m,15311c54',
                    '000001.XSHE,5m,54f9ac0a', '000001.XSHE,15m,3c2cd435',
                    '000001.XSHE,30m,0cfbf775', '000001.XSHE,60m,d21018b3',
                    '000001.XSHG,1d,3a24582f', '000001.XSHG,1m,d37d8741',
                    '000001.XSHG,5m,7bb329ad', '000001.XSHG,15m,7c4e48a5',
                    '000001.XSHG,30m,e5db84ef', '000001.XSHG,60m,af4be47d'
                    }
        return end, expected

    @async_run
    async def test_get_checksum(self):
        # make sure there's chksum-20200512.csv exists
        end, expected = await self.prepare_checksum_data()
        secs = Securities()
        with mock.patch('arrow.now', return_value=end):
            with mock.patch.object(secs, 'choose',
                                   side_effect=[['000001.XSHE', '000001.XSHG']]):
                try:
                    with mock.patch('sh.git.add', return_value=True):
                        await sq.do_checksum()
                except sh.ErrorReturnCode_1 as e:
                    if "nothing to commit" in e.stdout.decode('utf-8'):
                        self.assertTrue(True)
                    else:
                        logger.exception(e)
                except Exception as e:
                    logger.exception(e)

        # read from local file
        actual = await sq.get_checksum(20200512)

        self.assertSetEqual(expected, set(actual))

        save_to = os.path.expanduser(cfg.omega.chksum.folder)
        chksum_file = os.path.join(save_to, "chksum-20200512.csv")
        sh.rm(chksum_file)

        # read from repo, this time, we can only test if special entries exist
        actual = await sq.get_checksum(20200512)
        self.assertIn('000001.XSHG,1d,3a24582f', actual)

    @async_run
    async def test_memory(self):
        await cache.security.flushall()
        await fetcher.get_bars('000001.XSHE', arrow.now(), 1000,
                               FrameType.DAY)
        data = await cache.security.hgetall('000001.XSHE:1d')
        for i in range(5000):
            await cache.security.hmset_dict(i, data)
            if (i + 1) % 500 == 0:
                print(i)


if __name__ == '__main__':
    unittest.main()
