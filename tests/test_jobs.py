import logging
import os
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import arrow
import cfg4py
import numpy as np
import omicron
from omicron.core.events import Events
from omicron.core.lang import async_run
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.dal import cache
from omicron.dal import security_cache
from omicron.models.securities import Securities
from pyemit import emit

import omega.core.sanity
import omega.jobs
import omega.jobs.sync
import omega.jobs.sync as sq
from omega.config.cfg4py_auto_gen import Config
from omega.core import get_config_dir
from omega.core.events import ValidationError
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as fetcher
from omega.jobs import load_additional_jobs

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class MyTestCase(unittest.TestCase):
    @async_run
    async def setUp(self) -> None:
        os.environ[cfg4py.envar] = 'DEV'
        self.config_dir = get_config_dir()

        cfg4py.init(self.config_dir, False)

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
        await self.start_quotes_fetchers()

        await omicron.init(fetcher)
        home = Path(cfg.omega.home).expanduser()
        os.makedirs(str(home / "data/chksum"), exist_ok=True)

    async def start_quotes_fetchers(self):
        cfg: Config = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info['impl']
        params = fetcher_info['groups'][0]
        await fetcher.create_instance(impl, **params)

    def start_server(self):
        logger.info("starting omega quotes server")
        self.server_process = subprocess.Popen([sys.executable, '-m', 'omega.app',
                                                'start', 'jqadaptor'],
                                               env=os.environ)
        time.sleep(5)

    def stop_server(self) -> None:
        os.kill(self.server_process.pid, signal.SIGTERM)

    async def sync_and_check(self, code, frame_type, start, stop,
                             expected_head, expected_end):
        await sq.sync_bars_worker({frame_type.value: f"{start},{stop}"}, [code])

        head, tail = await security_cache.get_bars_range(code, frame_type)

        begin = tf.floor(arrow.get(start, tzinfo=cfg.tz), frame_type)
        end = sq._get_closed_frame(stop, frame_type)
        n_bars = tf.count_frames(begin, end, frame_type)
        bars = await security_cache.get_bars(code, end, n_bars, frame_type)
        bars = list(filter(lambda x: not np.isnan(x['close']), bars))

        # 进行单元测试的k线数据应该没有空洞。如果程序在首尾衔接处出现了问题，就会出现空洞，
        # 导致期望获得的数据与实际数据数量不相等。
        self.assertEqual(n_bars, len(bars))
        self.assertEqual(expected_head, head)
        self.assertEqual(expected_end, tail)

    @async_run
    async def test_000_sync_day_bars(self):
        code = '000001.XSHE'
        frame_type = FrameType.DAY

        # day 0 sync
        # assume the cache is totally empty, then we'll fetch up to
        # cfg.omega.max_bars bars data, and the cache should
        # have that many records as well
        start = '2020-05-08'
        stop = '2020-05-21'

        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, frame_type, start, stop, arrow.get(
                start).date(), arrow.get(stop).date())

        head = arrow.get('2020-05-11').date()
        tail = arrow.get('2020-05-20').date()
        # There're several records in the cache and all in sync scope. So after the
        # sync, the cache should have bars_to_fetch bars
        await security_cache.set_bars_range(code, frame_type, head, tail)
        await self.sync_and_check(code, frame_type, start, stop,
                                  arrow.get(start).date(),
                                  arrow.get(stop).date())

        # Day 1 sync
        # The cache already has bars_available records: [expected_head, ..., end]
        # After the sync, the cache should contains [expected_head, ..., end,
        # end + 1] bars in the cache
        stop = '2020-05-22'
        await self.sync_and_check(code, frame_type, start, stop, arrow.get(
                start).date(), arrow.get(stop).date())

    @async_run
    async def test_001_sync_min30_bars(self):
        code = '000001.XSHE'
        frame_type = FrameType.MIN30
        start = '2020-05-08'
        stop = '2020-05-21'

        # T0 sync
        # assume the cache is totally empty, then we'll fetch up to
        # cfg.omega.max_bars bars data, and the cache should
        # have that many records as well

        exp_head = arrow.get(start).replace(hour=10, tzinfo=cfg.tz).datetime
        exp_tail = arrow.get(stop).replace(hour=15, tzinfo=cfg.tz).datetime
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        head = arrow.get('2020-05-08 10:00', tzinfo=cfg.tz)
        tail = arrow.get('2020-05-20 15:00', tzinfo=cfg.tz)
        await security_cache.set_bars_range(code, frame_type, head, tail)

        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        # T + 1 day sync
        stop = '2020-05-22'
        exp_tail = arrow.get(stop).replace(hour=15, tzinfo=cfg.tz).datetime
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

    @async_run
    async def test_002_sync_week_bars(self):
        code = '000001.XSHE'
        frame_type = FrameType.WEEK
        start = '2020-05-07'
        stop = '2020-05-21'
        # T0 sync

        exp_head = arrow.get('2020-04-30').date()
        exp_tail = arrow.get('2020-05-15').date()
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        # T1 sync
        # Now assume we're at 2020-05-01, week bars of 20200430 is ready
        stop = '2020-05-22'
        exp_head = arrow.get('2020-04-30').date()
        exp_tail = arrow.get('2020-05-22').date()

        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

    @async_run
    async def test_003_sync_min60_bars(self):
        code = '000001.XSHE'
        frame_type = FrameType.MIN60

        start = '2020-05-08'
        stop = '2020-05-21'
        exp_head = arrow.get(start, tzinfo=cfg.tz).replace(hour=10, minute=30).datetime
        exp_tail = arrow.get(stop, tzinfo=cfg.tz).replace(hour=15).datetime

        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        head = arrow.get('2020-05-08 10:30', tzinfo=cfg.tz)
        tail = arrow.get('2020-05-20 15:00', tzinfo=cfg.tz)
        await security_cache.set_bars_range(code, frame_type, head, tail)

        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        # T + 1 day sync
        stop = '2020-05-22'
        exp_tail = arrow.get(stop).replace(hour=15, tzinfo=cfg.tz).datetime
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

    @async_run
    async def test_100_sync_calendar(self):
        await omega.jobs.sync.sync_calendar()

        with mock.patch('omega.fetcher.abstract_quotes_fetcher.AbstractQuotesFetcher'
                        '.get_all_trade_days', side_effect=[None]):
            await omega.jobs.sync.sync_calendar()

    @async_run
    async def test_200_validation(self):
        self.start_server()
        await self.prepare_checksum_data()

        errors = set()

        async def collect_error(report: tuple):
            if report[0] != ValidationError.UNKNOWN:
                errors.add(report)

        emit.register(Events.OMEGA_VALIDATION_ERROR, collect_error)

        codes = ['000001.XSHE']
        await cache.sys.set('jobs.bars_validation.range.start', '20200511')
        await cache.sys.set('jobs.bars_validation.range.stop', '20200513')
        await omega.core.sanity.do_validation(codes, '20200511', '20200512')
        self.assertSetEqual({(0, 20200511, None, None, None, None)}, set(errors))

        mock_checksum = {
            '000001.XSHE': {
                '1d':  '7918c38d',
                '1m':  '15311c54',
                '5m':  '54f9ac0a',
                '15m': '3c2cd435',
                '30m': '0cfbf775',
                '60m': 'd21018b3'
            },
            '000001.XSHG': {
                '1d':  '3a24582f',
                '1m':  'd37d8742',
                '5m':  '7bb329ad',
                '15m': '7c4e48a5',
                '30m': 'e5db84ef',
                '60m': 'af4be47d'
            }
        }

        await cache.sys.set('jobs.bars_validation.range.start', '20200511')
        await cache.sys.set('jobs.bars_validation.range.stop', '20200513')
        with mock.patch('omega.core.sanity.calc_checksums',
                        side_effect=[mock_checksum]):
            tf.day_frames = np.array([20200512])
            await omega.core.sanity.do_validation(codes, '20200511', '20200512')
            self.assertSetEqual({'20200512,000001.XSHE:1d,7918c38d,7918c38c',
                                 '20200512,000001.XSHG:1m,d37d8742,d37d8741'},
                                errors)

    @async_run
    async def test_201_start_job_validation(self):
        secs = Securities()
        with mock.patch.object(secs, 'choose', return_value=['000001.XSHE']):
            await omega.core.sanity.start_validation()

    async def prepare_checksum_data(self):
        end = arrow.get('2020-05-12 15:00')

        await cache.sys.set('jobs.checksum.cursor', 20200511)

        for frame_type in tf.minute_level_frames:
            n = len(tf.ticks[frame_type])
            await security_cache.clear_bars_range('000001.XSHE', frame_type)
            await security_cache.clear_bars_range('000001.XSHG', frame_type)

            await fetcher.get_bars('000001.XSHE', end, n, frame_type)
            await fetcher.get_bars('000001.XSHG', end, n, frame_type)

        await fetcher.get_bars('000001.XSHE', end.date(), 1, FrameType.DAY)
        await fetcher.get_bars('000001.XSHG', end.date(), 1, FrameType.DAY)

        expected = {
            '000001.XSHE': {
                '1d':  '7918c38c',
                '1m':  '15311c54',
                '5m':  '54f9ac0a',
                '15m': '3c2cd435',
                '30m': '0cfbf775',
                '60m': 'd21018b3'
            },
            '000001.XSHG': {
                '1d':  '3a24582f',
                '1m':  'd37d8741',
                '5m':  '7bb329ad',
                '15m': '7c4e48a5',
                '30m': 'e5db84ef',
                '60m': 'af4be47d'
            }
        }
        return end, expected

    @async_run
    async def test_get_checksum(self):
        end, expected = await self.prepare_checksum_data()

        save_to = (Path(cfg.omega.home) / "data/chksum").expanduser()
        chksum_file = os.path.join(save_to, "chksum-20200512.json")
        try:
            os.remove(chksum_file)
        except FileNotFoundError:
            pass

        # read from remote, and cache it
        actual = await omega.core.sanity.get_checksum(20200512)

        for code in ['000001.XSHE', '000001.XSHG']:
            self.assertDictEqual(expected.get(code), actual.get(code))

        # read from local cached file
        self.assertTrue(os.path.exists(chksum_file))
        actual = await omega.core.sanity.get_checksum(20200512)
        for code in ['000001.XSHE', '000001.XSHG']:
            self.assertDictEqual(expected.get(code), actual.get(code))

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

    @async_run
    async def test_load_additional_jobs(self):
        m = MagicMock()
        with mock.patch('omega.jobs.scheduler', m):
            load_additional_jobs()

        self.assertDictEqual(m.method_calls[0].kwargs['args'], {
            'save_to':    '~/.zillionare/omega/server_data/chksum',
            'start_date': '2020-01-01',
            'end_date':   'None'})

    @async_run
    async def test_quick_scan(self):
        await omega.core.sanity.quick_scan()


if __name__ == '__main__':
    unittest.main()
