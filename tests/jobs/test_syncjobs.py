import asyncio
import datetime
import logging
import os
import time
import unittest
from pathlib import Path
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dateutil import tz
from omicron import cache
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.models.securities import Securities
from pyemit import emit

import omega.core.sanity
import omega.jobs
import omega.jobs.syncjobs as syncjobs
from omega.config.schema import Config
from omega.core.events import Events, ValidationError
from omega.fetcher import archive
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from tests import init_test_env, start_archive_server, start_omega

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class TestSyncJobs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        init_test_env()

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)

        await self.create_quotes_fetcher()
        await omicron.init(aq)

    async def asyncTearDown(self) -> None:
        await omicron.shutdown()
        await emit.stop()

    async def create_quotes_fetcher(self):
        cfg: Config = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def test_job_timer(self):
        await syncjobs._start_job_timer("unittest")
        await asyncio.sleep(5)
        elapsed = await syncjobs._stop_job_timer("unittest")
        self.assertTrue(5 <= elapsed <= 7)

    async def test_load_sync_params(self):
        expected = [
            {
                "frame": "1d",
                "start": "2020-01-02",
                "stop": "2020-01-03",
                "delay": 3,
                "cat": [],
                "include": "000001.XSHE",
                "exclude": "000001.XSHG",
            }
        ]
        try:
            origin = cfg.omega.sync.bars
            cfg.omega.sync.bars = expected
            actual = syncjobs.load_sync_params(FrameType.DAY)
            self.assertDictEqual(expected[0], actual)
        finally:
            cfg.omega.sync.bars = origin

    async def test_trigger_bars_sync(self):
        sync_params = {
            "frame": "1d",
            "start": "2020-01-01",
            "delay": 3,
            "cat": [],
            "include": "000001.XSHE 000004.XSHE",
            "exclude": "000001.XSHG",
        }

        sync_request = []

        async def on_sync_bars(params: dict):
            sync_request.append(params)

        emit.register(Events.OMEGA_DO_SYNC, on_sync_bars)
        with mock.patch("arrow.now", return_value=arrow.get("2020-1-6 10:00")):
            await syncjobs.trigger_bars_sync(sync_params, force=True)

        await asyncio.sleep(0.2)
        self.assertDictEqual(
            {
                "start": arrow.get("2019-12-31").date(),
                "stop": arrow.get("2020-1-3").date(),
                "frame_type": FrameType.DAY,
            },
            sync_request[0],
        )

        sync_request = []
        with mock.patch("arrow.now", return_value=arrow.get("2020-1-6 15:00")):
            await syncjobs.trigger_bars_sync(sync_params, force=True)

            await asyncio.sleep(0.2)
            self.assertDictEqual(
                {
                    "start": arrow.get("2019-12-31").date(),
                    "stop": arrow.get("2020-1-6").date(),
                    "frame_type": FrameType.DAY,
                },
                sync_request[0],
            )

    async def test_parse_sync_params(self):
        """
        2020年元旦前后交易日如下：
        20191224, 20191225, 20191226, 20191227, 20191230, 20191231,
        20200102, 20200103, 20200106, 20200107
        """
        sync_params = {
            "frame": "1d",
            "start": "2020-01-01",
            "delay": "3",
            "cat": ["stock"],
            "include": "000001.XSHE 000004.XSHE",
            "exclude": "000001.XSHE 000001.XSHG",
        }
        # 0. stop is None and current time is in opening
        with mock.patch(
            "arrow.now", return_value=arrow.get("2021-03-01 10:45", tzinfo=cfg.tz)
        ):
            codes, ft, start, stop, delay = syncjobs.parse_sync_params(**sync_params)
            self.assertTrue("000001.XSHE" in codes)
            self.assertEqual(FrameType.DAY, ft)
            self.assertEqual(start, datetime.date(2019, 12, 31))
            self.assertEqual(stop, datetime.date(2021, 2, 26))
            self.assertEqual(3, delay)

        # 1. stop is None, and current time is after closed
        with mock.patch(
            "arrow.now", return_value=arrow.get("2021-3-1 16:00", tzinfo=cfg.tz)
        ):
            codes, ft, start, stop, delay = syncjobs.parse_sync_params(**sync_params)
            self.assertEqual(FrameType.DAY, ft)
            self.assertEqual(start, datetime.date(2019, 12, 31))
            self.assertEqual(stop, datetime.date(2021, 3, 1))
            self.assertEqual(3, delay)

        # 2. MIN5 frame, start is type of `date` could start align to first_frame?
        sync_params["frame"] = "5m"
        codes, ft, start, stop, delay = syncjobs.parse_sync_params(**sync_params)
        self.assertEqual(FrameType.MIN5, ft)
        self.assertEqual(start, arrow.get("2019-12-31 09:35", tzinfo=cfg.tz))

        # 3. given start of type `datetime`, could it align to latest closed frame?
        sync_params["start"] = "2020-01-01 10:36"
        codes, ft, start, stop, delay = syncjobs.parse_sync_params(**sync_params)
        self.assertEqual(start, arrow.get("2019-12-31 15:00", tzinfo=cfg.tz))

        # 4. give stop of type `date`, could it align to last_frame?
        sync_params["stop"] = "2020-01-02"
        codes, ft, start, stop, delay = syncjobs.parse_sync_params(**sync_params)
        self.assertEqual(stop, arrow.get("2020-01-02 15:00", tzinfo=cfg.tz))

        # 5. both start and stop are None
        expected_stop = arrow.get("2020-1-2 10:35", tzinfo=cfg.tz)
        with mock.patch(
            "arrow.now", return_value=arrow.get("2020-1-2 10:36", tzinfo=cfg.tz)
        ):
            sync_params["stop"] = None
            sync_params["start"] = None
            codes, ft, start, stop, delay = syncjobs.parse_sync_params(**sync_params)
            self.assertEqual(expected_stop, stop)
            self.assertEqual(1000, tf.count_frames(start, expected_stop, ft))

    async def _cache_get_bars_all(self, code, frame_type):
        head, tail = await cache.get_bars_range(code, frame_type)

        n_bars = tf.count_frames(head, tail, frame_type)
        return await cache.get_bars(code, tail, n_bars, frame_type)

    async def _sync_and_check(
        self, code, frame_type, start, stop, exp_head=None, exp_tail=None
    ):
        stop = stop or arrow.now(cfg.tz).datetime

        exp_head = exp_head or start
        exp_tail = exp_tail or stop

        await syncjobs.sync_bars(
            {"frame_type": frame_type, "start": start, "stop": stop, "secs": [code]}
        )

        bars = await self._cache_get_bars_all(code, frame_type)

        # 本单元测试的k线数据应该没有空洞。如果程序在首尾衔接处出现了问题，就会出现空洞，
        # 导致期望获得的数据与实际数据数量不相等。
        exp_bars_len = tf.count_frames(exp_head, exp_tail, frame_type)
        bars = list(filter(lambda x: not np.isnan(x["close"]), bars))

        self.assertEqual(exp_bars_len, len(bars))
        self.assertEqual(exp_head, bars[0]["frame"])
        self.assertEqual(exp_tail, bars[-1]["frame"])

    async def test_sync_bars_001(self):
        """sync_bars for FrameType.MONTH

        frames:
        20190731, 20190830, 20190930, 20191031, 20191129, 20191231,
        20200123, 20200228, 20200331, 20200430, 20200529, 20200630,
        """
        sync_params = {
            "include": "000001.XSHE",
            "frame": FrameType.MONTH,
            "start": "2020-04-23",
            "stop": "2020-06-11",
        }

        secs, frame_type, start, stop, delay = syncjobs.parse_sync_params(**sync_params)
        code = secs[0]

        # 1. T0 sync
        exp_head = arrow.get("2020-03-31").date()
        exp_tail = arrow.get("2020-05-29").date()
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self._sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        # 2. sync one more month after T0
        sync_params["stop"] = "2020-7-1"
        *_, stop, _ = syncjobs.parse_sync_params(**sync_params)
        exp_tail = arrow.get("2020-06-30").date()
        await self._sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

    async def test_sync_bars_002(self):
        """sync_bars for FrameType.WEEK

        frames:
        20200403, 20200410, 20200417, 20200424, *20200430, 20200508,
        20200515*, 20200522, 20200529, 20200605, 20200612, 20200619,
        20200624, 20200703, 20200710, 20200717, 20200724, 20200731,
        """
        sync_params = {
            "include": "000001.XSHE",
            "frame": FrameType.WEEK,
            "start": "2020-04-30",
            "stop": "2020-05-21",
        }

        secs, frame_type, start, stop, delay = syncjobs.parse_sync_params(**sync_params)
        code = secs[0]

        # T0 sync
        exp_head = arrow.get("2020-04-30").date()
        exp_tail = arrow.get("2020-05-15").date()
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self._sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        # T1 sync
        sync_params["stop"] = "2020-05-23"
        *_, stop, _ = syncjobs.parse_sync_params(**sync_params)
        exp_head = arrow.get("2020-04-30").date()
        exp_tail = arrow.get("2020-05-22").date()

        await self._sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

    async def test_sync_bars_003(self):
        """sync_bars for FrameType.DAY

        trade days:
        20200430, 20200506, 20200507, 20200508, 20200511, 20200512,
        20200513, 20200514, 20200515, 20200518, 20200519, 20200520,
        20200521, 20200522, 20200525, 20200526, 20200527, 20200528,
        20200529, 20200601, 20200602, 20200603
        """
        sync_params = {
            "include": "000001.XSHE",
            "frame": FrameType.DAY,
            "start": "2020-05-08",
            "stop": "2020-05-21",
        }

        secs, frame_type, start, stop, _ = syncjobs.parse_sync_params(**sync_params)
        # 1. day 0 sync
        # assume the cache is totally empty,

        code = secs[0]
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self._sync_and_check(code, frame_type, start, stop)

        # 2. There're several records in the cache and all in sync scope. So after the
        # sync, the cache should have bars_to_fetch bars
        chaos_head = arrow.get("2020-05-11").date()
        chaos_tail = arrow.get("2020-05-20").date()
        await cache.set_bars_range(code, frame_type, chaos_head, chaos_tail)
        await self._sync_and_check(code, frame_type, start, stop)

        # 3. Day 1 sync
        # The cache already has bars_available records: [expected_head, ..., end]
        # After the sync, the cache should contains [expected_head, ..., end,
        # end + 1] bars in the cache
        stop = arrow.get("2020-05-22").date()
        await self._sync_and_check(code, frame_type, start, stop)

        # 4. if sync_start >> tail? Now tail is 2020-05-08
        start = arrow.get("2020-05-26").date()
        stop = arrow.get("2020-05-28").date()
        exp_head = arrow.get("2020-05-08").date()

        await self._sync_and_check(code, frame_type, start, stop, exp_head)

        # 5. sync to today, before close
        with mock.patch("arrow.now", return_value=arrow.get("2020-05-29 10:00")):
            sync_params["stop"] = None
            secs, frame_type, start, stop, _ = syncjobs.parse_sync_params(**sync_params)
            await self._sync_and_check(code, frame_type, start, stop)

        with mock.patch("arrow.now", return_value=arrow.get("2020-5-29 16:00")):
            sync_params["stop"] = None
            secs, frame_type, start, stop, _ = syncjobs.parse_sync_params(**sync_params)

    async def test_sync_bars_004(self):
        """sync_bars for FrameType.MIN60

        trade days:
        20200430, 20200506, 20200507, 20200508, 20200511, 20200512,
        """
        sync_params = {
            "include": "000001.XSHE",
            "frame": FrameType.MIN60,
            "start": "2020-04-30",
            "stop": "2020-05-07",
        }

        # T0 given sart is type of date, sync will start from first frame of that day
        secs, frame_type, start, stop, _ = syncjobs.parse_sync_params(**sync_params)
        code = secs[0]

        await cache.security.delete(f"{code}:{frame_type.value}")
        await self._sync_and_check(code, frame_type, start, stop)

        # set range as part of start-stop, see if sync work
        head = arrow.get("2020-05-06 10:30", tzinfo=cfg.tz).datetime
        tail = arrow.get("2020-05-07 14:00", tzinfo=cfg.tz).datetime
        await cache.set_bars_range(code, frame_type, head, tail)

        await self._sync_and_check(code, frame_type, start, stop)

        # T + 1 day sync
        sync_params["stop"] = "2020-05-08"
        exp_tail = arrow.get("2020-05-08 15:00", tzinfo=cfg.tz).datetime
        *_, stop, _ = syncjobs.parse_sync_params(**sync_params)

        await self._sync_and_check(code, frame_type, start, stop, start, exp_tail)

    async def test_sync_bars_005(self):
        """sync bars for FrameType.MIN30

        trade days:
        20200430, 20200506, 20200507, 20200508, 20200511, 20200512
        """

        sync_params = {
            "include": "000001.XSHE",
            "frame": FrameType.MIN30,
            "start": "2020-04-30 15:00",
            "stop": "2020-05-07 15:00",
        }

        secs, frame_type, start, stop, _ = syncjobs.parse_sync_params(**sync_params)
        code = secs[0]

        # 1. T0 sync
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self._sync_and_check(code, frame_type, start, stop)

        # 2. T1 sync
        sync_params["stop"] = "2020-05-08 10:00"
        _, _, start, stop, *_ = syncjobs.parse_sync_params(**sync_params)
        await cache.security.delete(f"{code}:{frame_type.value}")

        await self._sync_and_check(code, frame_type, start, stop)

        # 3. T2 sync
        sync_params["stop"] = "2020-05-08 10:00"
        _, _, start, stop, *_ = syncjobs.parse_sync_params(**sync_params)
        await self._sync_and_check(code, frame_type, start, stop)

    async def test_sync_bars_006(self):
        """sync bars after archive data imported

        trade_days:

        20190102, 20190103, 20190104, 20190107, 20190108, 20190109,
        20190110, 20190111, 20190114, 20190115, 20190116, 20190117,
        """
        try:
            archive_server = await start_archive_server()
            await cache.security.delete("000001.XSHE:1d")

            # start import archive and check result
            await archive._main([201901], ["stock"])

            # in archive._main, omicron.shutdown is called
            await omicron.init()

            sec = "000001.XSHE"
            head, tail = await cache.get_bars_range(sec, FrameType.DAY)
            self.assertEqual(datetime.date(2019, 1, 4), head)
            self.assertEqual(datetime.date(2019, 1, 4), tail)

            # sync start right after archive data
            sync_params = {
                "include": "000001.XSHE",
                "frame": FrameType.DAY,
                "start": "2019-01-05",
                "stop": "2019-01-10",
            }

            _, frame_type, start, stop, delay = syncjobs.parse_sync_params(
                **sync_params
            )

            exp_head = arrow.get("2019-01-04").date()
            await self._sync_and_check(sec, frame_type, start, stop, exp_head)

            # sync starts before archive data
            sync_params["start"] = "2019-01-02"
            sync_params["stop"] = "2019-01-11"
            _, _, start, stop, _ = syncjobs.parse_sync_params(**sync_params)
            exp_head = arrow.get("2019-01-02").date()
            await self._sync_and_check(sec, frame_type, start, stop, exp_head)

            # sync starts after archive data with gap
            start = arrow.get("2019-01-15").date()
            stop = arrow.get("2019-01-17").date()
            exp_head = arrow.get("2019-01-02").date()
            await self._sync_and_check(sec, frame_type, start, stop, exp_head)
        finally:
            if archive_server:
                archive_server.kill()

    async def test_sync_calendar(self):
        try:
            days, weeks, months = tf.day_frames, tf.week_frames, tf.month_frames
            tf.day_frames, tf.week_frames, tf.month_frames = None, None, None
            await omega.jobs.syncjobs.sync_calendar()

            self.assertIn(20200102, tf.day_frames)
            self.assertIn(20200403, tf.week_frames)
            self.assertIn(20200630, tf.month_frames)
        finally:
            tf.day_frames = days
            tf.week_frames = weeks
            tf.month_frames = months

        # if source server returns None, we still have old tf.*_frames
        async_mock = mock.AsyncMock(return_value=None)
        with mock.patch(
            "omega.fetcher.abstract_quotes_fetcher.AbstractQuotesFetcher"
            ".get_all_trade_days",
            side_effect=async_mock,
        ):
            await omega.jobs.syncjobs.sync_calendar()
            self.assertIn(20200102, tf.day_frames)
            self.assertIn(20200403, tf.week_frames)
            self.assertIn(20200630, tf.month_frames)

    async def test_load_bars_sync_jobs(self):
        origin = cfg.omega.sync.bars
        try:
            cfg.omega.sync.bars = [
                {
                    "frame": "1m",
                    "start": "2020-01-02",
                    "stop": "2020-01-02",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE",
                    "exclude": "000001.XSHG",
                },
                {
                    "frame": "5m",
                    "start": "2020-01-2",
                    "stop": "2020-01-03",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE",
                    "exclude": "000001.XSHG",
                },
                {
                    "frame": "15m",
                    "start": "2020-01-02",
                    "stop": "2020-01-03",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE",
                    "exclude": "000001.XSHG",
                },
                {
                    "frame": "30m",
                    "start": "2020-01-02",
                    "stop": "2020-01-03",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE",
                    "exclude": "000001.XSHG",
                },
                {
                    "frame": "60m",
                    "start": "2020-01-02",
                    "stop": "2020-01-03",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE 000004.XSHE",
                    "exclude": "000001.XSHG",
                },
                {
                    "frame": "1d",
                    "start": "2020-01-02",
                    "stop": "2020-01-03",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE",
                    "exclude": "000001.XSHG",
                },
                {
                    "frame": "1W",
                    "start": "2020-01-02",
                    "stop": "2020-01-03",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE",
                    "exclude": "000001.XSHG",
                },
                {
                    "frame": "1M",
                    "start": "2020-01-02",
                    "stop": "2020-01-03",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE",
                    "exclude": "000001.XSHG",
                },
            ]

            scheduler = AsyncIOScheduler(timezone=cfg.tz)
            syncjobs.load_bars_sync_jobs(scheduler)

            actual = set([job.name for job in scheduler.get_jobs()])
            expected = set(
                [
                    "1m:9:31-59",
                    "1m:10:*",
                    "1m:11:0-30",
                    "1m:13-14:*",
                    "1m:15:00",
                    "5m:9:35-55/5",
                    "5m:10:*/5",
                    "5m:11:0-30/5",
                    "5m:13-14:*/5",
                    "5m:15:00",
                    "15m:9:45",
                    "15m:10:*/5",
                    "15m:11:15,30",
                    "15m:13-14:*/15",
                    "15m:15:00",
                    "30m:10-11:*/30",
                    "30m:13:30",
                    "30m:14-15:*/30",
                    "60m:10:30",
                    "60m:11:30",
                    "60m:14-15:00",
                    "1d:15:00",
                    "1M:15:00",
                ]
            )
            self.assertSetEqual(expected, actual)
        finally:
            cfg.omega.sync.bars = origin

    async def _test_200_validation(self):
        # fixme: recover later. All validation/checksum related cases need to be redesigned.
        await self.prepare_checksum_data()

        errors = set()

        async def collect_error(report: tuple):
            if report[0] != ValidationError.UNKNOWN:
                errors.add(report)

        emit.register(Events.OMEGA_VALIDATION_ERROR, collect_error)

        codes = ["000001.XSHE"]
        await cache.sys.set("jobs.bars_validation.range.start", "20200511")
        await cache.sys.set("jobs.bars_validation.range.stop", "20200513")
        await omega.core.sanity.do_validation(codes, "20200511", "20200512")
        self.assertSetEqual({(0, 20200511, None, None, None, None)}, set(errors))

        mock_checksum = {
            "000001.XSHE": {
                "1d": "7918c38d",
                "1m": "15311c54",
                "5m": "54f9ac0a",
                "15m": "3c2cd435",
                "30m": "0cfbf775",
                "60m": "d21018b3",
            },
            "000001.XSHG": {
                "1d": "3a24582f",
                "1m": "d37d8742",
                "5m": "7bb329ad",
                "15m": "7c4e48a5",
                "30m": "e5db84ef",
                "60m": "af4be47d",
            },
        }

        await cache.sys.set("jobs.bars_validation.range.start", "20200511")
        await cache.sys.set("jobs.bars_validation.range.stop", "20200513")
        with mock.patch(
            "omega.core.sanity.calc_checksums",
            side_effect=[mock_checksum, mock_checksum],
        ):
            try:
                _tmp = tf.day_frames
                tf.day_frames = np.array([20200512])
                codes = ["000001.XSHE", "000001.XSHG"]
                errors = set()
                await omega.core.sanity.do_validation(codes, "20200511", "20200512")
                self.assertSetEqual(
                    {
                        (3, 20200512, "000001.XSHE", "1d", "7918c38d", "7918c38c"),
                        (3, 20200512, "000001.XSHG", "1m", "d37d8742", "d37d8741"),
                    },
                    errors,
                )
            finally:
                tf.day_frames = _tmp

    async def _test_201_start_job_validation(self):
        # fixme: recover this testcase later
        secs = Securities()
        with mock.patch.object(secs, "choose", return_value=["000001.XSHE"]):
            await omega.core.sanity.start_validation()

    async def prepare_checksum_data(self):
        end = datetime.datetime(2020, 5, 12, 15)

        await cache.sys.set("jobs.checksum.cursor", 20200511)

        for frame_type in tf.minute_level_frames:
            n = len(tf.ticks[frame_type])
            await cache.clear_bars_range("000001.XSHE", frame_type)
            await cache.clear_bars_range("000001.XSHG", frame_type)

            await aq.get_bars("000001.XSHE", end, n, frame_type)
            await aq.get_bars("000001.XSHG", end, n, frame_type)

        await aq.get_bars("000001.XSHE", end.date(), 1, FrameType.DAY)
        await aq.get_bars("000001.XSHG", end.date(), 1, FrameType.DAY)

        expected = {
            "000001.XSHE": {
                "1d": "7918c38c",
                "1m": "15311c54",
                "5m": "54f9ac0a",
                "15m": "3c2cd435",
                "30m": "0cfbf775",
                "60m": "d21018b3",
            },
            "000001.XSHG": {
                "1d": "3a24582f",
                "1m": "d37d8741",
                "5m": "7bb329ad",
                "15m": "7c4e48a5",
                "30m": "e5db84ef",
                "60m": "af4be47d",
            },
        }
        return end, expected

    async def test_sync_security_list(self):
        await cache.security.delete("securities")
        await syncjobs.sync_security_list()
        secs = await cache.get_securities()
        self.assertTrue(len(secs) > 0)
