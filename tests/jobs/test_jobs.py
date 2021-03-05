import asyncio
import datetime
import logging
import os
import shutil
import time
import unittest
from pathlib import Path
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
import rlog
from dateutil import tz
from omicron import cache
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.models.securities import Securities
from pyemit import emit

import omega.core.sanity
import omega.jobs
import omega.jobs.sync as sync
from omega.config.schema import Config
from omega.core.events import Events, ValidationError
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.jobs import receiver
from tests import init_test_env, start_omega

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class TestJobs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        init_test_env()

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)

        await self.create_quotes_fetcher()
        await omicron.init(aq)
        home = Path(cfg.omega.home).expanduser()
        os.makedirs(str(home / "data/chksum"), exist_ok=True)

        self.omega = await start_omega()

    async def asyncTearDown(self) -> None:
        await omicron.shutdown()
        if self.omega:
            self.omega.kill()
            # await omega exit
            time.sleep(1)

    async def create_quotes_fetcher(self):
        cfg: Config = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def sync_and_check(
        self, code, frame_type, start, stop, expected_head, expected_tail
    ):
        await sync.sync_bars_worker(
            {"start": start, "stop": stop, "frame_type": frame_type}, [code]
        )

        head, tail = await cache.get_bars_range(code, frame_type)

        n_bars = tf.count_frames(expected_head, expected_tail, frame_type)
        bars = await cache.get_bars(code, expected_tail, n_bars, frame_type)

        # 本单元测试的k线数据应该没有空洞。如果程序在首尾衔接处出现了问题，就会出现空洞，
        # 导致期望获得的数据与实际数据数量不相等。
        bars = list(filter(lambda x: not np.isnan(x["close"]), bars))

        self.assertEqual(n_bars, len(bars))
        self.assertEqual(expected_head, head)
        self.assertEqual(expected_tail, tail)

    async def test_000_sync_day_bars(self):
        code = "000001.XSHE"
        frame_type = FrameType.DAY

        # day 0 sync
        # assume the cache is totally empty, then we'll fetch up to
        # cfg.omega.max_bars bars data, and the cache should
        # have that many records as well
        start = arrow.get("2020-05-08").date()
        stop = arrow.get("2020-05-21").date()

        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, frame_type, start, stop, start, stop)

        # There're several records in the cache and all in sync scope. So after the
        # sync, the cache should have bars_to_fetch bars
        head = arrow.get("2020-05-11").date()
        tail = arrow.get("2020-05-20").date()
        await cache.set_bars_range(code, frame_type, head, tail)
        await self.sync_and_check(code, frame_type, start, stop, start, stop)

        # Day 1 sync
        # The cache already has bars_available records: [expected_head, ..., end]
        # After the sync, the cache should contains [expected_head, ..., end,
        # end + 1] bars in the cache
        stop = arrow.get("2020-05-22").date()
        await self.sync_and_check(code, frame_type, start, stop, start, stop)

    async def test_001_sync_min30_bars(self):
        code = "000001.XSHE"
        frame_type = FrameType.MIN30
        start = arrow.get("2020-05-08").date()
        stop = arrow.get("2020-05-21").date()

        # T0 sync
        # assume the cache is totally empty, then we'll fetch up to
        # cfg.omega.max_bars bars data, and the cache should
        # have that many records as well

        # if start, stop is specified as date, then it'll shift to the previous day
        exp_head = arrow.get(start).replace(hour=10, tzinfo=cfg.tz)
        exp_tail = arrow.get(stop).replace(hour=15, tzinfo=cfg.tz)

        await cache.security.delete(f"{code}:{frame_type.value}")
        logger.info("testing %s, %s, %s, %s", start, stop, exp_head, exp_tail)
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        start = arrow.get("2020-05-07 15:00", tzinfo=cfg.tz).datetime
        stop = arrow.get("2020-05-22 10:00", tzinfo=cfg.tz).datetime
        await cache.security.delete(f"{code}:{frame_type.value}")

        logger.info("testing %s, %s, %s, %s", start, stop, exp_head, exp_tail)
        await self.sync_and_check(code, frame_type, start, stop, start, stop)

        stop = arrow.get("2020-05-25 15:00", tzinfo=cfg.tz).datetime
        logger.info("testing %s, %s, %s, %s", start, stop, exp_head, exp_tail)
        await self.sync_and_check(code, frame_type, start, stop, start, stop)

    async def test_002_sync_week_bars(self):
        code = "000001.XSHE"
        frame_type = FrameType.WEEK
        start = arrow.get("2020-05-07").date()
        stop = arrow.get("2020-05-21").date()
        # T0 sync

        exp_head = arrow.get("2020-04-30").date()
        exp_tail = arrow.get("2020-05-15").date()
        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        # T1 sync
        start = arrow.get("2020-04-30").date()
        stop = arrow.get("2020-05-23").date()
        exp_head = arrow.get("2020-04-30").date()
        exp_tail = arrow.get("2020-05-22").date()

        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

    async def test_003_sync_min60_bars(self):
        code = "000001.XSHE"
        frame_type = FrameType.MIN60

        start = arrow.get("2020-05-08").date()
        stop = arrow.get("2020-05-21").date()
        tzinfo = tz.gettz(cfg.tz)
        exp_head = datetime.datetime(
            start.year, start.month, start.day, 10, 30, tzinfo=tzinfo
        )
        exp_tail = datetime.datetime(stop.year, stop.month, stop.day, 15, tzinfo=tzinfo)

        await cache.security.delete(f"{code}:{frame_type.value}")
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        head = arrow.get("2020-05-08 10:30", tzinfo=cfg.tz).datetime
        tail = arrow.get("2020-05-20 15:00", tzinfo=cfg.tz).datetime
        await cache.set_bars_range(code, frame_type, head, tail)

        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

        # T + 1 day sync
        stop = arrow.get("2020-05-22").date()
        exp_tail = arrow.get(stop).replace(hour=15, tzinfo=cfg.tz).datetime
        await self.sync_and_check(code, frame_type, start, stop, exp_head, exp_tail)

    async def test_100_sync_calendar(self):
        await omega.jobs.sync.sync_calendar()

        async_mock = mock.AsyncMock(return_value=None)
        with mock.patch(
            "omega.fetcher.abstract_quotes_fetcher.AbstractQuotesFetcher"
            ".get_all_trade_days",
            side_effect=async_mock,
        ):
            await omega.jobs.sync.sync_calendar()

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

    async def _test_sync_bars(self):
        # fixme: recover this test later
        config_items = [
            [
                {
                    "frame": "1d",
                    "start": "2020-01-01",
                    "delay": 3,
                    "type": [],
                    "include": "000001.XSHE,000004.XSHE",
                    "exclude": "000001.XSHG",
                }
            ]
        ]

        sync_request = []

        async def on_sync_bars(params: dict):
            sync_request.append(params)

        emit.register(Events.OMEGA_DO_SYNC, on_sync_bars)
        for config in config_items:
            cfg.omega.sync.bars = config
            for frame_config in config:
                frame_type = FrameType(frame_config.get("frame"))
                sync_params = frame_config
                await sync.trigger_bars_sync(frame_type, sync_params, force=True)

        await asyncio.sleep(0.2)
        self.assertDictEqual(
            {"start": "2020-01-01", "stop": None, "frame_type": FrameType.DAY},
            sync_request[0],
        )

    async def test_start_logging(self):
        # remove handlers set by config file, if there is.
        root = logging.getLogger()
        root.handlers.clear()

        fmt = "%(asctime)s %(levelname)-1.1s %(process)d %(name)s:%(funcName)s:%(lineno)s | %(message)s"
        channel = "test_start_logging"
        redis_logger = logging.getLogger("test_redis")
        handler = rlog.RedisHandler(
            channel=channel,
            level=logging.DEBUG,
            host="localhost",
            port="6379",
            formatter=logging.Formatter(fmt),
        )

        redis_logger.addHandler(handler)

        _dir = "/tmp/omega/test_jobs"
        shutil.rmtree(_dir, ignore_errors=True)
        cfg4py.update_config(
            {
                "logreceiver": {
                    "klass": "omega.logging.receiver.redis.RedisLogReceiver",
                    "dsn": "redis://localhost:6379",
                    "channel": channel,
                    "filename": "/tmp/omega/test_jobs/omega.log",
                    "backup_count": 2,
                    "max_bytes": "0.08K",
                }
            }
        )

        await omega.jobs.start_logging()
        for i in range(5):
            redis_logger.info("this is %sth test log", i)

        await asyncio.sleep(0.5)
        self.assertEqual(3, len(os.listdir(_dir)))
        with open(f"{_dir}/omega.log.2", "r", encoding="utf-8") as f:
            content = f.readlines()[0]
            msg = content.split("|")[1]
            self.assertEqual(" this is 2th test log\n", msg)
