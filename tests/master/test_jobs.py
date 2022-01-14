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
import omicron
from omicron import cache
from pyemit import emit

import omega.master.jobs as syncjobs
from omega.core.events import Events
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker import jobs as workjobs
from tests import init_test_env

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
        await self.create_quotes_fetcher()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()
        await emit.stop()

    async def create_quotes_fetcher(self):
        cfg = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def test_job_timer(self):
        await syncjobs._start_job_timer("unittest")
        await asyncio.sleep(5)
        elapsed = await syncjobs._stop_job_timer("unittest")
        self.assertTrue(5 <= elapsed <= 7)

    async def test_sync_security_list(self):
        await cache.security.delete("securities")
        await syncjobs.sync_security_list()
        secs = await cache.get_securities()
        self.assertTrue(len(secs) > 0)

    @mock.patch("omega.master.jobs.get_now")
    async def test_sync_minute_bars(self, get_now):
        get_now.return_value = datetime.datetime(2022, 1, 11, 9, 32)
        emit.register(Events.OMEGA_DO_SYNC_MIN, workjobs.sync_minute_bars)
        await syncjobs.sync_minute_bars()

    @mock.patch("omega.master.jobs.get_now")
    async def test_sync_day_bars(self, get_now):
        get_now.return_value = datetime.datetime(2022, 1, 11, 16)
        emit.register(Events.OMEGA_DO_SYNC_DAY, workjobs.sync_day_bars)
        await syncjobs.sync_day_bars()

    @mock.patch("omega.master.jobs.get_now")
    async def test_daily_calibration_sync(self, get_now):
        await cache.sys.delete("master.task.daily_calibration.state")
        get_now.return_value = datetime.datetime(2022, 1, 11, 16)
        emit.register(
            Events.OMEGA_DO_SYNC_DAILY_CALIBRATION, workjobs.sync_daily_calibration
        )
        await syncjobs.daily_calibration_sync()

    @mock.patch("omega.master.jobs.get_now")
    async def test_sync_high_low_limit(self, get_now):
        get_now.return_value = datetime.datetime(2022, 1, 11, 16)
        emit.register(Events.OMEGA_DO_SYNC_HIGH_LOW_LIMIT, workjobs.sync_high_low_limit)
        await syncjobs.sync_high_low_limit()

    @mock.patch("omega.master.jobs.get_now")
    async def test_sync_year_quarter_month_week(self, get_now):
        get_now.return_value = datetime.datetime(2022, 1, 11, 16)
        emit.register(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            workjobs.sync_year_quarter_month_week,
        )
        await syncjobs.sync_year_quarter_month_week()
