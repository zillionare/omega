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
from omicron import cache
from omicron.models.calendar import Calendar as cal
from omicron.core.types import FrameType
from omicron.models.stock import Stock
from pyemit import emit

import omega.core.sanity
import omega.master
import omega.master.jobs as syncjobs
from omega.core.constants import get_queue_name
from omega.core.events import Events, ValidationError
from omega.worker import archive
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.jobs import sync_consumer
from tests import init_test_env, start_archive_server

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

    async def test_sync_minute_bars(self):
        # await cache.sys.hset("master.bars_sync.state.minute", "is_running", 0)
        suffix = "minute"
        state, done, scope = get_queue_name(suffix)

        await omicron.cache.sys.hdel(state, "is_running")
        await omicron.cache.sys.hdel(state, "done_count")
        emit.register(Events.OMEGA_DO_SYNC_MIN, sync_consumer)
        await syncjobs.sync_minute_bars()

    async def test_sync_day_bars(self):
        suffix = "day"

        state, done, scope = get_queue_name(suffix)

        await omicron.cache.sys.hdel(state, "is_running")
        await omicron.cache.sys.hdel(state, "done_count")
        emit.register(Events.OMEGA_DO_SYNC_MIN, sync_consumer)
        await syncjobs.sync_day_bars()

    async def test_daily_calibration_sync(self):
        suffix = "daily_calibration"
        state, done, scope = get_queue_name(suffix)
        await omicron.cache.sys.hdel(state, "is_running")
        await omicron.cache.sys.hdel(state, "done_count")
        emit.register(Events.OMEGA_DO_SYNC_MIN, sync_consumer)
        await syncjobs.daily_calibration_sync()

    async def test_sync_high_low_limit(self):
        suffix = "high_low_limit"
        state, done, scope = get_queue_name(suffix)
        await omicron.cache.sys.hdel(state, "is_running")
        await omicron.cache.sys.hdel(state, "done_count")
        emit.register(Events.OMEGA_DO_SYNC_MIN, sync_consumer)
        await syncjobs.sync_high_low_limit()
