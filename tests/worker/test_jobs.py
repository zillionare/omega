import logging
import unittest
from unittest import mock

import cfg4py
import numpy as np
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from omega.worker import jobs as worker_job
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher
from tests import dir_test_home, init_test_env
from tests.demo_fetcher.demo_fetcher import DemoFetcher

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await self.create_quotes_fetcher()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def create_quotes_fetcher(self):
        self.aq = AbstractQuotesFetcher()
        instance = DemoFetcher()
        self.aq._instances.append(instance)

    async def test_load_cron_task(self):
        scheduler = AsyncIOScheduler(timezone=cfg.tz)

        await worker_job.load_cron_task(scheduler)
        print(set([job.name for job in scheduler.get_jobs()]))
        base = {
            "sync_calendar",
            "sync_seclist_today",
        }
        self.assertSetEqual(base, set([job.name for job in scheduler.get_jobs()]))

    @mock.patch("omega.master.tasks.synctask.mail_notify")
    @mock.patch("omega.master.jobs.tf.save_calendar")
    @mock.patch("jqadaptor.fetcher.Fetcher.get_all_trade_days")
    async def test_sync_calendar(self, get_all_trade_days, *args):
        # all_trade_days.npy
        async def get_all_trade_days_mock():
            return np.load(
                f"{dir_test_home()}/data/all_trade_days.npy", allow_pickle=True
            )

        get_all_trade_days.side_effect = get_all_trade_days_mock
        await worker_job.sync_calendar()
