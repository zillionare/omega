import asyncio
import logging
import unittest

import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker import jobs as worker_job
from omega.master import jobs as master_job
from tests import init_test_env
from apscheduler.job import Job

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
        cfg = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def test_job_timer(self):
        await master_job._start_job_timer("unittest")
        await asyncio.sleep(5)
        elapsed = await master_job._stop_job_timer("unittest")
        self.assertTrue(5 <= elapsed <= 7)

    async def test_load_cron_task(self):
        scheduler = AsyncIOScheduler(timezone=cfg.tz)

        await worker_job.load_cron_task(scheduler)
        base = {
            "sync_fund_net_value",
            "sync_funds",
            "sync_fund_share_daily",
            "sync_fund_portfolio_stock",
        }
        self.assertSetEqual(base, set([job.name for job in scheduler.get_jobs()]))
