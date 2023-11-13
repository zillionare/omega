import asyncio
import sys
import unittest
from unittest import mock

import cfg4py
import omicron

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.app import Omega, start
from tests import init_test_env


class WorkerAppTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def create_quotes_fetcher(self):
        impl = "tests.demo_fetcher"
        account = "account"
        password = "passwd"
        await aq.create_instance(impl, account=account, password=password)

    @mock.patch("asyncio.unix_events._UnixSelectorEventLoop.run_until_complete")
    @mock.patch("asyncio.unix_events._UnixSelectorEventLoop.run_forever")
    async def test_start(self, _run1, _run2):
        impl = "tests.demo_fetcher"
        account = "account"
        password = "passwd"

        _run1.return_value = None
        _run2.return_value = None
        rc = start(impl, cfg=None, account=account, password=password)
        self.assertIsNone(rc)

    @mock.patch("traceback.print_exc")
    @mock.patch("pyemit.emit.start")
    @mock.patch("apscheduler.schedulers.asyncio.AsyncIOScheduler.start")
    async def test_omega_init(self, _start, _start2, _p):
        impl = "tests.demo_fetcher"
        account = "account"
        password = "passwd"
        _p.return_value = None

        _start.return_value = None
        _start2.return_value = None
        omega = Omega(impl, cfg=None, account=account, password=password)
        rc = await omega.init()
        self.assertIsNone(rc)

        with mock.patch("omega.worker.app.omicron.init") as f:
            f.side_effect = [ValueError()]

            with mock.patch("os._exit") as f2:
                f2.return_value = None

                omega = Omega(impl, cfg=None, account=account, password=password)
                rc = await omega.init()
                self.assertIsNone(rc)
