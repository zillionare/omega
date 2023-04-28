import datetime
import logging
import unittest
from unittest import mock

import cfg4py
import omicron
from freezegun import freeze_time

from omega.master.tasks.sync_xr_xd_reports import (
    get_xrxd_sync_task,
    run_xrxd_sync_task,
    sync_all_xrxd_reports,
    sync_xrxd_reports,
)
from omega.worker.tasks.task_utils import cache_init
from tests import init_test_env

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestMasterTasks_XRXDReports(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        # we need init omicron and cache in two steps, due to cache contains no data
        await omicron.cache.init()
        await cache_init()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def test_run_task(self):
        task = await get_xrxd_sync_task(datetime.datetime(2022, 9, 8, 10, 0))

        with mock.patch("omega.master.tasks.sec_synctask.SecuritySyncTask.run") as _run:
            _run.return_value = True
            rc = await run_xrxd_sync_task(task)
            self.assertTrue(rc)

            _run.return_value = False
            rc = await run_xrxd_sync_task(task)
            self.assertFalse(rc)

            with freeze_time("2022-09-03 10:00:00"):
                rc = await sync_xrxd_reports()
                self.assertTrue(rc)

            with freeze_time("2022-09-02 10:00:00"):
                _run.return_value = True
                rc = await sync_xrxd_reports()
                self.assertTrue(rc)

                _run.return_value = False
                rc = await sync_xrxd_reports()
                self.assertFalse(rc)

    async def test_sync_all_task(self):
        with mock.patch("omega.master.tasks.sec_synctask.SecuritySyncTask.run") as _run:
            _run.return_value = True
            rc = await sync_all_xrxd_reports()
            self.assertTrue(rc)

            _run.return_value = False
            rc = await sync_all_xrxd_reports()
            self.assertTrue(rc)
