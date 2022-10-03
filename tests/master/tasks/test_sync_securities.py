import datetime
import logging
import unittest
from unittest import mock

import arrow
import cfg4py
import omicron
from freezegun import freeze_time
from omicron.dal.cache import cache
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.security import Security
from pyemit import emit

import omega.worker.tasks.sec_synctask as workjobs
from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.sec_synctask import SecuritySyncTask
from omega.master.tasks.sync_securities import (
    delete_temporal_data,
    get_securities_dfs_filename,
    get_security_sync_date,
    get_security_sync_task,
    run_security_sync_task,
    sync_securities_list,
)
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.worker.tasks.task_utils import cache_init
from tests import init_test_env, mock_jq_data
from tests.demo_fetcher.demo_fetcher import DemoFetcher

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs_Securities(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
        await self.create_quotes_fetcher()

        # we need init omicron and cache in two steps, due to cache contains no data
        await omicron.cache.init()
        await cache_init()
        await omicron.init()

        # create influxdb client
        url, token, bucket, org = (
            cfg.influxdb.url,
            cfg.influxdb.token,
            cfg.influxdb.bucket_name,
            cfg.influxdb.org,
        )
        self.client = InfluxClient(url, token, bucket, org)
        await self.client.drop_measurement("security_list")
        await cache.sys.delete(constants.SECS_SYNC_ARCHIVE_HEAD)
        await cache.sys.delete(constants.SECS_SYNC_ARCHIVE_TAIL)

    async def asyncTearDown(self) -> None:
        await omicron.close()
        await emit.stop()

    async def create_quotes_fetcher(self):
        self.aq = AbstractQuotesFetcher()
        instance = DemoFetcher()
        self.aq._instances.append(instance)

    async def test_sync_secslist_date1(self, *args):
        # 测试非交易日
        end = arrow.get("2005-07-09")  # Saturday
        with mock.patch("arrow.now", return_value=end):
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_HEAD, "2005-07-07")
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_TAIL, "2005-07-07")
            async for sync_dt, head, tail in get_security_sync_date():
                self.assertEqual(sync_dt, datetime.datetime(2005, 7, 8, 0, 0))
                self.assertEqual(
                    await cache.sys.get(constants.SECS_SYNC_ARCHIVE_TAIL),
                    "2005-07-07",
                )
                break

    async def test_sync_secslist_date2(self, *args):
        # 测试交易日
        end = arrow.get("2005-07-12")  # Tuesday
        idx = 1
        with mock.patch("arrow.now", return_value=end):
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_HEAD, "2005-07-01")
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_TAIL, "2005-07-07")
            async for sync_dt, head, tail in get_security_sync_date():
                if idx == 1:
                    self.assertEqual(sync_dt, datetime.datetime(2005, 7, 8, 0, 0))
                    self.assertEqual(
                        await cache.sys.get(constants.SECS_SYNC_ARCHIVE_TAIL),
                        "2005-07-07",
                    )
                    await cache.sys.set(constants.SECS_SYNC_ARCHIVE_TAIL, "2005-07-08"),
                    idx += 1
                elif idx == 2:
                    self.assertEqual(sync_dt, datetime.datetime(2005, 7, 11, 0, 0))
                    self.assertEqual(
                        await cache.sys.get(constants.SECS_SYNC_ARCHIVE_TAIL),
                        "2005-07-08",
                    )
                    idx += 1
                else:
                    break

    async def test_sync_secslist_date3(self, *args):
        # tail和head未初始化的情况
        await cache.sys.delete(constants.SECS_SYNC_ARCHIVE_HEAD)
        await cache.sys.delete(constants.SECS_SYNC_ARCHIVE_TAIL)

        with freeze_time("2022-09-08"):
            with mock.patch(
                "omega.master.tasks.sync_securities.Security.get_datescope_from_db"
            ) as _scope:
                _scope.return_value = (None, None)
                async for sync_dt, head, tail in get_security_sync_date():
                    self.assertEqual(sync_dt, datetime.datetime(2022, 9, 7, 0, 0))
                    break

                dt1 = datetime.date(2005, 1, 4)
                dt2 = datetime.date(2022, 9, 5)
                _scope.return_value = (dt1, dt2)
                async for sync_dt, head, tail in get_security_sync_date():
                    self.assertEqual(sync_dt, datetime.datetime(2022, 9, 6, 0, 0))
                    break

            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_HEAD, "2005-01-05")
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_TAIL, "2022-09-08")
            async for sync_dt, head, tail in get_security_sync_date():
                self.assertEqual(sync_dt, datetime.datetime(2005, 1, 4, 0, 0))
                break

            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_HEAD, "2005-01-04")
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_TAIL, "2022-09-08")
            _tmp_var = None
            async for sync_dt, head, tail in get_security_sync_date():
                _tmp_var = sync_dt
            self.assertIsNone(_tmp_var)

    async def test_sync_secslist_dfs(self, *args):
        dt1 = datetime.date(2005, 1, 4)
        rc = get_securities_dfs_filename(dt1)
        self.assertEqual(rc, "securities/20050104")

        rc = await delete_temporal_data("xxxxxxx")
        self.assertIsNone(rc)

    async def test_sync_secslist_runtask_0(self):
        task = SecuritySyncTask("", "", None)
        with mock.patch(
            "omega.master.tasks.sync_securities.SecuritySyncTask.run"
        ) as _run:
            _run.return_value = False

            rc = await run_security_sync_task(task)
            self.assertFalse(rc)

    async def test_sync_secslist_runtask_1(self):
        dt = datetime.date(2009, 1, 1)
        rc = await get_security_sync_task(dt)
        self.assertEqual(rc.recs_per_task, 2000)

        dt = datetime.date(2013, 1, 1)
        rc = await get_security_sync_task(dt)
        self.assertEqual(rc.recs_per_task, 3500)

        dt = datetime.date(2016, 1, 1)
        rc = await get_security_sync_task(dt)
        self.assertEqual(rc.recs_per_task, 4500)

        dt = datetime.date(2019, 1, 1)
        rc = await get_security_sync_task(dt)
        self.assertEqual(rc.recs_per_task, 5500)

        dt = datetime.date(2021, 1, 1)
        rc = await get_security_sync_task(dt)
        self.assertEqual(rc.recs_per_task, 6500)

        dt = datetime.date(2022, 1, 1)
        rc = await get_security_sync_task(dt)
        self.assertEqual(rc.recs_per_task, 7500)

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.sec_synctask.mail_notify")
    @mock.patch("tests.demo_fetcher.demo_fetcher.DemoFetcher.get_security_list")
    async def test_sync_securities_list(self, _sec_list, mail_notify, *args):
        email_content = ""

        emit.register(Events.OMEGA_DO_SYNC_SECURITIES, workjobs.sync_security_list)
        end = arrow.get("2005-07-04")

        async def get_sync_date_mock(*args, **kwargs):
            for sync_date in [end.naive]:
                yield sync_date, sync_date, sync_date

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body
            print(body)

        mail_notify.side_effect = mail_notify_mock

        # sync_week_bars
        task = SecuritySyncTask(
            event=Events.OMEGA_DO_SYNC_SECURITIES,
            name="sync_securitylist_test",
            end=end.naive,
            timeout=60,
            recs_per_task=2000,
        )
        await task.cleanup(success=True)

        mock_data1 = mock_jq_data("securitylist_0223.pik")
        _sec_list.side_effect = [mock_data1]

        with mock.patch(
            "omega.master.tasks.sync_securities.SecuritySyncTask",
            side_effect=[task],
        ):
            with mock.patch(
                "omega.master.tasks.sync_securities.get_security_sync_date",
                side_effect=get_sync_date_mock,
            ):
                await sync_securities_list()
                self.assertTrue(task.status)
                self.assertEqual(
                    await cache.sys.get(constants.SECS_SYNC_ARCHIVE_TAIL),
                    "2005-07-04",
                )

                # read from influxdb
                secs = await Security.query_security_via_date(
                    code="000001.XSHE", date=end.naive.date()
                )
                self.assertGreater(len(secs), 5)

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.sec_synctask.mail_notify")
    @mock.patch("tests.demo_fetcher.demo_fetcher.DemoFetcher.get_security_list")
    async def test_sync_securities_list_twice(self, _sec_list, mail_notify, *args):
        email_content = ""

        emit.register(Events.OMEGA_DO_SYNC_SECURITIES, workjobs.sync_security_list)
        end1 = arrow.get("2005-07-07")
        end2 = arrow.get("2005-07-08")

        async def get_sync_date_mock(*args, **kwargs):
            for sync_date in [end1.naive, end2.naive]:
                yield sync_date, sync_date, sync_date

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body
            print(body)

        mail_notify.side_effect = mail_notify_mock

        # 构建两个任务
        task1 = SecuritySyncTask(
            event=Events.OMEGA_DO_SYNC_SECURITIES,
            name="sync_securitylist_test",
            end=end1.naive,
            timeout=60 * 5,
            recs_per_task=2000,
        )
        task2 = SecuritySyncTask(
            event=Events.OMEGA_DO_SYNC_SECURITIES,
            name="sync_securitylist_test",
            end=end2.naive,
            timeout=60 * 5,
            recs_per_task=2000,
        )
        # 清理任务
        await task1.cleanup(success=True)
        await task2.cleanup(success=True)

        # 准备数据
        mock_data1 = mock_jq_data("securitylist_0223.pik")
        _sec_list.side_effect = [mock_data1, mock_data1]

        with mock.patch(
            "omega.master.tasks.sync_securities.SecuritySyncTask",
            side_effect=[task1, task2],
        ):
            with mock.patch(
                "omega.master.tasks.sync_securities.get_security_sync_date",
                side_effect=get_sync_date_mock,
            ):
                await sync_securities_list()  # 执行两次任务
                self.assertTrue(task1.status)
                self.assertTrue(task2.status)
                self.assertEqual(
                    await cache.sys.get(constants.SECS_SYNC_ARCHIVE_TAIL),
                    "2005-07-08",
                )

                # read from influxdb
                secs = await Security.query_security_via_date(
                    code="000001.XSHE", date=end2.naive.date()
                )
                self.assertGreater(len(secs), 5)
