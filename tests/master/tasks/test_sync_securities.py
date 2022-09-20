import datetime
import logging
import unittest
from unittest import mock

import arrow
import cfg4py
import omicron
from omicron.dal.cache import cache
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.security import Security
from pyemit import emit

import omega.worker.tasks.sec_synctask as workjobs
from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.sec_synctask import SecuritySyncTask
from omega.master.tasks.sync_securities import (
    get_security_sync_date,
    sync_securities_list,
)
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.tasks.task_utils import cache_init
from tests import init_test_env

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
        cfg = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        account = fetcher_info["account"]
        password = fetcher_info["password"]
        await aq.create_instance(impl, account=account, password=password)

    async def test_sync_secslist_date1(self, *args):
        # 测试非交易日
        end = arrow.get("2005-07-09")  # Saturday
        with mock.patch("arrow.now", return_value=end):
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_HEAD, "2005-07-07"),
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_TAIL, "2005-07-07"),
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
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_HEAD, "2005-07-01"),
            await cache.sys.set(constants.SECS_SYNC_ARCHIVE_TAIL, "2005-07-07"),
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

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.sec_synctask.mail_notify")
    async def test_sync_securities_list(self, mail_notify, *args):
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
    async def test_sync_securities_list_twice(self, mail_notify, *args):
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

        # sync_week_bars
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
        await task1.cleanup(success=True)
        await task2.cleanup(success=True)
        with mock.patch(
            "omega.master.tasks.sync_securities.SecuritySyncTask",
            side_effect=[task1, task2],
        ):
            with mock.patch(
                "omega.master.tasks.sync_securities.get_security_sync_date",
                side_effect=get_sync_date_mock,
            ):
                await sync_securities_list()
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
