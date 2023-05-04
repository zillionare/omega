import asyncio
import datetime
import itertools
import logging
import os
import pickle
import unittest
from typing import Dict
from unittest import mock

import arrow
import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from coretypes import FrameType, SecurityType
from freezegun import freeze_time
from omicron.dal.cache import cache
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

import omega.master.jobs as syncjobs
import omega.worker.tasks.synctask as workjobs
from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.synctask import BarsSyncTask
from omega.scripts import load_lua_script
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.worker.tasks.task_utils import cache_init
from tests import dir_test_home, init_test_env, mock_jq_data
from tests.demo_fetcher.demo_fetcher import DemoFetcher

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
        await self.create_quotes_fetcher()

        # we need init omicron and cache in two steps, due to cache contains no data
        await omicron.cache.init()
        await cache_init()
        await omicron.init()
        await load_lua_script()

        # create influxdb client
        url, token, bucket, org = (
            cfg.influxdb.url,
            cfg.influxdb.token,
            cfg.influxdb.bucket_name,
            cfg.influxdb.org,
        )
        self.client = InfluxClient(url, token, bucket, org)

        for ft in itertools.chain(tf.day_level_frames, tf.minute_level_frames):
            name = f"stock_bars_{ft.value}"
            await self.client.drop_measurement(name)

    async def asyncTearDown(self) -> None:
        await omicron.close()
        await emit.stop()

    async def create_quotes_fetcher(self):
        self.aq = AbstractQuotesFetcher()
        instance = DemoFetcher()
        self.aq._instances.append(instance)

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.synctask.BarsSyncTask.parse_bars_sync_scope")
    @mock.patch("omega.worker.tasks.fetchers.fetcher.get_bars_batch")
    async def test_sync_minute_bars(self, _fetch_bars, parse_bars_scope, *args):
        emit.register(Events.OMEGA_DO_SYNC_MIN, workjobs.sync_minute_bars)
        name = "minute"
        timeout = 60
        end = arrow.get("2022-02-18 09:31:00")

        seclist1 = ["000001.XSHE", "300001.XSHE"]
        seclist2 = ["000001.XSHG"]
        # 两次调用任务
        parse_bars_scope.side_effect = [seclist1, seclist2, seclist1, seclist2]

        await Stock.reset_cache()
        task = BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_MIN,
            name=name,
            frame_type=[FrameType.MIN1],
            end=end.naive,
            n_bars=1,
            timeout=timeout,
        )
        await task.cleanup(success=True)
        await cache.sys.delete(constants.BAR_SYNC_MINUTE_TAIL)

        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task, task]):
            with mock.patch("arrow.now", return_value=end):
                mock_data1 = mock_jq_data("000001_300001_0218_0931_n1.pik")
                mock_data2 = mock_jq_data("000001_idx_0218_0931_n1.pik")
                _fetch_bars.side_effect = [mock_data1, mock_data2]

                await syncjobs.sync_minute_bars()
                self.assertTrue(task.status)
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_MINUTE_TAIL),
                    "2022-02-18 09:31:00",
                )

                base_dir = os.path.join(
                    dir_test_home(), "data", "test_sync_minute_bars"
                )
                start = datetime.datetime(2022, 2, 18, 9, 31)
                actual = {}
                with freeze_time("2022-02-18 09:31:00"):
                    async for code, bar in Stock.batch_get_min_level_bars_in_range(
                        ["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                        FrameType.MIN1,
                        start,
                        end.naive,
                    ):
                        actual[code] = bar

                with open(os.path.join(base_dir, "min_data.pik"), "rb") as f:
                    exp = pickle.load(f)
                    self.assertSetEqual(set(actual.keys()), set(exp.keys()))
                    # fixme: add bars equality

            end = arrow.get("2022-02-18 09:32:00")
            task.status = None
            task.end = end.naive
            with mock.patch("arrow.now", return_value=end):
                # 第二次调用 redis有tail
                mock_data1 = mock_jq_data("000001_300001_0218_0931_n1.pik")
                mock_data2 = mock_jq_data("000001_idx_0218_0931_n1.pik")
                _fetch_bars.side_effect = [mock_data1, mock_data2]

                await syncjobs.sync_minute_bars()
                self.assertTrue(task.status)
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_MINUTE_TAIL),
                    "2022-02-18 09:32:00",
                )

    async def test_load_cron_task(self):
        scheduler = AsyncIOScheduler(timezone=cfg.tz)

        await syncjobs.load_cron_task(scheduler)
        base = {
            "daily_bars_sync",
            "1m:10:*",
            "1m:11:0-30",
            "1m:13:1-59",
            "1m:14:*",
            "sync_trade_price_limits",
            "1m:15:00",
            "after_hour_sync_job",
            "1m:9:31-59",
            "sync_month_bars",
            "sync_week_bars",
            "sync_min_5_15_30_60",
            "sync_securities",
            "sync_xrxd",
            "day_sync_task",
            "day_factor_fix_task",
            "sync_cache_price_limits",
            "reload_calendar",
        }
        print(set([job.name for job in scheduler.get_jobs()]))
        self.assertSetEqual(base, set([job.name for job in scheduler.get_jobs()]))

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.synctask.mail_notify")
    @mock.patch("omega.master.tasks.synctask.BarsSyncTask.parse_bars_sync_scope")
    @mock.patch("omega.worker.tasks.fetchers.fetcher.get_bars_batch")
    async def test_after_hour_sync_job(
        self, _fetch_bars, parse_bars_scope, mail_notify, *args
    ):
        email_content = ""

        seclist1 = ["000001.XSHE", "300001.XSHE"]
        seclist2 = ["000001.XSHG"]
        parse_bars_scope.side_effect = [
            seclist1,
            seclist2,
            seclist1,
            seclist2,
            seclist1,
            seclist2,
            seclist1,
            seclist2,
        ]

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body
            print(body)

        mail_notify.side_effect = mail_notify_mock
        emit.register(Events.OMEGA_DO_SYNC_DAY, workjobs.after_hour_sync)
        end = arrow.get("2022-02-23 15:05:00")
        task = BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_DAY,
            name="after_hour_sync",
            frame_type=[FrameType.MIN1, FrameType.DAY],
            end=end.naive,
            timeout=30,
            recs_per_sec=240 + 4,
        )
        await task.cleanup(success=True)
        await Stock.reset_cache()

        mock_data1 = mock_jq_data("000001_300001_0223_1m.pik")
        mock_data2 = mock_jq_data("000001_300001_0223_1d.pik")
        mock_data3 = mock_jq_data("000001_idx_0223_1m.pik")
        mock_data4 = mock_jq_data("000001_idx_0223_1d.pik")
        _fetch_bars.side_effect = [mock_data1, mock_data2, mock_data3, mock_data4]

        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.after_hour_sync_job()
                self.assertTrue(task.status)
                # 将redis数据读出来，并序列化之后和准备的文件做对比
                start = tf.combine_time(end, 9, 31)
                influx_bars = {}
                with freeze_time("2022-02-23 15:05:00"):
                    async for code, bars in Stock.batch_get_min_level_bars_in_range(
                        codes=["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                        frame_type=FrameType.MIN1,
                        start=start,
                        end=end.naive.replace(minute=0),
                    ):
                        influx_bars[code] = bars

                bars1 = pickle.dumps(
                    influx_bars,
                    protocol=cfg.pickle.ver,
                )
                with open(
                    f"{dir_test_home()}/data/test_after_hour_sync_job/after_hour_sync_job.pik",
                    "rb",
                ) as f:
                    bars2 = f.read()
                self.assertEqual(bars1, bars2)

        # 测试数据为None 时邮件是否正常
        print("测试数据为None 时邮件是否正常")
        email_content = ""
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            _fetch_bars.side_effect = [None, None, None, None]
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.after_hour_sync_job()
                self.assertFalse(task.status)
                self.assertIn("Got None Data", email_content)

        # 测试超时
        async def _sync_cache(typ: SecurityType, ft: FrameType, params: Dict):
            await asyncio.sleep(3)

        print("测试超时")
        email_content = ""
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            _fetch_bars.side_effect = [mock_data1, mock_data2, mock_data3, mock_data4]
            with mock.patch("arrow.now", return_value=end):
                task.timeout = 0
                with mock.patch(
                    "omega.worker.tasks.synctask.sync_to_cache", side_effect=_sync_cache
                ):
                    await syncjobs.after_hour_sync_job()
                print(email_content)
                self.assertFalse(task.status)
                self.assertIn("timeout", email_content)

        # 测试重复运行
        print("测试重复运行")
        await task.is_running()
        task.status = None
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            _fetch_bars.side_effect = [mock_data1, mock_data2, mock_data3, mock_data4]
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.after_hour_sync_job()
                self.assertFalse(task.status)
                await task.delete_state()

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((False, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.synctask.mail_notify")
    @mock.patch("omega.master.tasks.synctask.BarsSyncTask.parse_bars_sync_scope")
    async def test_quota_case1(self, parse_bars_scope, mail_notify, *args):
        email_content = ""

        seclist1 = ["000001.XSHE", "300001.XSHE"]
        seclist2 = ["000001.XSHG"]
        parse_bars_scope.side_effect = [seclist1, seclist2]

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body
            print(body)

        mail_notify.side_effect = mail_notify_mock
        emit.register(Events.OMEGA_DO_SYNC_DAY, workjobs.after_hour_sync)
        end = arrow.get("2022-02-23 15:05:00")
        task = BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_DAY,
            name="test_quota",
            frame_type=[FrameType.MIN1, FrameType.DAY],
            end=end.naive,
            timeout=30,
            recs_per_sec=240 + 4,
        )

        # 测试quota不够
        email_content = ""
        task.recs_per_sec = 1e9
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.after_hour_sync_job()
                self.assertFalse(task.status)
                self.assertIn("quota insufficient", email_content)
