import asyncio
import datetime
import itertools
import logging
import os
import pickle
import unittest
from unittest import mock

import arrow
import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from coretypes import FrameType, SecurityType
from omicron.dal.cache import cache
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

import omega.master.jobs as syncjobs
from omega.core import constants
from omega.core.events import Events
from omega.master.dfs import Storage
from omega.worker import jobs as workjobs
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from tests import assert_bars_equal, init_test_env, test_dir

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
        await self.create_quotes_fetcher()

        # we need init omicron and cache in two steps, due to cache contains no data
        await omicron.cache.init()
        await workjobs.cache_init()
        await omicron.init()

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
        cfg = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    @mock.patch(
        "omega.master.jobs.BarsSyncTask.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.mail_notify")
    async def test_sync_minute_bars(self, mail_notify, *args):
        emit.register(Events.OMEGA_DO_SYNC_MIN, workjobs.sync_minute_bars)
        name = "minute"
        timeout = 60
        end = arrow.get("2022-02-18 09:31:00")
        await Stock.reset_cache()
        task = syncjobs.BarsSyncTask(
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
                await syncjobs.sync_minute_bars()
                self.assertTrue(task.status)
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_MINUTE_TAIL),
                    "2022-02-18 09:31:00",
                )
                base_dir = os.path.join(test_dir(), "data", "test_sync_minute_bars")
                bars = pickle.dumps(
                    await Stock.batch_get_bars(
                        codes=["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                        n=1,
                        frame_type=FrameType.MIN1,
                        end=end.naive,
                    ),
                    protocol=cfg.pickle.ver,
                )
                with open(os.path.join(base_dir, "min_data.pik"), "rb") as f:
                    self.assertEqual(bars, f.read())
            end = arrow.get("2022-02-18 09:32:00")
            task.status = None
            task.end = end.naive
            with mock.patch("arrow.now", return_value=end):
                # 第二次调用 redis有tail
                await syncjobs.sync_minute_bars()
                self.assertTrue(task.status)
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_MINUTE_TAIL),
                    "2022-02-18 09:32:00",
                )

    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch("omega.master.jobs.BarsSyncTask.get_quota", return_value=1000000)
    @mock.patch("omega.master.jobs.get_sync_date")
    async def test_daily_calibration_sync(self, get_sync_date, *args):
        emit.register(
            Events.OMEGA_DO_SYNC_DAILY_CALIBRATION, workjobs.sync_daily_calibration
        )
        end = arrow.get("2022-02-18 15:00:00")

        async def get_sync_date_mock(*args, **kwargs):
            for item in [(end.naive, end.naive, end.naive)]:
                yield item

        get_sync_date.side_effect = get_sync_date_mock
        name = "calibration_sync"
        frame_type = [
            FrameType.MIN1,
            # FrameType.MIN5,
            # FrameType.MIN15,
            # FrameType.MIN30,
            # FrameType.MIN60,
            FrameType.DAY,
        ]
        task = syncjobs.BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_DAILY_CALIBRATION,
            name=name,
            end=end.naive,
            frame_type=frame_type,  # 需要同步的类型
            timeout=60,
            recs_per_sec=int((240 * 2 + 4) // 0.75),
        )
        await task.cleanup(success=True)
        # 清楚dfs数据
        dfs = Storage()
        for typ, ft in itertools.product(
            [SecurityType.STOCK, SecurityType.INDEX], frame_type
        ):
            await dfs.delete(syncjobs.get_bars_filename(typ, end.naive, ft))

        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end.naive):
                await syncjobs.daily_calibration_job()
                base_dir = os.path.join(
                    test_dir(), "data", "test_daily_calibration_sync"
                )
                for typ, ft in itertools.product(
                    [SecurityType.STOCK, SecurityType.INDEX],
                    frame_type,
                ):
                    # dfs读出来
                    filename = syncjobs.get_bars_filename(typ, end.naive, ft)
                    data = await dfs.read(filename)

                    with open(
                        os.path.join(base_dir, f"dfs_{typ.value}_{ft.value}.pik"), "rb"
                    ) as f:
                        local_data = f.read()
                    self.assertEqual(data, local_data)
                for ft, n_bars in zip(
                    frame_type, (240, 240 // 5, 240 // 15, 240 // 30, 240 // 60, 1)
                ):
                    # 从dfs查询 并对比
                    influx_bars = await Stock.batch_get_bars(
                        codes=["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                        n=n_bars,
                        frame_type=ft,
                        end=end.naive,
                    )

                    influx_bars = pickle.dumps(influx_bars, protocol=cfg.pickle.ver)
                    with open(
                        os.path.join(base_dir, f"influx_{ft.value}.pik"), "rb"
                    ) as f:
                        local_influx_bars = f.read()
                    print(f"influx_{ft.value}.pik")
                    self.assertEqual(influx_bars, local_influx_bars)

    @mock.patch(
        "omega.master.jobs.BarsSyncTask.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch("omega.master.jobs.get_month_week_day_sync_date")
    async def test_sync_trade_price_limits(self, get_day_sync_date, *args):
        emit.register(
            Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, workjobs.sync_trade_price_limits
        )
        end = arrow.get("2022-02-18")

        async def get_week_sync_date_mock(*args, **kwargs):
            for sync_date in [end.naive]:
                yield sync_date

        get_day_sync_date.side_effect = get_week_sync_date_mock
        # sync_week_bars
        task = syncjobs.BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS,
            name="trader_limit_test",
            frame_type=[FrameType.DAY],
            end=end.naive,
            timeout=30,
            recs_per_sec=2,
        )
        await task.cleanup(success=True)
        dfs = Storage()
        await dfs.delete(
            syncjobs.get_trade_limit_filename(SecurityType.INDEX, end.naive)
        )
        await dfs.delete(
            syncjobs.get_trade_limit_filename(SecurityType.STOCK, end.naive)
        )
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            # with mock.patch("arrow.now", return_value=end):
            await syncjobs.sync_trade_price_limits()
            self.assertTrue(task.status)
            self.assertEqual(
                await cache.sys.get(constants.BAR_SYNC_TRADE_PRICE_TAIL), "2022-02-18"
            )
            # todo inflaxdb读出来看对不对
            base_dir = os.path.join(test_dir(), "data", "test_sync_trade_price_limits")
            # 从dfs查询 并对比
            bars = await Stock.get_trade_price_limits(
                code="000001.XSHE", begin=end.naive.date(), end=end.naive.date()
            )
            influx_bars = pickle.dumps(bars, protocol=cfg.pickle.ver)
            with open(os.path.join(base_dir, "influx.pik"), "rb") as f:
                self.assertEqual(influx_bars, f.read())

            # dfs读出来
            for typ in [SecurityType.STOCK, SecurityType.INDEX]:
                filename = syncjobs.get_trade_limit_filename(typ, end.naive)
                data = await dfs.read(filename)
                with open(os.path.join(base_dir, f"dfs_{typ.value}.pik"), "rb") as f:
                    local_data = f.read()
                self.assertEqual(data, local_data)

    @mock.patch(
        "omega.master.jobs.BarsSyncTask.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.get_month_week_day_sync_date")
    @mock.patch("omega.master.jobs.mail_notify")
    async def test_sync_week_bars(self, mail_notify, get_week_sync_date, *args):
        emit.register(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            workjobs.sync_year_quarter_month_week,
        )
        end = arrow.get("2022-02-18").date()

        async def get_week_sync_date_mock(*args, **kwargs):
            for sync_date in [end]:
                yield sync_date

        get_week_sync_date.side_effect = get_week_sync_date_mock

        # sync_week_bars
        task = syncjobs.BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            name="week_test",
            frame_type=[FrameType.WEEK],
            end=end,
            timeout=30,
            recs_per_sec=2,
        )
        await task.cleanup(success=True)
        dfs = Storage()
        await dfs.delete(
            syncjobs.get_bars_filename(SecurityType.INDEX, end, FrameType.WEEK)
        )
        await dfs.delete(
            syncjobs.get_bars_filename(SecurityType.STOCK, end, FrameType.WEEK)
        )
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.sync_week_bars()
                self.assertTrue(task.status)
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_WEEK_TAIL), "2022-02-18"
                )
                base_dir = os.path.join(test_dir(), "data", "test_sync_week_bars")

                # 从dfs查询 并对比
                actual = await Stock.batch_get_bars(
                    codes=["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                    n=1,
                    frame_type=FrameType.WEEK,
                    end=end,
                )
                expected = {}
                with open(os.path.join(base_dir, "influx_1w.pik"), "rb") as f:
                    expected = pickle.load(f)

                self.assertSetEqual(set(actual.keys()), set(expected.keys()))
                for code in actual.keys():
                    assert_bars_equal(actual[code], expected[code])

                expected_keys = [
                    set(["000001.XSHE", "300001.XSHE"]),
                    set(["000001.XSHG"]),
                ]

                for i, (typ, ft) in enumerate(
                    itertools.product(
                        [SecurityType.STOCK, SecurityType.INDEX], [FrameType.WEEK]
                    )
                ):
                    filename = syncjobs.get_bars_filename(typ, end, ft)
                    data = await dfs.read(filename)
                    actual = pickle.loads(data)

                    self.assertSetEqual(set(actual.keys()), expected_keys[i])
                    for code in expected_keys[i]:
                        assert_bars_equal(actual[code], expected[code])

    @mock.patch(
        "omega.master.jobs.BarsSyncTask.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.get_month_week_day_sync_date")
    @mock.patch("omega.master.jobs.mail_notify")
    async def test_sync_month_bars(self, mail_notify, get_week_sync_date, *args):
        emit.register(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            workjobs.sync_year_quarter_month_week,
        )

        # todo 测试同步月线 周线 写，minio inflaxdb 并读出来对比数据是否正确
        end = arrow.get("2022-01-28").date()

        async def get_week_sync_date_mock(*args, **kwargs):
            for sync_date in [end]:
                yield sync_date

        get_week_sync_date.side_effect = get_week_sync_date_mock
        frame_type = FrameType.MONTH
        # sync_week_bars
        task = syncjobs.BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            name="month_test",
            frame_type=[frame_type],
            end=end,
            timeout=30,
            recs_per_sec=2,
        )
        await task.cleanup(success=True)
        dfs = Storage()
        await dfs.delete(
            syncjobs.get_bars_filename(SecurityType.INDEX, end, FrameType.MONTH)
        )
        await dfs.delete(
            syncjobs.get_bars_filename(SecurityType.STOCK, end, FrameType.MONTH)
        )
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.sync_month_bars()
                self.assertTrue(task.status)
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_MONTH_TAIL), "2022-01-28"
                )
                base_dir = os.path.join(test_dir(), "data", "test_sync_month_bars")
                # 从dfs查询 并对比
                actual = await Stock.batch_get_bars(
                    codes=["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                    n=1,
                    frame_type=FrameType.MONTH,
                    end=end,
                )
                expected = {}
                with open(os.path.join(base_dir, "influx_1M.pik"), "rb") as f:
                    expected = pickle.load(f)

                self.assertSetEqual(set(expected.keys()), set(actual.keys()))
                for code in expected.keys():
                    assert_bars_equal(expected[code], actual[code])

                expected_keys = [
                    set(["000001.XSHE", "300001.XSHE"]),
                    set(["000001.XSHG"]),
                ]
                for i, (typ, ft) in enumerate(
                    itertools.product(
                        [SecurityType.STOCK, SecurityType.INDEX], [FrameType.MONTH]
                    )
                ):
                    filename = syncjobs.get_bars_filename(typ, end, ft)
                    data = await dfs.read(filename)
                    actual = pickle.loads(data)

                    self.assertSetEqual(set(actual.keys()), expected_keys[i])
                    for code in expected_keys[i]:
                        assert_bars_equal(expected[code], actual[code])

    async def test_load_cron_task(self):
        scheduler = AsyncIOScheduler(timezone=cfg.tz)

        await syncjobs.load_cron_task(scheduler)
        base = {
            "daily_calibration_sync",
            "1m:10:*",
            "1m:11:0-31",
            "1m:13-14:*",
            "sync_trade_price_limits",
            "1m:15:00",
            "after_hour_sync_job",
            "1m:9:31-59",
            "sync_month_bars",
            "sync_week_bars",
            "sync_min_5_15_30_60",
        }
        print(set([job.name for job in scheduler.get_jobs()]))
        self.assertSetEqual(base, set([job.name for job in scheduler.get_jobs()]))

    @mock.patch(
        "omega.master.jobs.BarsSyncTask.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.mail_notify")
    async def test_after_hour_sync_job(self, mail_notify, *args):
        email_content = ""

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body
            print(body)

        mail_notify.side_effect = mail_notify_mock
        emit.register(Events.OMEGA_DO_SYNC_DAY, workjobs.after_hour_sync)
        end = arrow.get("2022-02-23 15:05:00")
        task = syncjobs.BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_DAY,
            name="test",
            frame_type=[FrameType.MIN1, FrameType.DAY],
            end=end.naive,
            timeout=30,
            recs_per_sec=240 * 2 + 4,
        )
        await task.cleanup(success=True)
        await Stock.reset_cache()
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.after_hour_sync_job()
                self.assertTrue(task.status)
                # 将redis数据读出来，并序列化之后和准备的文件做对比
                influx_bars = await Stock.batch_get_bars(
                    codes=["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                    n=240,
                    frame_type=FrameType.MIN1,
                    end=end.naive.replace(minute=0),
                )
                bars1 = pickle.dumps(
                    influx_bars,
                    protocol=cfg.pickle.ver,
                )
                with open(
                    f"{test_dir()}/data/test_after_hour_sync_job/after_hour_sync_job.pik",
                    "rb",
                ) as f:
                    bars2 = f.read()
                self.assertEqual(bars1, bars2)

        # 测试数据为None 时邮件是否正常

        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch(
                "omega.worker.abstract_quotes_fetcher.AbstractQuotesFetcher.get_bars_batch",
                return_value=None,
            ):
                with mock.patch("arrow.now", return_value=end):
                    await syncjobs.after_hour_sync_job()
                    self.assertFalse(task.status)
                    self.assertIn("Got None Data", email_content)

        # 测试quota不够
        email_content = ""
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                task.timeout = 5.1
                await syncjobs.after_hour_sync_job()
                print(email_content)
                self.assertFalse(task.status)
                self.assertIn("超时", email_content)

        # 测试quota不够
        email_content = ""
        task.recs_per_sec = 1e9
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.after_hour_sync_job()
                self.assertFalse(task.status)
                self.assertIn("quota不足", email_content)

        # 测试重复运行
        await task.update_state(is_running=1, worker_count=0)
        task.status = None
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.after_hour_sync_job()
                self.assertFalse(task.status)

    async def test_get_sync_date(self):
        await cache.sys.delete(constants.BAR_SYNC_ARCHIVE_HEAD)
        await cache.sys.delete(constants.BAR_SYNC_ARCHIVE_TAIL)
        with mock.patch("arrow.now", return_value=arrow.get("2022-02-18 15:05:00")):
            generator = syncjobs.get_sync_date()
            sync_dt, head, tail = await generator.__anext__()
            print(sync_dt, head, tail)
            await cache.sys.set(
                constants.BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d")
            )
            await cache.sys.set(
                constants.BAR_SYNC_ARCHIVE_TAIL, tail.strftime("%Y-%m-%d")
            )

        with mock.patch("arrow.now", return_value=arrow.get("2022-02-22 02:05:00")):
            generator = syncjobs.get_sync_date()
            sync_dt, head, tail = await generator.__anext__()
            print(sync_dt, head, tail)
            self.assertIsNone(head)
            self.assertEqual(tail.strftime("%Y-%m-%d"), "2022-02-18")

            await cache.sys.set(
                constants.BAR_SYNC_ARCHIVE_TAIL, tail.strftime("%Y-%m-%d")
            )
            sync_dt, head, tail = await generator.__anext__()
            self.assertEqual(tail.strftime("%Y-%m-%d"), "2022-02-21")
            await cache.sys.set(
                constants.BAR_SYNC_ARCHIVE_TAIL, tail.strftime("%Y-%m-%d")
            )

            sync_dt, head, tail = await generator.__anext__()
            self.assertEqual(head.strftime("%Y-%m-%d"), "2022-02-16")

        await cache.sys.delete(constants.BAR_SYNC_ARCHIVE_HEAD)
        await cache.sys.delete(constants.BAR_SYNC_ARCHIVE_TAIL)
        with mock.patch("arrow.now", return_value=arrow.get("2005-01-05 02:05:00")):
            generator = syncjobs.get_sync_date()
            try:
                await generator.__anext__()
            except Exception as e:
                self.assertIsInstance(e, StopAsyncIteration)
            else:
                self.assertEqual(1, 0)

    async def test_get_month_week_day_sync_date(self):
        tail_key = "test_sync_tail"
        await cache.sys.delete(tail_key)
        with mock.patch("arrow.now", return_value=arrow.get("2005-01-05 02:05:00")):
            generator = syncjobs.get_month_week_day_sync_date(tail_key, FrameType.DAY)
            tail = await generator.__anext__()
            await cache.sys.set(tail_key, tail.strftime("%Y-%m-%d"))
            tail = await generator.__anext__()
            await cache.sys.set(tail_key, tail.strftime("%Y-%m-%d"))
            self.assertEqual(tail, datetime.date(2005, 1, 5))

        with mock.patch("arrow.now", return_value=arrow.get("2005-01-05 02:05:00")):
            generator = syncjobs.get_month_week_day_sync_date(tail_key, FrameType.DAY)
            try:
                await generator.__anext__()
            except Exception as e:
                self.assertIsInstance(e, StopAsyncIteration)
            else:
                self.assertEqual(1, 0)

    @mock.patch(
        "omega.master.jobs.BarsSyncTask.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.get_month_week_day_sync_date")
    @mock.patch("omega.master.jobs.mail_notify")
    async def test_sync_min_5_15_30_60(self, mail_notify, get_week_sync_date, *args):
        emit.register(
            Events.OMEGA_DO_SYNC_OTHER_MIN,
            workjobs.sync_min_5_15_30_60,
        )
        end = arrow.get("2022-02-18 15:00:00")

        async def get_week_sync_date_mock(*args, **kwargs):
            for sync_date in [end]:
                yield sync_date

        get_week_sync_date.side_effect = get_week_sync_date_mock
        frame_type = [
            FrameType.MIN5,
            FrameType.MIN15,
            FrameType.MIN30,
            FrameType.MIN60,
        ]
        task = syncjobs.BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_OTHER_MIN,
            name="min_5_15_30_60",
            frame_type=frame_type,
            end=end.naive,
            timeout=30,
            recs_per_sec=48 + 16 + 8 + 4,
        )

        dfs = Storage()

        for typ, ft in itertools.product(
            [SecurityType.STOCK, SecurityType.INDEX], frame_type
        ):
            await dfs.delete(syncjobs.get_bars_filename(typ, end.naive, ft))

        await task.cleanup(success=True)
        with mock.patch("omega.master.jobs.BarsSyncTask", side_effect=[task]):
            with mock.patch("arrow.now", return_value=end):
                await syncjobs.sync_min_5_15_30_60()
                self.assertTrue(task.status)
                base_dir = os.path.join(
                    test_dir(), "data", "test_daily_calibration_sync"
                )
                for typ, ft in itertools.product(
                    [SecurityType.STOCK, SecurityType.INDEX],
                    frame_type,
                ):
                    # dfs读出来
                    filename = syncjobs.get_bars_filename(typ, end.naive, ft)
                    data = await dfs.read(filename)

                    with open(
                        os.path.join(base_dir, f"dfs_{typ.value}_{ft.value}.pik"), "rb"
                    ) as f:
                        local_data = f.read()
                    self.assertEqual(data, local_data)
                for ft, n_bars in zip(
                    frame_type, (240, 240 // 5, 240 // 15, 240 // 30, 240 // 60, 1)
                ):
                    # 从dfs查询 并对比
                    influx_bars = await Stock.batch_get_bars(
                        codes=["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                        n=n_bars,
                        frame_type=ft,
                        end=end.naive,
                    )

                    influx_bars = pickle.dumps(influx_bars, protocol=cfg.pickle.ver)
                    with open(
                        os.path.join(base_dir, f"influx_{ft.value}.pik"), "rb"
                    ) as f:
                        local_influx_bars = f.read()
                    self.assertEqual(influx_bars, local_influx_bars)
