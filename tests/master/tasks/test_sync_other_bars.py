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

import omega.worker.tasks.synctask as workjobs
from omega.core import constants
from omega.core.events import Events
from omega.master.dfs import Storage
from omega.master.tasks.sync_other_bars import (
    get_month_week_day_sync_date,
    sync_min_5_15_30_60,
    sync_month_bars,
    sync_week_bars,
)
from omega.master.tasks.synctask import BarsSyncTask
from omega.master.tasks.task_utils import get_bars_filename
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.tasks.task_utils import cache_init
from tests import assert_bars_equal, init_test_env, test_dir

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs_OtherBars(unittest.IsolatedAsyncioTestCase):
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

    async def test_get_month_week_day_sync_date(self):
        tail_key = "test_sync_tail"
        await cache.sys.delete(tail_key)
        with mock.patch("arrow.now", return_value=arrow.get("2005-01-06 02:05:00")):
            generator = get_month_week_day_sync_date(tail_key, FrameType.DAY)
            tail = await generator.__anext__()
            await cache.sys.set(tail_key, tail.strftime("%Y-%m-%d"))
            tail = await generator.__anext__()
            await cache.sys.set(tail_key, tail.strftime("%Y-%m-%d"))
            self.assertEqual(tail, datetime.date(2005, 1, 5))

        with mock.patch("arrow.now", return_value=arrow.get("2005-01-05 02:05:00")):
            generator = get_month_week_day_sync_date(tail_key, FrameType.DAY)
            try:
                await generator.__anext__()
            except Exception as e:
                self.assertIsInstance(e, StopAsyncIteration)
            else:
                self.assertEqual(1, 0)

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.sync_other_bars.get_month_week_day_sync_date")
    @mock.patch("omega.master.tasks.synctask.mail_notify")
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
        task = BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            name="week_test",
            frame_type=[FrameType.WEEK],
            end=end,
            timeout=30,
            recs_per_sec=2,
        )
        await task.cleanup(success=True)
        dfs = Storage()
        await dfs.delete(get_bars_filename(SecurityType.INDEX, end, FrameType.WEEK))
        await dfs.delete(get_bars_filename(SecurityType.STOCK, end, FrameType.WEEK))
        with mock.patch(
            "omega.master.tasks.sync_other_bars.BarsSyncTask",
            side_effect=[task],
        ):
            with mock.patch("arrow.now", return_value=end):
                await sync_week_bars()
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
                    filename = get_bars_filename(typ, end, ft)
                    data = await dfs.read(filename)
                    actual = pickle.loads(data)

                    self.assertSetEqual(set(actual.keys()), expected_keys[i])
                    for code in expected_keys[i]:
                        assert_bars_equal(actual[code], expected[code])

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.sync_other_bars.get_month_week_day_sync_date")
    @mock.patch("omega.master.tasks.synctask.mail_notify")
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
        task = BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            name="month_test",
            frame_type=[frame_type],
            end=end,
            timeout=30,
            recs_per_sec=2,
        )
        await task.cleanup(success=True)
        dfs = Storage()
        await dfs.delete(get_bars_filename(SecurityType.INDEX, end, FrameType.MONTH))
        await dfs.delete(get_bars_filename(SecurityType.STOCK, end, FrameType.MONTH))
        with mock.patch(
            "omega.master.tasks.sync_other_bars.BarsSyncTask",
            side_effect=[task],
        ):
            with mock.patch("arrow.now", return_value=end):
                await sync_month_bars()
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
                    filename = get_bars_filename(typ, end, ft)
                    data = await dfs.read(filename)
                    actual = pickle.loads(data)

                    self.assertSetEqual(set(actual.keys()), expected_keys[i])
                    for code in expected_keys[i]:
                        assert_bars_equal(expected[code], actual[code])

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.sync_other_bars.get_month_week_day_sync_date")
    @mock.patch("omega.master.tasks.synctask.mail_notify")
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
        task = BarsSyncTask(
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
            await dfs.delete(get_bars_filename(typ, end.naive, ft))

        await task.cleanup(success=True)
        await cache.sys.delete(constants.BAR_SYNC_OTHER_MIN_TAIL)
        with mock.patch(
            "omega.master.tasks.sync_other_bars.BarsSyncTask",
            side_effect=[task],
        ):
            with mock.patch("arrow.now", return_value=end):
                await sync_min_5_15_30_60()
                self.assertTrue(task.status)
                base_dir = os.path.join(
                    test_dir(), "data", "test_daily_calibration_sync"
                )
                for typ, ft in itertools.product(
                    [SecurityType.STOCK, SecurityType.INDEX],
                    frame_type,
                ):
                    # dfs读出来
                    filename = get_bars_filename(typ, end.naive, ft)
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
