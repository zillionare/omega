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
from coretypes import FrameType, SecurityType
from freezegun import freeze_time
from omicron.dal.cache import cache
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

import omega.worker.tasks.synctask as workjobs
from omega.core import constants
from omega.core.events import Events
from omega.master.dfs import Storage
from omega.master.tasks.calibration_task import (
    get_daily_bars_sync_task,
    get_sync_date,
    sync_daily_bars_1m,
    sync_daily_bars_day,
    sync_day_bar_factors,
)
from omega.master.tasks.synctask import BarsSyncTask
from omega.master.tasks.task_utils import get_bars_filename
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.worker.tasks.task_utils import cache_init
from tests import dir_test_home, init_test_env, mock_jq_data
from tests.demo_fetcher.demo_fetcher import DemoFetcher

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs_Calibration(unittest.IsolatedAsyncioTestCase):
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

        await Stock.reset_cache()
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

    async def test_get_sync_date(self):
        key_head = constants.BAR_SYNC_ARCHIVE_HEAD
        key_tail = constants.BAR_SYNC_ARCHIVE_TAIL
        await cache.sys.delete(constants.BAR_SYNC_ARCHIVE_HEAD)
        await cache.sys.delete(constants.BAR_SYNC_ARCHIVE_TAIL)
        with mock.patch("arrow.now", return_value=arrow.get("2022-02-18 15:05:00")):
            generator = get_sync_date(key_head, key_tail)
            sync_dt, head, tail = await generator.__anext__()
            print(sync_dt, head, tail)
            await cache.sys.set(
                constants.BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d")
            )
            await cache.sys.set(
                constants.BAR_SYNC_ARCHIVE_TAIL, tail.strftime("%Y-%m-%d")
            )

        with mock.patch("arrow.now", return_value=arrow.get("2022-02-22 02:05:00")):
            generator = get_sync_date(key_head, key_tail)
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
        with mock.patch("arrow.now", return_value=arrow.get("2005-01-04 02:05:00")):
            generator = get_sync_date(key_head, key_tail)
            try:
                await generator.__anext__()
            except Exception as e:
                self.assertIsInstance(e, StopAsyncIteration)
            else:
                self.assertEqual(1, 0)

    async def test_sync_date2(self):
        # tail和head未初始化的情况
        key_head = constants.BAR_SYNC_ARCHIVE_HEAD
        key_tail = constants.BAR_SYNC_ARCHIVE_TAIL

        with freeze_time("2022-09-08"):
            await cache.sys.set(constants.BAR_SYNC_ARCHIVE_HEAD, "2005-01-04")
            await cache.sys.set(constants.BAR_SYNC_ARCHIVE_TAIL, "2022-09-08")
            _tmp_var = None
            async for sync_dt, head, tail in get_sync_date(key_head, key_tail):
                _tmp_var = sync_dt
            self.assertIsNone(_tmp_var)

        await cache.sys.delete(constants.BAR_SYNC_ARCHIVE_HEAD)
        await cache.sys.delete(constants.BAR_SYNC_ARCHIVE_TAIL)

    async def test_sync_task_params(self):
        dt = datetime.datetime(2022, 9, 8, 16, 0, 0)
        rc = await get_daily_bars_sync_task(dt, FrameType.DAY)
        self.assertEqual(rc.recs_per_sec, 4)

    async def test_sync_task_1m(self):
        await cache.sys.set(constants.BAR_SYNC_ARCHIVE_HEAD, "2005-01-04")
        await cache.sys.set(constants.BAR_SYNC_ARCHIVE_TAIL, "2022-09-06")
        with freeze_time("2022-09-08 15:00:00"):
            with mock.patch(
                "omega.master.tasks.calibration_task.run_daily_bars_sync_task"
            ) as _run:
                _run.return_value = False
                rc = await sync_daily_bars_1m()
                self.assertIsNone(rc)

    async def test_sync_task_day(self):
        await cache.sys.set(constants.BAR_SYNC_DAY_HEAD, "2005-01-04")
        await cache.sys.set(constants.BAR_SYNC_DAY_TAIL, "2022-09-06")
        with freeze_time("2022-09-08 15:00:00"):
            with mock.patch(
                "omega.master.tasks.calibration_task.run_daily_bars_sync_task"
            ) as _run:
                _run.return_value = False
                rc = await sync_daily_bars_day()
                self.assertIsNone(rc)

                _run.return_value = True
                rc = await sync_daily_bars_day()
                self.assertIsNone(rc)

        await cache.sys.set(constants.BAR_SYNC_DAY_HEAD, "2005-01-05")
        await cache.sys.set(constants.BAR_SYNC_DAY_TAIL, "2022-09-07")
        with freeze_time("2022-09-08 15:00:00"):
            with mock.patch(
                "omega.master.tasks.calibration_task.run_daily_bars_sync_task"
            ) as _run:
                _run.return_value = True
                rc = await sync_daily_bars_day()
                self.assertIsNone(rc)

    @mock.patch("omega.master.tasks.calibration_task.ding")
    @mock.patch("omega.master.tasks.calibration_task.mail_notify")
    async def test_sync_task_day_factor(self, _notify, _ding):
        _notify.side_effect = [True]
        _ding.return_value = True

        with freeze_time("2022-09-04 15:00:00"):
            rc = await sync_day_bar_factors()
            self.assertFalse(rc)

        with freeze_time("2022-09-08 15:00:00"):
            with mock.patch(
                "omega.master.tasks.calibration_task.Security.get_xrxd_info"
            ) as _xrxd:
                _xrxd.return_value = False
                rc = await sync_day_bar_factors()
                self.assertFalse(rc)

                _xrxd.return_value = True
                with mock.patch(
                    "omega.master.tasks.calibration_task.run_daily_bars_sync_task"
                ) as _run:
                    _run.return_value = True
                    rc = await sync_day_bar_factors()
                    self.assertTrue(rc)

    @mock.patch("omega.master.tasks.calibration_task.Security.get_xrxd_info")
    @mock.patch("omega.master.tasks.calibration_task.run_daily_bars_sync_task")
    async def test_sync_task_day_factor2(self, _run, _xrxd):
        _xrxd.return_value = True
        _run.return_value = False

        with freeze_time("2022-09-08 15:00:00"):
            with mock.patch("omega.master.tasks.calibration_task.ding") as _ding:
                _ding.return_value = True
                with mock.patch(
                    "omega.master.tasks.calibration_task.mail_notify", return_value=True
                ):
                    rc = await sync_day_bar_factors()
                    self.assertIsNone(rc)

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.synctask.BarsSyncTask.parse_bars_sync_scope")
    @mock.patch("omega.master.tasks.calibration_task.get_sync_date")
    @mock.patch("tests.demo_fetcher.demo_fetcher.DemoFetcher.get_bars_batch")
    async def test_daily_bars_sync(
        self, _get_bars_batch, get_sync_date, parse_bars_scope, *args
    ):
        emit.register(
            Events.OMEGA_DO_SYNC_DAILY_CALIBRATION, workjobs.sync_daily_calibration
        )
        end = arrow.get("2022-02-23 15:00:00")

        async def get_sync_date_mock(*args, **kwargs):
            for item in [(end.naive, end.naive, end.naive)]:
                yield item

        get_sync_date.side_effect = get_sync_date_mock

        seclist1 = ["000001.XSHE", "300001.XSHE"]
        seclist2 = ["000001.XSHG"]
        parse_bars_scope.side_effect = [seclist1, seclist2]

        name = "calibration_sync"
        frame_type = [
            FrameType.MIN1,
        ]
        task = BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_DAILY_CALIBRATION,
            name=name,
            end=end.naive,
            frame_type=frame_type,  # 需要同步的类型
            timeout=60,
            recs_per_sec=240 + 4,
        )
        await task.cleanup(success=True)
        # 清除dfs数据
        dfs = Storage()
        for typ, ft in itertools.product(
            [SecurityType.STOCK, SecurityType.INDEX], frame_type
        ):
            await dfs.delete(get_bars_filename(typ, end.naive, ft))

        mock_data1 = mock_jq_data("000001_300001_0223_1m.pik")
        mock_data2 = mock_jq_data("000001_idx_0223_1m.pik")
        _get_bars_batch.side_effect = [mock_data1, mock_data2]

        with mock.patch(
            "omega.master.tasks.calibration_task.BarsSyncTask", side_effect=[task]
        ):
            with mock.patch("arrow.now", return_value=end):
                await sync_daily_bars_1m()
                base_dir_jq = os.path.join(dir_test_home(), "jq_data")
                base_dir_local = os.path.join(dir_test_home(), "local_data")
                for typ, ft in itertools.product(
                    [SecurityType.STOCK, SecurityType.INDEX],
                    frame_type,
                ):
                    # dfs读出来
                    filename = get_bars_filename(typ, end.naive, ft)
                    data = await dfs.read(filename)

                    _prefix = "000001_300001_0223"
                    if typ == SecurityType.INDEX:
                        _prefix = "000001_idx_0223"
                    with open(
                        os.path.join(base_dir_jq, f"{_prefix}_{ft.value}.pik"), "rb"
                    ) as f:
                        local_data = f.read()

                    self.assertEqual(data, local_data)
                for ft, n_bars in zip(frame_type, (240, 1)):
                    # 从dfs查询 并对比
                    influx_bars = {}
                    start = tf.shift(end, -n_bars + 1, ft)
                    if ft in tf.minute_level_frames:
                        batch_get_bars = Stock.batch_get_min_level_bars_in_range
                    else:
                        batch_get_bars = Stock.batch_get_day_level_bars_in_range

                    async for code, bars in batch_get_bars(
                        codes=["000001.XSHE", "300001.XSHE", "000001.XSHG"],
                        frame_type=ft,
                        start=start,
                        end=end.naive,
                    ):
                        influx_bars[code] = bars

                    influx_bars_dump = pickle.dumps(
                        influx_bars, protocol=cfg.pickle.ver
                    )

                    _prefix = "000001_300001_000001_0223"
                    with open(
                        os.path.join(base_dir_local, f"{_prefix}_{ft.value}_local.pik"),
                        "rb",
                    ) as f:
                        local_influx_bars = f.read()

                    self.assertEqual(influx_bars_dump, local_influx_bars)
