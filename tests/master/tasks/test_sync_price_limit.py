import datetime
import itertools
import logging
import os
import unittest
from unittest import mock

import arrow
import cfg4py
import numpy as np
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
from omega.master.tasks.sync_price_limit import (
    get_trade_price_limits_sync_date,
    run_sync_price_limits_task,
    sync_cache_price_limits,
    sync_trade_price_limits,
)
from omega.master.tasks.synctask import BarsSyncTask
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.worker.tasks.task_utils import cache_init
from tests import dir_test_home, init_test_env, mock_jq_data
from tests.demo_fetcher.demo_fetcher import DemoFetcher

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs_PriceLimit(unittest.IsolatedAsyncioTestCase):
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

        await Stock.reset_cache()
        await self.client.delete_bucket()
        await self.client.create_bucket()

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
    @mock.patch("tests.demo_fetcher.demo_fetcher.DemoFetcher.get_trade_price_limits")
    async def test_sync_trade_price_limits(self, _fetch_bars, parse_bars_scope, *args):
        emit.register(
            Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, workjobs.sync_trade_price_limits
        )
        end = arrow.get("2022-02-18")

        seclist1 = ["000001.XSHE", "300001.XSHE"]
        seclist2 = ["000001.XSHG"]
        parse_bars_scope.side_effect = [seclist1, seclist2]

        async def get_week_sync_date_mock(*args, **kwargs):
            for sync_date in [end.naive]:
                yield sync_date

        # sync_week_bars
        task = BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS,
            name="trader_limit_test",
            frame_type=[FrameType.DAY],
            end=end.naive,
            timeout=30,
            recs_per_sec=2,
        )
        await task.cleanup(success=True)

        mock_data1 = mock_jq_data("000001_300001_0218_limit.pik")
        mock_data2 = mock_jq_data("000001_idx_0218_limit.pik")
        _fetch_bars.side_effect = [mock_data1, mock_data2]

        with mock.patch(
            "omega.master.tasks.sync_other_bars.BarsSyncTask",
            side_effect=[task],
        ):
            with mock.patch(
                "omega.master.tasks.sync_price_limit.get_trade_price_limits_sync_date",
                side_effect=get_week_sync_date_mock,
            ):
                await sync_trade_price_limits()
                self.assertTrue(task.status)
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_TRADE_PRICE_TAIL),
                    "2022-02-18",
                )

                # 从inflaxdb读出来看对不对
                actual = await Stock.get_trade_price_limits(
                    code="000001.XSHE", begin=end.naive.date(), end=end.naive.date()
                )

                exp = np.array(
                    [(datetime.date(2022, 2, 18), 18.06, 14.78)],
                    dtype=[("frame", "O"), ("high_limit", "<f4"), ("low_limit", "<f4")],
                )
                np.testing.assert_array_equal(actual["frame"], exp["frame"])
                np.testing.assert_array_almost_equal(
                    actual["high_limit"], exp["high_limit"], decimal=2
                )
                np.testing.assert_array_almost_equal(
                    actual["low_limit"], exp["low_limit"], decimal=2
                )

    async def test_price_limit_task(self):
        task = BarsSyncTask(
            "", "taskname", end=None, frame_type=[FrameType.DAY, FrameType.WEEK]
        )
        with mock.patch("omega.master.tasks.sync_price_limit.BarsSyncTask.run") as _run:
            _run.return_value = False

            rc = await run_sync_price_limits_task(task, False)
            self.assertFalse(rc)

    async def test_price_limit_sync_date(self):
        key = constants.BAR_SYNC_TRADE_PRICE_TAIL
        await cache.sys.delete(key)
        with freeze_time("2022-09-08 10:00:00"):
            async for sync_dt in get_trade_price_limits_sync_date(key, FrameType.DAY):
                self.assertEqual(sync_dt, datetime.date(2005, 1, 4))
                break

        with freeze_time("2022-09-03 10:00:00"):  # weekend
            async for sync_dt in get_trade_price_limits_sync_date(key, FrameType.DAY):
                self.assertEqual(sync_dt, datetime.date(2005, 1, 4))
                break

        await cache.sys.set(key, "2022-09-07")
        with freeze_time("2022-09-08 10:00:00"):  # 间隔两天
            _tmp_dt = None
            async for sync_dt in get_trade_price_limits_sync_date(key, FrameType.DAY):
                _tmp_dt = sync_dt
            self.assertIsNone(_tmp_dt)

        await cache.sys.set(key, "2022-09-05")
        with freeze_time("2022-09-03 10:00:00"):
            _tmp_dt = None
            async for sync_dt in get_trade_price_limits_sync_date(key, FrameType.DAY):
                _tmp_dt = sync_dt
            self.assertIsNone(_tmp_dt)

    @mock.patch("omega.master.tasks.sync_price_limit.run_sync_price_limits_task")
    async def test_price_limit_task_2(self, _run):
        _run.return_value = False

        key = constants.BAR_SYNC_TRADE_PRICE_TAIL
        await cache.sys.delete(key)
        with freeze_time("2022-09-08 10:00:00"):
            rc = await sync_trade_price_limits()
            self.assertIsNone(rc)

    @mock.patch("omega.master.tasks.sync_price_limit.run_sync_price_limits_task")
    async def test_price_limit_cache_task(self, _run):
        _run.return_value = True

        with freeze_time("2022-09-08 09:06:00"):
            rc = await sync_cache_price_limits()
            self.assertTrue(rc)

        with freeze_time("2022-09-08 09:17:00"):
            rc = await sync_cache_price_limits()
            self.assertTrue(rc)
