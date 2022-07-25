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
from omicron.dal.cache import cache
from omicron.dal.influx.influxclient import InfluxClient
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame as tf
from pyemit import emit

import omega.worker.tasks.synctask as workjobs
from omega.core import constants
from omega.core.events import Events
from omega.master.dfs import Storage
from omega.master.tasks.sync_price_limit import (
    get_trade_limit_filename,
    sync_trade_price_limits,
)
from omega.master.tasks.synctask import BarsSyncTask
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.tasks.task_utils import cache_init
from tests import assert_bars_equal, init_test_env, test_dir

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
        cfg = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    @mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 500000, 1000000)),
    )
    @mock.patch("omega.master.tasks.synctask.BarsSyncTask.parse_bars_sync_scope")
    async def test_sync_trade_price_limits(self, parse_bars_scope, *args):
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
        dfs = Storage()
        await dfs.delete(get_trade_limit_filename(SecurityType.INDEX, end.naive))
        await dfs.delete(get_trade_limit_filename(SecurityType.STOCK, end.naive))
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
                # todo inflaxdb读出来看对不对
                base_dir = os.path.join(
                    test_dir(), "data", "test_sync_trade_price_limits"
                )
                # 从dfs查询 并对比
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

                # dfs读出来
                for typ in [SecurityType.STOCK, SecurityType.INDEX]:
                    filename = get_trade_limit_filename(typ, end.naive)
                    data = await dfs.read(filename)
                    with open(
                        os.path.join(base_dir, f"dfs_{typ.value}.pik"), "rb"
                    ) as f:
                        local_data = f.read()
                    self.assertEqual(data, local_data)
