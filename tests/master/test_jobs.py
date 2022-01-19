import asyncio
import datetime
import logging
import unittest
from unittest import mock
import numpy as np
import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron.dal.cache import cache
from pyemit import emit
import pickle
import omega.master.jobs as syncjobs
from omega.master.app import handle_work_heart_beat
from omega.core import constants
from omega.core.events import Events
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker import jobs as workjobs
from omega.worker.dfs import TempStorage
from tests import init_test_env
from omega.core.constants import HIGH_LOW_LIMIT
from zillionare_core_types.core.types import FrameType
from omicron.models.stock import Stock
from tests import test_dir

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestSyncJobs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
        await self.create_quotes_fetcher()
        await omicron.cache.init()
        await workjobs.cache_init()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()
        await emit.stop()

    async def create_quotes_fetcher(self):
        cfg = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def test_job_timer(self):
        await syncjobs._start_job_timer("unittest")
        await asyncio.sleep(5)
        elapsed = await syncjobs._stop_job_timer("unittest")
        self.assertTrue(5 <= elapsed <= 7)

    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch("omega.master.jobs.TimeFrame.save_calendar")
    # @mock.patch("omicron.dal.cache.save_calendar")
    @mock.patch("jqadaptor.fetcher.Fetcher.get_all_trade_days")
    async def test_sync_calendar(self, get_all_trade_days, *args):
        # all_trade_days.npy
        async def get_all_trade_days_mock():
            print("=====")
            return np.load(f"{test_dir()}/data/all_trade_days.npy", allow_pickle=True)

        get_all_trade_days.side_effect = get_all_trade_days_mock
        await syncjobs.sync_calendar()

    @mock.patch("omega.master.jobs.mail_notify")
    async def test_sync_security_list(self, *args):
        await cache.security.delete("securities")
        await syncjobs.sync_security_list()
        secs = Stock.choose(["stock"])
        self.assertTrue(len(secs) > 0)

    @mock.patch("omega.master.jobs.get_timeout", return_value=10)
    @mock.patch("omicron.models.stock.Stock.batch_cache_bars")
    @mock.patch(
        "omega.master.jobs.Task.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch(
        "omega.master.jobs.get_now", return_value=datetime.datetime(2022, 1, 11, 16)
    )
    @mock.patch(
        "omega.worker.abstract_quotes_fetcher.AbstractQuotesFetcher.get_bars_batch"
    )
    async def test_sync_minute_bars(self, get_bars_batch, get_now, mail_notify, *args):
        async def clear():
            state = f"{constants.TASK_PREFIX}.minute.state"
            await cache.sys.delete(state)
            await cache.sys.delete(constants.BAR_SYNC_STATE_MINUTE)

        await clear()

        async def get_bars_batch_mock(*args, **kwargs):
            """聚宽的数据被序列化到文件里了，mock读出来  根据帧类型mock"""
            frame_type = kwargs["frame_type"]
            with open(f"{test_dir()}/data/{frame_type.value}.pick", "rb") as f:
                return pickle.loads(f.read())

        get_bars_batch.side_effect = get_bars_batch_mock
        await cache.sys.hset(
            constants.BAR_SYNC_STATE_MINUTE, "tail", "2022-01-10 15:00:00"
        )

        emit.register(Events.OMEGA_DO_SYNC_MIN, workjobs.sync_minute_bars)
        ret = await syncjobs.sync_minute_bars()
        self.assertTrue(ret)
        self.assertEqual(
            await cache.sys.hget(constants.BAR_SYNC_STATE_MINUTE, "tail"),
            "2022-01-11 15:00:00",
        )
        await clear()

        # 非交易日
        await clear()
        get_now.return_value = datetime.datetime(2022, 1, 9)
        ret = await syncjobs.sync_minute_bars()
        self.assertFalse(ret)

        # 中午11点40 执行时
        # 非交易时间 重置到11点30
        await clear()
        get_now.return_value = datetime.datetime(2022, 1, 11, 11, 40)
        ret = await syncjobs.sync_minute_bars()
        self.assertTrue(ret)
        self.assertEqual(
            await cache.sys.hget(constants.BAR_SYNC_STATE_MINUTE, "tail"),
            "2022-01-11 11:30:00",
        )

        await clear()
        # 测试数据为None
        get_bars_batch.side_effect = None
        get_bars_batch.return_value = None

        email_content = ""

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body
            print(body)

        mail_notify.side_effect = mail_notify_mock
        # 测试bars 为None
        await syncjobs.sync_minute_bars()
        self.assertIn("Got None Data", email_content)

    @mock.patch("omega.master.jobs.get_timeout", return_value=10)
    @mock.patch("omicron.models.stock.Stock.batch_cache_bars")
    @mock.patch(
        "omega.master.jobs.Task.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch(
        "omega.worker.abstract_quotes_fetcher.AbstractQuotesFetcher.get_bars_batch"
    )
    @mock.patch(
        "omega.master.jobs.get_now", return_value=datetime.datetime(2022, 1, 11, 16)
    )
    async def test_sync_day_bars(self, get_now, get_bars_batch, mail_notify, *args):
        async def clear():
            state = f"{constants.TASK_PREFIX}.day.state"
            await cache.sys.delete(state)

        await clear()
        email_content = ""

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body
            print(body)

        mail_notify.side_effect = mail_notify_mock

        async def get_bars_batch_mock(*args, **kwargs):
            """聚宽的数据被序列化到文件里了，mock读出来  根据帧类型mock"""
            frame_type = kwargs["frame_type"]
            with open(f"{test_dir()}/data/{frame_type.value}.pick", "rb") as f:
                return pickle.loads(f.read())

        get_bars_batch.side_effect = get_bars_batch_mock
        emit.register(Events.OMEGA_DO_SYNC_DAY, workjobs.sync_day_bars)
        ret = await syncjobs.sync_day_bars()
        self.assertTrue(ret)

        # 测试数据为None
        get_bars_batch.side_effect = None
        get_bars_batch.return_value = None

        email_content = ""

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body
            print(body)

        mail_notify.side_effect = mail_notify_mock
        # 测试bars 为None
        await syncjobs.sync_day_bars()
        self.assertIn("Got None Data", email_content)

    @mock.patch("omega.master.jobs.get_timeout", return_value=10)
    @mock.patch("omicron.models.stock.Stock.persist_bars")
    @mock.patch("omega.master.jobs.Storage", side_effect=TempStorage)
    @mock.patch(
        "omega.master.jobs.get_now", return_value=datetime.datetime(2022, 1, 11, 16)
    )
    @mock.patch(
        "omega.worker.abstract_quotes_fetcher.AbstractQuotesFetcher.get_bars_batch"
    )
    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch(
        "omega.master.jobs.Task.get_quota",
        return_value=1000000,
    )
    async def test_daily_calibration_sync(
        self, get_quota, mail_notify, get_bars_batch, *args
    ):
        state = f"{constants.TASK_PREFIX}.daily_calibration.state"
        email_content = ""

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body

        mail_notify.side_effect = mail_notify_mock

        async def get_bars_batch_mock(*args, **kwargs):
            """聚宽的数据被序列化到文件里了，mock读出来  根据帧类型mock"""
            frame_type = kwargs["frame_type"]
            with open(f"{test_dir()}/data/{frame_type.value}.pick", "rb") as f:
                return pickle.loads(f.read())

        get_bars_batch.side_effect = get_bars_batch_mock

        async def clear():
            p = cache.sys.pipeline()
            p.delete(constants.BAR_SYNC_ARCHIVE_HEAD)
            p.delete(constants.BAR_SYNC_ARCHIVE_TAIl)
            p.delete(state)
            await p.execute()

        await clear()

        emit.register(
            Events.OMEGA_DO_SYNC_DAILY_CALIBRATION, workjobs.sync_daily_calibration
        )

        def count_frames():
            """mock 计算帧间隔的方法"""
            i = 4

            def inner(*args, **kwargs):
                nonlocal i
                i -= 1
                return i

            return inner

        def day_frame_mock():
            i = 3

            def inner():
                nonlocal i
                if i > 1:
                    temp = 20050101
                else:
                    temp = 20220107
                i -= 1
                return temp

            return inner

        with mock.patch(
            "omega.master.jobs.get_first_day_frame", side_effect=day_frame_mock()
        ):
            with mock.patch(
                "omicron.models.timeframe.TimeFrame.count_frames",
                side_effect=count_frames(),
            ):
                ret = await syncjobs.daily_calibration_sync()
                self.assertTrue(ret)
                # 检查redis的head 和tail
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_ARCHIVE_HEAD), "2022-01-07"
                )
                self.assertEqual(
                    await cache.sys.get(constants.BAR_SYNC_ARCHIVE_TAIl), "2022-01-10"
                )

        await clear()
        get_quota.return_value = 0
        # 测试quota 不够
        with mock.patch(
            "omega.master.jobs.get_first_day_frame", side_effect=day_frame_mock()
        ):
            with mock.patch(
                "omicron.models.timeframe.TimeFrame.count_frames",
                side_effect=count_frames(),
            ):
                ret = await syncjobs.daily_calibration_sync()
                self.assertIn(f"剩余可用quota：{get_quota.return_value}", email_content)

    @mock.patch("omega.master.jobs.get_timeout", return_value=10)
    @mock.patch(
        "omega.master.jobs.Task.get_quota",
        return_value=1000000,
    )
    @mock.patch(
        "omega.master.jobs.get_now", return_value=datetime.datetime(2022, 1, 11, 16)
    )
    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch(
        "omega.worker.abstract_quotes_fetcher.AbstractQuotesFetcher.get_high_limit_price"
    )
    async def test_sync_high_low_limit(
        self, get_high_limit_price, mail_notify, get_now, get_quota, *args
    ):
        async def get_high_limit_price_mock(*args, **kwargs):
            return np.load(f"{test_dir()}/data/high_low_limit.npy", allow_pickle=True)

        get_high_limit_price.side_effect = get_high_limit_price_mock
        await cache.sys.delete("master.task.high_low_limit.state")
        emit.register(Events.OMEGA_DO_SYNC_HIGH_LOW_LIMIT, workjobs.sync_high_low_limit)

        await syncjobs.sync_high_low_limit()
        # 检查redis中有没有数据
        resp = await cache.sys.hgetall(HIGH_LOW_LIMIT)
        self.assertDictEqual(
            resp,
            {
                "000001.XSHE.high_limit": "18.91",
                "000001.XSHE.low_limit": "15.47",
                "300001.XSHE.high_limit": "27.43",
                "300001.XSHE.low_limit": "18.29",
                "600000.XSHG.high_limit": "9.59",
                "600000.XSHG.low_limit": "7.85",
            },
        )

        email_content = ""

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body

        mail_notify.side_effect = mail_notify_mock

        # 测试数据为None
        get_high_limit_price.side_effect = None
        get_high_limit_price.return_value = None

        # 测试bars 为None
        await syncjobs.sync_high_low_limit()
        self.assertIn("Got None Data", email_content)

        # quota为0
        get_quota.return_value = 0
        ret = await syncjobs.sync_high_low_limit()
        self.assertIn(f"剩余可用quota：{get_quota.return_value}", email_content)

        # 非交易日返回false
        get_now.return_value = datetime.datetime(2022, 1, 9, 16)
        ret = await syncjobs.sync_high_low_limit()
        self.assertFalse(ret)

    @mock.patch("omega.master.jobs.get_timeout", return_value=10)
    @mock.patch("omicron.models.stock.Stock.persist_bars")
    @mock.patch(
        "omega.master.jobs.get_now", return_value=datetime.datetime(2022, 1, 11, 16)
    )
    @mock.patch("omega.master.jobs.Storage", side_effect=TempStorage)
    @mock.patch(
        "omega.master.jobs.Task.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch(
        "omega.worker.abstract_quotes_fetcher.AbstractQuotesFetcher.get_bars_batch"
    )
    async def test_sync_year_quarter_month_week(
        self, get_bars_batch, mail_notify, get_quota, *args
    ):
        week_state = f"{constants.TASK_PREFIX}.{FrameType.WEEK.value}.state"
        month_state = f"{constants.TASK_PREFIX}.{FrameType.MONTH.value}.state"

        async def clear():
            p = cache.sys.pipeline()
            p.delete(constants.BAR_SYNC_WEEK_TAIl)
            p.delete(constants.BAR_SYNC_MONTH_TAIl)
            p.delete(week_state)
            p.delete(month_state)
            await p.execute()

        await clear()
        emit.register(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            workjobs.sync_year_quarter_month_week,
        )

        async def get_bars_batch_mock(*args, **kwargs):
            """聚宽的数据被序列化到文件里了，mock读出来"""
            with open(f"{test_dir()}/data/month_week.pick", "rb") as f:
                return pickle.loads(f.read())

        get_bars_batch.side_effect = get_bars_batch_mock

        def count_frames():
            """mock 计算帧间隔的方法"""
            i = 5

            def inner(*args, **kwargs):
                nonlocal i
                i -= 1
                return i

            return inner

        email_content = ""

        async def mail_notify_mock(subject, body, **kwargs):
            nonlocal email_content
            email_content = body

        mail_notify.side_effect = mail_notify_mock

        with mock.patch(
            "omicron.models.timeframe.TimeFrame.count_frames",
            side_effect=count_frames(),
        ):
            await syncjobs.sync_year_quarter_month_week()
            # 检查redis里的周是否是某个值
            self.assertEqual(
                await cache.sys.get(constants.BAR_SYNC_WEEK_TAIl), "2005-01-07"
            )
            self.assertEqual(
                await cache.sys.get(constants.BAR_SYNC_MONTH_TAIl), "2005-01-31"
            )

        await clear()
        # 测试上游返回None值
        get_bars_batch.return_value = None
        get_bars_batch.side_effect = None
        # mock 写dfs的方法
        await clear()
        with mock.patch(
            "omicron.models.timeframe.TimeFrame.count_frames",
            side_effect=count_frames(),
        ):
            await syncjobs.sync_year_quarter_month_week()
            self.assertIn("Got None Data", email_content)

        await cache.sys.hset(week_state, "is_running", 1)
        await cache.sys.hset(month_state, "is_running", 1)

        # 测试已经在运行了 重复启动
        with mock.patch(
            "omicron.models.timeframe.TimeFrame.count_frames",
            side_effect=count_frames(),
        ):
            ret = await syncjobs.sync_year_quarter_month_week()
            self.assertFalse(ret)
        await clear()

        get_quota.return_value = 0
        # 测试quota 不够
        with mock.patch(
            "omicron.models.timeframe.TimeFrame.count_frames",
            side_effect=count_frames(),
        ):
            ret = await syncjobs.sync_year_quarter_month_week()
            self.assertIn(f"剩余可用quota：{get_quota.return_value}", email_content)
        await clear()
        # 测试超时
        get_quota.return_value = 100000

    async def test_load_cron_task(self):
        scheduler = AsyncIOScheduler(timezone=cfg.tz)

        await syncjobs.load_cron_task(scheduler)
        base = {
            "sync_calendar",
            "1m:10:*",
            "sync_fund_share_daily",
            "sync_year_quarter_month_week",
            "daily_calibration_sync",
            "sync_high_low_limit",
            "1m:13-14:*",
            "sync_fund_net_value",
            "sync_fund_portfolio_stock",
            "sync_day_bars",
            "1m:11:0-30",
            "sync_security_list",
            "1m:15:00",
            "1m:9:31-59",
        }
        self.assertSetEqual(base, set([job.name for job in scheduler.get_jobs()]))

    @mock.patch("omega.master.jobs.get_timeout", return_value=10)
    @mock.patch(
        "omega.master.jobs.Task.get_quota",
        return_value=1000000,
    )
    @mock.patch("omega.master.jobs.mail_notify")
    @mock.patch("omicron.models.stock.Stock.choose", return_value=[])
    async def test_task(self, *args):
        queue_name = "test"
        state = f"{constants.TASK_PREFIX}.{queue_name}.state"

        async def tasks():
            return []

        await cache.sys.delete(state)
        params = {}
        task = syncjobs.Task(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK, queue_name, params, 10
        )
        task.tasks = tasks
        ret = await task.run()
        self.assertFalse(ret)
        # 测试获取当前时间

        self.assertIsInstance(syncjobs.get_now(), datetime.datetime)
        self.assertIsInstance(syncjobs.get_first_day_frame(), np.int64)

        # 测试超时时发送邮件
        await task.send_email()

    # @mock.patch("omega.master.jobs.mail_notify")
    # async def test_sync_funds(self, *args):
    #     secs = await syncjobs.sync_funds()
    #     self.assertTrue(len(secs))

    # @mock.patch("omega.master.jobs.mail_notify")
    # async def test_sync_fund_net_value(self, *args):
    #     await syncjobs.sync_fund_net_value()

    # @mock.patch("omega.master.jobs.mail_notify")
    # async def test_sync_fund_share_daily(self, *args):
    #     await syncjobs.sync_fund_share_daily()

    # @mock.patch("omega.master.jobs.mail_notify")
    # async def test_sync_fund_portfolio_stock(self, *args):
    #     await syncjobs.sync_fund_portfolio_stock()
