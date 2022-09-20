#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from typing import Optional

import arrow
import cfg4py
from cfg4py.config import Config
from coretypes import Frame, FrameType
from omicron import cache, tf

from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.calibration_task import (
    sync_daily_bars_1m,
    sync_daily_bars_day,
    sync_day_bar_factors,
)
from omega.master.tasks.sync_other_bars import (
    sync_min_5_15_30_60,
    sync_month_bars,
    sync_week_bars,
)
from omega.master.tasks.sync_price_limit import (
    sync_cache_price_limits,
    sync_trade_price_limits,
)
from omega.master.tasks.sync_securities import sync_securities_list
from omega.master.tasks.sync_xr_xd_reports import sync_xrxd_reports
from omega.master.tasks.synctask import BarsSyncTask, master_syncbars_task
from omega.scripts import close_frame, update_unclosed_bar

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


async def get_after_hour_sync_job_task() -> Optional[BarsSyncTask]:
    """获取盘后同步的task实例"""
    now = arrow.now().naive
    if not tf.is_trade_day(now):  # pragma: no cover
        logger.info("非交易日，不同步")
        return
    end = tf.last_min_frame(now, FrameType.MIN1)
    if now < end:  # pragma: no cover
        logger.info("当天未收盘，禁止同步")
        return
    name = "day"

    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_DAY,
        name=name,
        frame_type=[FrameType.MIN1, FrameType.DAY],
        end=end,
        timeout=3600 * 2,  # 实际观察大约350秒左右
        recs_per_sec=240 + 4,
        quota_type=2,  # 白天的同步任务
    )
    return task


@master_syncbars_task()
async def after_hour_sync_job():
    """交易日盘后同步任务入口

    收盘之后同步今天的日线和分钟线
    """
    task = await get_after_hour_sync_job_task()
    if not task:
        return
    await task.run()
    return task


async def get_sync_minute_date():
    """获取这次同步分钟线的时间和n_bars"""
    end = arrow.now().naive.replace(second=0, microsecond=0)
    first = end.replace(hour=9, minute=30, second=0, microsecond=0)
    # 检查当前时间是否在交易时间内
    if not tf.is_trade_day(end):  # pragma: no cover
        logger.info("非交易日，不同步")
        return False
    if end < first:  # pragma: no cover
        logger.info("时间过早，不能拿到k线数据")
        return False

    end = tf.floor(end, FrameType.MIN1)
    tail = await cache.sys.get(constants.BAR_SYNC_MINUTE_TAIL)
    # tail = "2022-02-22 13:29:00"
    if tail:
        # todo 如果有，这个时间最早只能是今天的9点31分,因为有可能是昨天执行完的最后一次
        tail = datetime.datetime.strptime(tail, "%Y-%m-%d %H:%M:00")
        if tail < first:
            tail = first
    else:
        tail = first

    # 取上次同步截止时间+1 计算出n_bars
    tail = tf.floor(tail + datetime.timedelta(minutes=1), FrameType.MIN1)
    n_bars = tf.count_frames(tail, end, FrameType.MIN1)  # 获取到一共有多少根k线
    return end, n_bars


async def get_sync_minute_bars_task() -> Optional[BarsSyncTask]:
    """构造盘中分钟线的task实例"""
    ret = await get_sync_minute_date()
    if not ret:  # pragma: no cover
        return
    else:
        end, n_bars = ret
    name = "minute"
    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_MIN,
        name=name,
        frame_type=[FrameType.MIN1],
        end=end,
        timeout=60 * n_bars + 30,  # 增加30秒的超时
        n_bars=n_bars,
        recs_per_sec=n_bars,
        quota_type=2,  # 白天的同步任务
    )
    return task


async def run_sync_minute_bars_task(task: BarsSyncTask):
    """执行task的方法"""
    flag = await task.run()
    if flag:
        # 说明正常执行完的
        await cache.sys.set(
            constants.BAR_SYNC_MINUTE_TAIL,
            task.end.strftime("%Y-%m-%d %H:%M:00"),
        )
        frame = task.end
        for frame_type in (
            FrameType.MIN5,
            FrameType.MIN15,
            FrameType.MIN30,
            FrameType.MIN60,
            FrameType.DAY,
            FrameType.WEEK,
            FrameType.MONTH,
        ):
            await update_unclosed_bar(frame_type, frame)

            if frame == tf.ceiling(frame, frame_type):
                await close_frame(frame_type, frame)

    return task


@master_syncbars_task()
async def sync_minute_bars():
    """盘中同步每分钟的数据
    1. 从redis拿到上一次同步的分钟数据
    2. 计算开始和结束时间
    """
    task = await get_sync_minute_bars_task()
    if not task:
        return
    await run_sync_minute_bars_task(task)


async def load_cron_task(scheduler):
    # 交易日交易时间段的数据同步任务
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=9,
        minute="31-59",
        name=f"{FrameType.MIN1.value}:9:31-59",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=10,
        minute="*",
        name=f"{FrameType.MIN1.value}:10:*",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=11,
        minute="0-31",  # 0-31，执行32次
        name=f"{FrameType.MIN1.value}:11:0-31",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour="13-14",
        minute="*",
        name=f"{FrameType.MIN1.value}:13-14:*",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=15,
        minute="0-1",  # 15:00,15:01，执行2次
        name=f"{FrameType.MIN1.value}:15:00",
    )

    scheduler.add_job(
        after_hour_sync_job,
        "cron",
        hour="15",
        minute=5,
        name="after_hour_sync_job",
    )

    # 以下追赶性质的任务，应该单独执行，不能和日常同步任务混在一起，
    # 因为jqadaptor只有一个线程，定时器也只能触发一个同类型的任务
    scheduler.add_job(
        sync_month_bars,
        "cron",
        hour=1,
        minute=15,
        name="sync_month_bars",
    )
    scheduler.add_job(
        sync_week_bars,
        "cron",
        hour=1,
        minute=25,
        name="sync_week_bars",
    )
    scheduler.add_job(
        sync_daily_bars_day,  # 下载日线
        "cron",
        hour=1,
        minute=35,
        name="day_sync_task",
    )
    scheduler.add_job(
        sync_trade_price_limits,
        "cron",
        hour=1,
        minute="45",  # 同步前一个交易日的涨跌停数据
        name="sync_trade_price_limits",
    )

    scheduler.add_job(
        sync_daily_bars_1m,
        "cron",
        hour=2,
        minute=0,
        name="daily_bars_sync",
    )
    scheduler.add_job(
        sync_min_5_15_30_60,
        "cron",
        hour=2,
        minute=20,
        name="sync_min_5_15_30_60",
    )

    scheduler.add_job(
        sync_securities_list,
        "cron",
        hour="8",
        minute="5",
        name="sync_securities",  # 聚宽8点更新，写入昨日数据到db，今日数据到cache
    )
    scheduler.add_job(
        sync_xrxd_reports,
        "cron",
        hour="8",
        minute="11",
        name="sync_xrxd",
    )
    scheduler.add_job(
        sync_day_bar_factors,  # 修正factor
        "cron",
        hour=9,
        minute=30,
        name="day_factor_fix_task",
    )
    scheduler.add_job(
        sync_cache_price_limits,
        "cron",
        hour=9,
        minute="1,31",  # 第一次为了交易界面方便使用，第二次是正确的数据（修正后）
        second=10,
        name="sync_cache_price_limits",
    )
