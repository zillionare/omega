#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import datetime
import logging
from typing import List, Optional, Tuple, Union

import aiohttp
import arrow
import async_timeout
import cfg4py
from cfg4py.config import Config
from omicron import cache
from omicron.core.types import FrameType
from omicron.models.calendar import Calendar as cal
from omicron.models.stock import Stock
from pyemit import emit

from omega.core.constants import (
    BAR_SYNC_ARCHIVE_HEAD,
    BAR_SYNC_ARCHIVE_TAIl,
    get_queue_name,
)
from omega.core.events import Events
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


async def _start_job_timer(job_name: str):
    key_start = f"master.bars_{job_name}.start"

    pl = cache.sys.pipeline()
    pl.delete(f"master.bars_{job_name}.*")

    pl.set(key_start, arrow.now(tz=cfg.tz).format("YYYY-MM-DD HH:mm:ss"))
    await pl.execute()


async def _stop_job_timer(job_name: str) -> int:
    key_start = f"master.bars_{job_name}.start"
    key_stop = f"master.bars_{job_name}.stop"
    key_elapsed = f"master.bars_{job_name}.elapsed"

    start = arrow.get(await cache.sys.get(key_start), tzinfo=cfg.tz)
    stop = arrow.now(tz=cfg.tz)
    elapsed = (stop - start).seconds

    pl = cache.sys.pipeline()
    pl.set(key_stop, stop.format("YYYY-MM-DD HH:mm:ss"))
    pl.set(key_elapsed, elapsed)
    await pl.execute()

    return elapsed


async def sync_calendar():
    """从上游服务器获取所有交易日，并计算出周线帧和月线帧

    Returns:
    """
    trade_days = await aq.get_all_trade_days()
    if trade_days is None or len(trade_days) == 0:
        logger.warning("failed to fetch trade days.")
        return None

    cal.day_frames = [cal.date2int(x) for x in trade_days]
    weeks = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.weekday() < last.weekday() or (cur - last).days >= 7:
            weeks.append(last)
        last = cur

    if weeks[-1] < last:
        weeks.append(last)

    cal.week_frames = [cal.date2int(x) for x in weeks]
    await cache.save_calendar("week_frames", map(cal.date2int, weeks))

    months = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.day < last.day:
            months.append(last)
        last = cur
    months.append(last)

    cal.month_frames = [cal.date2int(x) for x in months]
    await cache.save_calendar("month_frames", map(cal.date2int, months))
    logger.info("trade_days is updated to %s", trade_days[-1])


async def sync_security_list():
    """更新证券列表

    注意证券列表在AbstractQuotesServer取得时就已保存，此处只是触发
    """
    secs = await aq.get_security_list()
    logger.info("%s secs are fetched and saved.", len(secs))


async def generate_task():
    codes = Stock.choose()
    exclude = getattr(cfg.omega.sync.bars, "exclude", "")
    if exclude:
        exclude = map(lambda x: x, exclude.split(" "))
        codes = list(set(codes) - set(exclude))
    include = getattr(cfg.omega.sync.bars, "include", "")
    if include:
        include = list(filter(lambda x: x, cfg.omega.sync.bars.include.split(" ")))
        codes.extend(include)
    return codes


async def check_done(timeout, state, count, done):
    while True:
        try:
            async with async_timeout.timeout(timeout):
                while True:
                    done_count = await cache.sys.hget(state, "done_count")
                    if done_count is None or int(done_count) != count:
                        await asyncio.sleep(1)
                    else:
                        await cache.sys.delete(done)
                        return
        except asyncio.exceptions.TimeoutError:
            print("超时了, 继续等待，并发送报警邮件")


async def __daily_calibration_sync(tread_date, head=None, tail=None):
    suffix = "daily_calibration"
    start = tread_date.replace(hour=9, minute=31, microsecond=0, second=0)
    end = tread_date.replace(hour=15, minute=0, microsecond=0, second=0)
    # 检查 end 是否在交易日

    params = {
        "suffix": suffix + f'.{tread_date.strftime("%Y-%m-%d")}',
        "start": start,
        "end": end,
        "timeout": 60 * 60 * 6,
        "resample": True,  # 重采样
        "check_and_persistence": True,
        "after_delete": True,  # 因为凌晨2点同步用的是不同的队列，所以需要删除
    }
    ret = await sync_bars(params)
    if ret is not None:
        return

    # await sync_day_bars(suffix + f'.{head}', start, end, check_and_persistence=True, timeout=60 * 60 * 6)
    if head is not None:
        await cache.sys.set(BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d"))
    if tail is not None:
        await cache.sys.set(BAR_SYNC_ARCHIVE_TAIl, tail.strftime("%Y-%m-%d"))

    await daily_calibration_sync()


async def daily_calibration_sync():
    """凌晨2点数据同步，调用sync_day_bars，添加参数写minio和重采样
    然后需要往前追赶同步，剩余quota > 1天的量就往前赶，并在redis记录已经有daily_calibration_sync在运行了
    """
    # 检查head和tail
    # 检查tail和最近一个交易日有没有空洞，如果有空洞，先同步空洞
    total = len(await generate_task())
    quota = await aq.get_quota()
    # 检查quota够不够，如果不够则return
    if quota * 0.75 < total * 240 * 2:
        print("quota不够，返回")
        return

    # state, done, scope = get_queue_name(suffix)
    days = datetime.timedelta(days=1)
    head, tail = await cache.sys.get(BAR_SYNC_ARCHIVE_HEAD), await cache.sys.get(
        BAR_SYNC_ARCHIVE_TAIl
    )
    now = datetime.datetime.now()

    if not head or not tail:
        # 任意一个缺失都不行
        print("说明是首次同步，查找上一个已收盘的交易日")
        pre_trade_day = cal.day_shift(now, -1)
        await __daily_calibration_sync(
            datetime.datetime.combine(pre_trade_day, datetime.time(0, 0)),
            head=pre_trade_day,
            tail=pre_trade_day,
        )

    else:
        # 说明不是首次同步，检查tail到现在有没有空洞
        tail_date = datetime.datetime.strptime(tail, "%Y-%m-%d")
        head_date = datetime.datetime.strptime(head, "%Y-%m-%d")
        if (
            cal.count_frames(
                tail_date,
                now.replace(hour=0, minute=0, second=0, microsecond=0),
                FrameType.DAY,
            )
            - 1
            > 1
        ):
            await __daily_calibration_sync(tail_date, tail=tail_date + days)
        else:
            tread_date = head_date - days
            await __daily_calibration_sync(tread_date, head=tread_date)


async def sync_day_bars():
    """
    收盘之后同步今天的数据
    """
    suffix = "day"
    start = datetime.datetime.now().replace(hour=9, minute=31, second=0, microsecond=0)
    end = datetime.datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    params = {
        "suffix": suffix,
        "start": start,
        "end": end,
        "timeout": 60 * 60 * 2,
        "resample": True,  # 重采样
        "after_delete": True,
    }
    await sync_bars(params)


async def sync_minute_bars():
    """盘中同步每分钟的数据
    1. 从redis拿到上一次同步的分钟数据
    2. 计算开始和结束时间
    """
    suffix = "minute"
    state, done, scope = get_queue_name(suffix)
    end = datetime.datetime.now().replace(second=0, microsecond=0)
    # 检查当前时间是否在交易时间内
    if end.hour * 60 + end.minute not in cal.ticks[FrameType.MIN1]:
        if 11 <= end.hour < 13:
            end = end.replace(hour=11, minute=30)
        else:
            end = end.replace(hour=15, second=0, minute=0)

    start = await cache.sys.hget(state, "start")
    if not start:
        # 如果么有end,则取上一分钟的时间
        start = end - datetime.timedelta(minutes=1)
    else:
        # todo 如果有，这个时间最早只能是今天的9点31分
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:00")
    params = {"suffix": suffix, "start": start, "end": end}
    await sync_bars(params)


async def sync_high_low_limit():

    suffix = "high_low_limit"  # 队列后缀
    timeout = 60 * 60 * 10
    end = datetime.datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    state, done, scope = get_queue_name(suffix)
    params = {"suffix": suffix, "end": end, "sync_high_low_limit": True}
    await create_task_and_join(
        state, done, scope, timeout, end, after_delete=False, params=params
    )


async def create_task_and_join(state, done, scope, timeout, end, after_delete, params):
    """创建任务并且阻塞住"""
    if after_delete:
        # 先设置超时时间，防止程序终止导致内存溢出
        await cache.sys.expire(state, timeout)
    await cache.sys.hset(state, "is_running", 1)
    tasks = await generate_task()
    count = len(tasks)
    p = cache.sys.pipeline()
    p.delete(done)
    p.hmset(state, "task_count", count)
    p.delete(scope)
    p.lpush(scope, *tasks)
    await p.execute()
    await emit.emit(Events.OMEGA_DO_SYNC_MIN, params)
    await check_done(timeout, state, count, done)

    p = cache.sys.pipeline()
    if after_delete:
        p.delete(state)
    else:
        p.hmset(state, "start", end.strftime("%Y-%m-%d %H:%M:00"))
    p.hdel(state, "is_running")
    p.hdel(state, "task_count")
    p.hdel(state, "done_count")
    await p.execute()


async def sync_bars(params):
    suffix = params.get("suffix", "minute")  # 队列后缀
    timeout = params.get("timeout", 60)  # 允许的超时时间
    start = params.get("start")  # 开始时间
    end = params.get("end")  # 结束时间
    after_delete = params.get("after_delete")  # 结束时间
    if not cal.is_trade_day(end):
        print("非交易日，不同步")
        return
    state, done, scope = get_queue_name(suffix)

    is_running = await cache.sys.hget(state, "is_running")
    if is_running is not None:
        logger.info(f"检测到有正在执行的分钟同步任务，本次不执行：{end}")
        print(f"检测到有正在执行的分钟同步任务，本次不执行：{end}")
        return False
    n_bars = cal.count_frames(start, end, FrameType.MIN1)  # 获取到一共有多少根k线
    if n_bars < 1:
        msg = "k线数量小于1 不同步"
        logger.info(msg)
        print(msg)
        return
    params.update({"n_bars": n_bars})
    await create_task_and_join(state, done, scope, timeout, end, after_delete, params)
