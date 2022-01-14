# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-07 18:28
import asyncio
import datetime
import hashlib
import json
import logging
import pickle
import time
import traceback
from functools import wraps
from typing import List

import cfg4py
import numpy as np
from dfs import Storage
from omicron import cache
from omicron.core.types import FrameType, SecurityType
from omicron.extensions.np import numpy_append_fields
from omicron.models.stock import Stock

from omega.core.constants import HIGH_LOW_LIMIT
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


async def worker_exit(state, scope, error=None):
    if error is None:
        error = traceback.format_exc()
    # 删除is_running 并写上错误堆栈信息
    p = cache.sys.pipeline()
    p.hdel(state, "is_running")
    p.hmset(state, "error", error)
    p.delete(scope)  # 删除任务队列，让其他没退出的消费者也退出，错误上报至master
    await p.execute()


def decorator():
    def inner(f):
        @wraps(f)
        async def decorated_function(params):
            """装饰所有worker，统一处理错误信息"""
            # 执行之前，将状态中的worker_count + 1，以确保生产者能知道有多少个worker收到了消息
            scope, state, end, n_bars, fail = await parse_params(params)

            await cache.sys.hincrby(state, "worker_count")
            try:
                error_msg = await f(params)
                if error_msg is not None:
                    await worker_exit(state, scope, error_msg)
            # 说明消费者消费时错误了
            except Exception as e:
                logger.exception(e)
                await worker_exit(state, scope)

            return

        return decorated_function

    return inner


async def get_limit():
    return 3000


async def fetch_bars(
    secs: List[str], end: datetime.datetime, n_bars: int, frame_type: FrameType
):
    bars = await AbstractQuotesFetcher.get_bars_batch(
        secs, end=end, n_bars=n_bars, frame_type=frame_type
    )
    temp = []
    if bars is not None:
        for code in bars:
            bar = numpy_append_fields(bars[code], "code", code, dtypes=[("code", "O")])
            temp.append(bar)
        return np.concatenate(temp)
    return None


async def fetch_min_bars(secs: List[str], end: datetime.datetime, n_bars: int):
    """
    从远端获取分钟线数据
    Args:
        secs: 股票的列表 ["00001.XSHG", ...]
        end:  时间
        n_bars: 多少根bar

    Returns:

    """
    return await fetch_bars(secs, end, n_bars, FrameType.MIN1)


async def fetch_week_bars(secs: List[str], end: datetime.datetime, n_bars: int):
    """
    从远端获取分钟线数据
    Args:
        secs: 股票的列表 ["00001.XSHG", ...]
        end:  时间
        n_bars: 多少根bar

    Returns:

    """
    return await fetch_bars(secs, end, n_bars, FrameType.WEEK)


async def get_high_low_limit(secs, end):
    """获取涨跌停价"""
    return await AbstractQuotesFetcher.get_high_limit_price(secs, end)


def get_remnant_s():
    """获取从现在开始到晚上12点还剩多少秒"""
    now = datetime.datetime.now()
    end = datetime.datetime(year=now.year, month=now.month, day=now.day + 1)
    # 当前时间秒数
    return int((end - now).total_seconds())


async def fetch_day_bars(secs: List[str], end: datetime.datetime):
    """
    获取日线数据
    从get_bars中取到标准数据
    从get_price中取到涨跌停
    Args:
        secs: 股票的列表
        end: 时间

    Returns: 返回np数组

    """
    dtypes = [("high_limit", "<f8"), ("low_limit", "<f8"), ("code", "O")]
    """获取日线数据，和 今日的涨跌停并合并"""
    bars = await AbstractQuotesFetcher.get_bars_batch(
        secs, end=end, n_bars=1, frame_type=FrameType.DAY
    )
    high_low_limit = await get_high_low_limit(secs, end)
    result = []
    for sec in high_low_limit:
        name = sec[1]
        bar = numpy_append_fields(
            bars[name],
            ("high_limit", "low_limit", "code"),
            [sec["high_limit"], sec["low_limit"], name],
            dtypes,
        )
        result.append(bar)
    return np.concatenate(result)


async def get_secs(limit: int, n_bars: int, scope: str):
    """
    从redis遍历取任务执行
    Args:
        limit: 调用接口限制条数
        n_bars: 每支股票需要取多少条
        scope: 从那个队列取

    Returns:

    """
    while True:
        p = cache.sys.pipeline()
        step = limit // n_bars  # 根据 单次api允许获取的条数 和一共多少根k线 计算每次最多可以获取多少个股票的
        p.lrange(scope, 0, step - 1)
        p.ltrim(scope, step, -1)
        secs, _ = await p.execute()
        if not len(secs):
            break
        yield secs


async def sync_high_low_limit(params):
    """同步涨跌停worker"""
    scope, state, end, n_bars, fail = await parse_params(params)

    limit = await get_limit()
    hashmap = []
    total = 0
    async for secs in get_secs(limit, 1, scope):
        bars = await get_high_low_limit(secs, end)
        if bars is None:
            return "Got None data"
        for sec in bars:
            name = sec["code"]
            high_limit = sec["high_limit"]
            low_limit = sec["low_limit"]
            hashmap.extend(
                [
                    f"{name}.high_limit",
                    str(high_limit),
                    f"{name}.low_limit",
                    str(low_limit),
                ]
            )
            total += 1
    if hashmap:
        await cache.sys.hmset(HIGH_LOW_LIMIT, *hashmap)
        # 取到到晚上12点还有多少秒
        await cache.sys.expire(HIGH_LOW_LIMIT, get_remnant_s())
    await cache.sys.hincrby(state, "done_count", total)


@decorator()
async def sync_minute_bars(params):
    """
    盘中同步分钟
    Args:
        params:

    Returns:

    """
    scope, state, end, n_bars, fail = await parse_params(params)
    limit = await get_limit()
    total = 0
    async for secs in get_secs(limit, n_bars, scope):
        bars = await fetch_min_bars(secs, end, n_bars)
        if bars is None:
            return "Got None data"
            # continue
        await Stock.batch_cache_bars(FrameType.MIN1, bars)
        total += len(secs)

    await cache.sys.hincrby(state, "done_count", total)


async def parse_params(params):
    return (
        params.get("__scope"),
        params.get("__state"),
        params.get("end"),
        params.get("n_bars", 1),
        params.get("__fail"),
    )


@decorator()
async def sync_day_bars(params):
    """这是下午三点收盘后的同步，仅写cache，同步分钟线和日线"""
    scope, state, end, n_bars, fail = await parse_params(params)
    total = 0
    tasks = []
    limit = await get_limit()

    async for secs in get_secs(limit, n_bars, scope):
        min_bars = await fetch_min_bars(secs, end, n_bars)
        if min_bars is None:
            continue
        await Stock.batch_cache_bars(FrameType.MIN1, min_bars)
        total += len(secs)

        # 写cache
    for i in range(0, len(tasks), limit):
        temp = tasks[i : i + limit]
        bars = await fetch_day_bars(temp, end)
        if bars is None:
            continue
        await Stock.batch_cache_bars(FrameType.DAY, bars)
        total += len(temp)

    await cache.sys.hincrby(state, "done_count", total // 2)


async def checksum(value1, value2):
    return hashlib.md5(value1).hexdigest() == hashlib.md5(value2).hexdigest()


async def handle_dfs_data(bars1, bars2, queue_name):
    """
    检查两次数据的md5
    Args:
        bars1:
        bars2:
        queue_name: 检查通过后把数据放进redis

    Returns: bool

    """
    value1 = pickle.dumps(bars1, protocol=cfg.pickle.ver)
    value2 = pickle.dumps(bars2, protocol=cfg.pickle.ver)
    if await checksum(value1, value2):
        await cache.temp.lpush(queue_name, value1)
        return True
    else:
        return False


async def __sync_daily_calibration(
    scope: str, queue_min: str, queue_day: str, params: dict
):
    """
    Args:
        scope: 从那个队列中取任务执行，队列名
        queue_min: 分钟线队列名
        queue_day: 日线队列名
        params: master发送过来的所有参数

    Returns:

    """
    _, state, end, n_bars, fail = await parse_params(params)

    limit = await get_limit()
    total = 0
    tasks = []
    async for secs in get_secs(limit, n_bars, scope):
        tasks.extend(secs)
        min_bars1 = await fetch_min_bars(secs, end, n_bars)
        min_bars2 = await fetch_min_bars(secs, end, n_bars)
        if min_bars1 is None or min_bars2 is None:
            await cache.sys.lpush(
                fail,
                str(
                    {
                        "code": secs,
                        "reason": "got none data",
                        "frame_type": "1m",
                        "params": params,
                    }
                ),
            )
            continue
        # 校验通过，持久化
        if not await handle_dfs_data(min_bars1, min_bars2, queue_min):
            await cache.sys.lpush(
                fail,
                str(
                    {
                        "code": secs,
                        "reason": "checksum fail",
                        "frame_type": "1m",
                        "params": params,
                    }
                ),
            )
            continue
        # todo 写dfs
        await Stock.persist_bars(FrameType.MIN1, min_bars1)
        total += len(secs)

    for i in range(0, len(tasks), limit):
        temp = tasks[i : i + limit]
        bars1 = await fetch_day_bars(temp, end)
        bars2 = await fetch_day_bars(temp, end)
        if bars1 is None or bars2 is None:
            await cache.sys.lpush(
                fail,
                str(
                    {
                        "code": temp,
                        "reason": "got none data",
                        "frame_type": "1d",
                        "params": params,
                    }
                ),
            )

            continue
        if not await handle_dfs_data(bars1, bars2, queue_day):
            await cache.sys.lpush(
                fail,
                str(
                    {
                        "code": temp,
                        "reason": "checksum fail",
                        "frame_type": "1m",
                        "params": params,
                    }
                ),
            )
            continue

        await Stock.persist_bars(FrameType.DAY, bars1)
        total += len(temp)
    return total // 2


@decorator()
async def sync_daily_calibration(params):
    """凌晨2点的数据校准，
    同步分钟线 持久化
    同步日线 持久化"""
    scope, state, end, n_bars, fail = await parse_params(params)
    stock_min = params.get("stock_min")
    index_min = params.get("index_min")
    stock_day = params.get("stock_day")
    index_day = params.get("index_day")

    stock_queue = params.get("stock_queue")
    index_queue = params.get("index_queue")
    total = 0
    total += await __sync_daily_calibration(stock_queue, stock_min, stock_day, params)
    total += await __sync_daily_calibration(index_queue, index_min, index_day, params)

    await cache.sys.hincrby(state, "done_count", total)


@decorator()
async def sync_year_quarter_month_week(params):
    scope, state, end, n_bars, fail = await parse_params(params)
    stock_queue = params.get("stock_queue")
    index_queue = params.get("index_queue")
    stock_data = params.get("stock_data")
    index_data = params.get("index_data")
    frame_type = params.get("frame_type")
    limit = await get_limit()
    total = 0
    tasks = []
    async for secs in get_secs(limit, n_bars, stock_queue):
        tasks.extend(secs)
        stock_bars1 = await fetch_bars(secs, end, n_bars, frame_type)
        stock_bars2 = await fetch_bars(secs, end, n_bars, frame_type)
        if stock_bars1 is None or stock_bars2 is None:
            await cache.sys.lpush(
                fail,
                str(
                    {
                        "code": secs,
                        "reason": "got none data",
                        "frame_type": frame_type.value,
                        "params": params,
                    }
                ),
            )
            continue
        # 校验通过，持久化
        if not await handle_dfs_data(stock_bars1, stock_bars2, stock_data):
            await cache.sys.lpush(
                fail,
                str(
                    {
                        "code": secs,
                        "reason": "checksum fail",
                        "frame_type": frame_type.value,
                        "params": params,
                    }
                ),
            )
            continue
        await Stock.persist_bars(frame_type, stock_bars1)
        total += len(secs)

    async for secs in get_secs(limit, n_bars, index_queue):
        tasks.extend(secs)
        index_bars1 = await fetch_bars(secs, end, n_bars, frame_type)
        index_bars2 = await fetch_bars(secs, end, n_bars, frame_type)
        if index_bars1 is None or index_bars2 is None:
            await cache.sys.lpush(
                fail,
                str(
                    {
                        "code": secs,
                        "reason": "got none data",
                        "frame_type": FrameType.WEEK.value,
                        "params": params,
                    }
                ),
            )
            continue
        # 校验通过，持久化
        if not await handle_dfs_data(index_bars1, index_bars2, index_data):
            await cache.sys.lpush(
                fail,
                str(
                    {
                        "code": secs,
                        "reason": "checksum fail",
                        "frame_type": FrameType.WEEK.value,
                        "params": params,
                    }
                ),
            )
            continue
        await Stock.persist_bars(frame_type, index_bars1)
        total += len(secs)
    await cache.sys.hincrby(state, "done_count", total)
