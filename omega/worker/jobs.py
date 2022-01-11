# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-07 18:28
import asyncio
import datetime
import logging
import time

import numpy as np
from omicron import cache
from omicron.core.types import FrameType
from omicron.extensions.np import numpy_append_fields
from omicron.models.stock import Stock

from omega.core.constants import HIGH_LOW_LIMIT, get_queue_name
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


async def get_limit():
    return 3000


async def fetch_min_bars(secs, end, n_bars):
    """从远端获取分钟线数据"""
    bars = await AbstractQuotesFetcher.get_bars_batch(
        secs, end=end, n_bars=n_bars, frame_type=FrameType.MIN1
    )
    temp = []
    if bars is not None:
        for code in bars:
            bar = numpy_append_fields(bars[code], "code", code, dtypes=[("code", "O")])
            temp.append(bar)
        return np.concatenate(temp)
    return None


async def get_high_low_limit(secs, end):
    """获取涨跌停价"""
    return await AbstractQuotesFetcher.get_high_limit_price(secs, end)


def get_remnant_s():
    """获取从现在开始到晚上12点还剩多少秒"""
    now = datetime.datetime.now()
    end = datetime.datetime(year=now.year, month=now.month, day=now.day + 1)
    # 当前时间秒数
    return int((end - now).total_seconds())


async def fetch_day_bars(secs, end):
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
            [sec[2], sec[3], name],
            dtypes,
        )
        result.append(bar)
    return np.concatenate(result)


async def get_secs(limit, n_bars, scope):
    while True:
        p = cache.sys.pipeline()
        step = limit // n_bars  # 根据 单次api允许获取的条数 和一共多少根k线 计算每次最多可以获取多少个股票的
        p.lrange(scope, 0, step - 1)
        p.ltrim(scope, step, -1)
        secs, _ = await p.execute()
        if not len(secs):
            print("队列无数据，已完成")
            break
        yield secs


async def sync_high_low_limit(scope, end):
    limit = await get_limit()
    hashmap = []
    total = 0
    async for secs in get_secs(limit, 1, scope):
        bars = await get_high_low_limit(secs, end)
        if bars is None:
            continue
        for sec in bars:
            name = sec[1]
            high_limit = sec[2]
            low_limit = sec[3]
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
    return total


async def sync_bars(end, n_bars, scope, resample, check_and_persistence, day_bars):
    limit = await get_limit()
    tasks = []
    async for secs in get_secs(limit, n_bars, scope):
        tasks.extend(secs)
        bars = await fetch_min_bars(secs, end, n_bars)
        if resample:
            print("进行重采样")
        if check_and_persistence:
            print("进行校验并持久化")
            bars = await fetch_min_bars(secs, end, n_bars)
            await Stock.persist_bars(FrameType.MIN1, bars)

    if day_bars:
        for i in range(0, len(tasks), limit):
            temp = tasks[i : i + limit]
            bars = await fetch_day_bars(temp, end)
            if check_and_persistence or True:
                bars = await fetch_day_bars(temp, end)
                await Stock.persist_bars(FrameType.DAY, bars)
            else:
                print("不进行持久化，写cache")
                # Stock.cache_bars()
    return len(tasks)


async def sync_consumer(params: dict):
    """"""
    s = time.time()
    end = params.get("end")
    suffix = params.get("suffix")  # 队列后缀
    n_bars = params.get("n_bars", 1)  # 多少个bars
    resample = params.get("resample")  # 重采样
    check_and_persistence = params.get("check_and_persistence")  # 校验并进行持久化 这个说明是
    day_bars = params.get("day_bars")  # 是否需要日线
    high_low_limit = params.get("sync_high_low_limit")  # 是否是同步涨跌停数据
    state, done, scope = get_queue_name(suffix)

    if high_low_limit:
        total = await sync_high_low_limit(scope, end)
    else:
        total = await sync_bars(
            end, n_bars, scope, resample, check_and_persistence, day_bars
        )

    await cache.sys.hincrby(state, "done_count", total)
    print("剩余AbstractQuotesFetcher.quota", AbstractQuotesFetcher.quota)
    logger.info(f"本次同步已完成，耗时：{time.time() - s}, 参数{params}")
