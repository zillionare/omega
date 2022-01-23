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

import async_timeout
import cfg4py
import numpy as np
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from coretypes import FrameType
from omicron import cache
from omicron.extensions.np import numpy_append_fields
from omicron.models.stock import Stock
from omicron.notify.mail import mail_notify
from coretypes import FrameType, SecurityType

from omega.core.constants import HIGH_LOW_LIMIT
from omega.worker import exception
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


async def worker_exit(state, scope, error=None):
    if error is None:  # pragma: no cover
        error = traceback.format_exc()
    # 删除is_running 并写上错误堆栈信息
    p = cache.sys.pipeline()
    p.hdel(state, "is_running")
    p.hmset(state, "error", error)
    p.delete(scope)  # 删除任务队列，让其他没退出的消费者也退出，错误上报至master
    await p.execute()


async def push_fail_secs(secs, fail):
    await cache.sys.lpush(fail, str(secs))


def abnormal_work_report():
    def inner(f):
        @wraps(f)
        async def decorated_function(params):
            """装饰所有worker，统一处理错误信息"""
            scope, state, end, n_bars, fail, timeout = await parse_params(params)
            if isinstance(timeout, int):
                timeout -= 5  # 提前5秒退出
            try:
                async with async_timeout.timeout(timeout):
                    # 执行之前，将状态中的worker_count + 1，以确保生产者能知道有多少个worker收到了消息
                    await cache.sys.hincrby(state, "worker_count")
                    try:
                        ret = await f(params)
                        return ret
                    except exception.WorkerException as e:
                        await worker_exit(state, scope, e.msg)
                    except Exception as e:  # pragma: no cover
                        # 说明消费者消费时错误了
                        logger.exception(e)
                        await worker_exit(state, scope)
            except asyncio.exceptions.TimeoutError:  # pragma: no cover
                await worker_exit(state, scope)
                return False

        return decorated_function

    return inner


async def get_limit():
    return 3000


async def cache_init():
    """初始化缓存 交易日历和证券代码"""
    if not await cache.security.exists(f"calendar:{FrameType.DAY.value}"):
        await AbstractQuotesFetcher.get_all_trade_days()
    if not await cache.security.exists("security:stock"):
        await AbstractQuotesFetcher.get_security_list()


async def fetch_bars(
    secs: List[str], end: datetime.datetime, n_bars: int, frame_type: FrameType
):
    bars = await AbstractQuotesFetcher.get_bars_batch(
        secs, end=end, n_bars=n_bars, frame_type=frame_type
    )
    temp = []
    if bars is not None:
        for code in bars:
            temp_bar = bars[code]
            if not isinstance(temp_bar, np.ndarray):
                continue
            bar = numpy_append_fields(temp_bar, "code", code, dtypes=[("code", "O")])
            temp.append(bar)
        return np.concatenate(temp)


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
    end_str = end.strftime("%Y-%m-%d")
    for sec in high_low_limit:
        date = sec["time"].astype("M8[ms]").astype("O").strftime("%Y-%m-%d")
        if date != end_str:
            continue
        name = sec[1]
        bar = numpy_append_fields(
            bars[name],
            ("high_limit", "low_limit", "code"),
            [sec["high_limit"], sec["low_limit"], name],
            dtypes,
        )
        result.append(bar)
    if result:
        return np.concatenate(result)
    else:
        return result


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
        step = limit // n_bars  # 根据 单次api允许获取的条数 和一共多少根k线 计算每次最多可以获取多少个股票的
        p = cache.sys.pipeline()
        p.lrange(scope, 0, step - 1)
        p.ltrim(scope, step, -1)
        secs, _ = await p.execute()
        if not len(secs):
            break
        yield secs


@abnormal_work_report()
async def sync_high_low_limit(params):
    """同步涨跌停worker"""
    scope, state, end, n_bars, fail, timeout = await parse_params(params)

    limit = await get_limit()
    hashmap = []
    total = 0
    async for secs in get_secs(limit, 1, scope):
        bars = await get_high_low_limit(secs, end)
        if bars is None:
            await push_fail_secs(secs, fail)
            raise exception.GotNoneData()
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
    return True


async def __sync_min_bars(params):
    """同步分支线数据并写缓存"""
    limit = await get_limit()
    scope, state, end, n_bars, fail, timeout = await parse_params(params)
    total = 0
    tasks = []
    async for secs in get_secs(limit, n_bars, scope):
        bars = await fetch_min_bars(secs, end, n_bars)
        if bars is None:
            await push_fail_secs(secs, fail)
            raise exception.GotNoneData()
        if isinstance(bars, np.ndarray):
            await Stock.batch_cache_bars(FrameType.MIN1, bars)
        total += len(secs)
        tasks.extend(secs)
    return total, tasks


@abnormal_work_report()
async def sync_minute_bars(params):
    """
    盘中同步分钟
    Args:
        params:

    Returns:

    """
    scope, state, end, n_bars, fail, timeout = await parse_params(params)
    total, tasks = await __sync_min_bars(params)

    await cache.sys.hincrby(state, "done_count", total)


async def parse_params(params):
    logger.info(params)
    return (
        params.get("__scope"),
        params.get("__state"),
        params.get("end"),
        params.get("n_bars", 1),
        params.get("__fail"),
        params.get("__timeout"),
    )


@abnormal_work_report()
async def sync_day_bars(params):
    """这是下午三点收盘后的同步，仅写cache，同步分钟线和日线"""
    scope, state, end, n_bars, fail, timeout = await parse_params(params)
    limit = await get_limit()

    total, tasks = await __sync_min_bars(params)

    for i in range(0, len(tasks), limit):
        temp = tasks[i : i + limit]
        bars = await fetch_day_bars(temp, end)
        if bars is None:
            await push_fail_secs(temp, fail)
            raise exception.GotNoneData()
        if isinstance(bars, np.ndarray):
            await Stock.batch_cache_bars(FrameType.DAY, bars)
        total += len(temp)

    await cache.sys.hincrby(state, "done_count", total // 2)
    return True


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


async def persistence_daily_calibration(bars1, bars2, secs, fail, queue, frame_type):
    # 将数据持久化
    if bars1 is None or bars2 is None:
        await push_fail_secs(secs, fail)
        raise exception.GotNoneData()
    # 校验通过，持久化
    if not await handle_dfs_data(bars1, bars2, queue):
        await push_fail_secs(secs, fail)
        raise exception.ChecksumFail()
    if isinstance(bars1, np.ndarray):
        await Stock.persist_bars(frame_type, bars1)
    return len(secs)


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
    _, state, end, n_bars, fail, timeout = await parse_params(params)

    limit = await get_limit()
    total = 0
    tasks = []
    async for secs in get_secs(limit, n_bars, scope):
        tasks.extend(secs)
        min_bars1 = await fetch_min_bars(secs, end, n_bars)
        min_bars2 = await fetch_min_bars(secs, end, n_bars)
        total += await persistence_daily_calibration(
            min_bars1, min_bars2, secs, fail, queue_min, FrameType.MIN1
        )

    for i in range(0, len(tasks), limit):
        temp = tasks[i : i + limit]
        bars1 = await fetch_day_bars(temp, end)
        bars2 = await fetch_day_bars(temp, end)
        total += await persistence_daily_calibration(
            bars1, bars2, temp, fail, queue_day, FrameType.DAY
        )

    return total // 2


@abnormal_work_report()
async def sync_daily_calibration(params):
    """凌晨2点的数据校准，
    同步分钟线 持久化
    同步日线 持久化"""
    scope, state, end, n_bars, fail, timeout = await parse_params(params)
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


async def __sync_year_quarter_month_week(params, queue, data):
    scope, state, end, n_bars, fail, timeout = await parse_params(params)
    frame_type = params.get("frame_type")

    limit = await get_limit()
    total = 0
    async for secs in get_secs(limit, n_bars, queue):
        bars1 = await fetch_bars(secs, end, n_bars, frame_type)
        bars2 = await fetch_bars(secs, end, n_bars, frame_type)
        await persistence_daily_calibration(bars1, bars2, secs, fail, data, frame_type)
        total += len(secs)
    return total


@abnormal_work_report()
async def sync_year_quarter_month_week(params):
    scope, state, end, n_bars, fail, timeout = await parse_params(params)
    stock_queue = params.get("stock_queue")
    index_queue = params.get("index_queue")
    stock_data = params.get("stock_data")
    index_data = params.get("index_data")
    total = 0
    total += await __sync_year_quarter_month_week(params, stock_queue, stock_data)
    total += await __sync_year_quarter_month_week(params, index_queue, index_data)

    await cache.sys.hincrby(state, "done_count", total)


def cron_work_report():
    def inner(f):
        @wraps(f)
        async def decorated_function():
            """装饰所有worker，统一处理错误信息"""
            has_same_task = await cache.sys.exists(f"""cron_{f.__name__}""")
            if has_same_task:
                return None
            await cache.sys.setex(f"""cron_{f.__name__}""", 3600 * 2, 1)
            try:
                ret = await f()
                return ret
            except Exception as e:  # pragma: no cover
                # 说明消费者消费时错误了
                logger.exception(e)
                subject = f"执行定时任务{f.__name__}时发生异常"
                body = f"详细信息：\n{traceback.format_exc()}"
                await mail_notify(subject, body, html=True)

        return decorated_function

    return inner


@cron_work_report()
async def sync_funds():
    """更新基金列表"""
    secs = await AbstractQuotesFetcher.get_fund_list()
    logger.info("%s secs are fetched and saved.", len(secs))
    return secs


@cron_work_report()
async def sync_fund_net_value():
    """更新基金净值数据"""
    now = datetime.datetime.now().date()
    ndays = 8
    n = 0
    while n < ndays:
        await AbstractQuotesFetcher.get_fund_net_value(
            day=now - datetime.timedelta(days=n)
        )
        n += 1
        if n > 2:
            break
    return True


@cron_work_report()
async def sync_fund_share_daily():
    """更新基金份额数据"""
    now = datetime.datetime.now().date()
    ndays = 8
    n = 0
    while n < ndays:
        await AbstractQuotesFetcher.get_fund_share_daily(
            day=now - datetime.timedelta(days=n)
        )
        n += 1
    return True


@cron_work_report()
async def sync_fund_portfolio_stock():
    """更新基金十大持仓股数据"""
    now = datetime.datetime.now().date()
    ndays = 8
    n = 0
    while n < ndays:
        await AbstractQuotesFetcher.get_fund_portfolio_stock(
            pub_date=now - datetime.timedelta(days=n)
        )
        n += 1
    return True


async def load_cron_task(scheduler: AsyncIOScheduler):

    scheduler.add_job(
        sync_fund_net_value,
        "cron",
        hour=4,
        minute=15,
        name="sync_fund_net_value",
    )
    scheduler.add_job(
        sync_funds,
        "cron",
        hour=4,
        minute=0,
        name="sync_funds",
    )
    scheduler.add_job(
        sync_fund_share_daily,
        "cron",
        hour=4,
        minute=5,
        name="sync_fund_share_daily",
    )
    scheduler.add_job(
        sync_fund_portfolio_stock,
        "cron",
        hour=4,
        minute=10,
        name="sync_fund_portfolio_stock",
    )
