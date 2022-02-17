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
from typing import Dict, List
import pandas as pd

import async_timeout
import cfg4py
import numpy as np
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from coretypes import FrameType
from omicron import cache
from omicron.extensions.np import numpy_append_fields
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.notify.mail import mail_notify
from retrying import retry

from omega.core.constants import TRADE_PRICE_LIMITS
from omega.worker import exception
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as fetcher

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
            scope, state, timeout = params.get("__scope"), params.get("__state"), params.get("__timeout")
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


async def cache_init():
    """初始化缓存 交易日历和证券代码"""
    if not await cache.security.exists(f"calendar:{FrameType.DAY.value}"):
        await fetcher.get_all_trade_days()
    if not await cache.security.exists("security:stock"):
        await fetcher.get_security_list()


@retry(stop_max_attempt_number=5)
async def fetch_bars(
    secs: List[str], end: datetime.datetime, n_bars: int, frame_type: FrameType
)->Dict[str, np.ndarray]:
    # todo: omicron可以保存dict[code->bars],无需进行转换。这里的转换只是为了适应底层逻辑，没有特别的功能，在底层可以修改的情况下，可以不做不必要的转换。
    return await fetcher.get_bars_batch(
        secs, end=end, n_bars=n_bars, frame_type=frame_type
    )


async def fetch_min_bars(secs: List[str], end: datetime.datetime, n_bars: int)->Dict[str, np.ndarray]:
    """
    从远端获取分钟线数据
    Args:
        secs: 股票的列表 ["00001.XSHG", ...]
        end:  时间
        n_bars: 多少根bar

    Returns:

    """
    # todo: 这个封装的必要性？
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
    # todo: 这个封装的必要性？
    return await fetch_bars(secs, end, n_bars, FrameType.WEEK)


@retry(stop_max_attempt_number=5)
async def get_trade_price_limits(secs, end):
    """获取涨跌停价"""
    # todo: 可以重命名为get_trade_limit_price
    return await fetcher.get_trade_price_limits(secs, end)


def get_remnant_s():
    """获取从现在开始到晚上12点还剩多少秒"""
    # todo: 内部函数，建议使用_get_x
    now = datetime.datetime.now()
    end = datetime.datetime(year=now.year, month=now.month, day=now.day + 1)
    # 当前时间秒数
    return int((end - now).total_seconds())


@retry(stop_max_attempt_number=5)
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
    bars = await fetcher.get_bars_batch(
        secs, end=end, n_bars=1, frame_type=FrameType.DAY
    )
    high_low_limit = await get_trade_price_limits(secs, end)
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
    scope, state, end, fail = params.get("scope"), params.get("state"), params.get("end"), params.get("fail")

    limit = await fetcher.max_result_size("bars")
    hashmap = [] # todo: bad name
    total = 0
    async for secs in get_secs(limit, 1, scope):
        bars = await get_trade_price_limits(secs, end)
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
        await cache.sys.hmset(TRADE_PRICE_LIMITS, *hashmap)
        # 取到到晚上12点还有多少秒
        # fixme: 为什么是到晚上12点？这个数据会等到calibration_sync时才会写入到persistent db中
        await cache.sys.expire(TRADE_PRICE_LIMITS, get_remnant_s())
    await cache.sys.hincrby(state, "done_count", total)
    return True


async def __sync_min_bars(params):
    """同步分支线数据并写缓存"""
    # todo: add unittest
    limit = await fetcher.max_result_size("bars")
    scope, end, n_bars, fail = params.get("__scope"), params.get("end"), params.get("n_bars", 1), params.get("__fail")
    secs_done = []
    async for secs in get_secs(limit, n_bars, scope):
        bars = await fetch_min_bars(secs, end, n_bars)
        if len(bars) == 0:
            await push_fail_secs(secs, fail)
            raise exception.GotNoneData()
        
        await Stock.batch_cache_bars(FrameType.MIN1, bars)
        secs_done.extend(secs)
    return secs_done


@abnormal_work_report()
async def sync_minute_bars(params):
    """
    盘中同步分钟
    Args:
        params:

    Returns:

    """
    state = params.get("__state")
    total, tasks = await __sync_min_bars(params)

    await cache.sys.hincrby(state, "done_count", total)


async def parse_params(params):
    # todo: remove me
    logger.info(params)
    return (
        params.get("__state"),
        params.get("end"),
        params.get("__fail"),
        params.get("__scope"),
        params.get("n_bars", 1),
        params.get("__timeout"),
    )


@abnormal_work_report()
async def after_hour_sync(params):
    """这是下午三点收盘后的同步，仅写cache，同步分钟线和日线"""
    state, end, fail = params.get("__state"), params.get("end"), params.get("__fail")
    limit = await fetcher.max_result_size("bars")

    secs = await __sync_min_bars(params)

    for i in range(0, len(secs), limit):
        batch = secs[i : i + limit]
        bars = await fetch_day_bars(batch, end)
        if len(bars) == 0:
            await push_fail_secs(batch, fail)
            raise exception.GotNoneData()
        
        await Stock.batch_cache_bars(FrameType.DAY, bars)

    await cache.sys.hincrby(state, "done_count", len(secs))


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


@retry(stop_max_attempt_number=5)
async def persist_bars(frame_type: FrameType, bars: pd.DataFrame):
    logger.info(f"正在写入influxdb:frame_type:{frame_type}")
    # todo
    await Stock.persist_bars(frame_type, bars)

    logger.info(f"已经写入inflaxdb:frame_type:{frame_type}")


async def persistence_daily_calibration(bars1, bars2, secs, fail, queue, frame_type):
    # 将数据持久化
    if bars1 is None or bars2 is None:
        await push_fail_secs(secs, fail)
        raise exception.GotNoneData()
    # 校验通过，持久化
    if not await handle_dfs_data(bars1, bars2, queue):
        await push_fail_secs(secs, fail)
        raise exception.ChecksumFail()
    if frame_type != FrameType.MIN1 and isinstance(bars1, np.ndarray):
        await persist_bars(frame_type, bars1)
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
    end, n_bars, fail = params.get("end"), params.get("n_bars", 1), params.get("__fail")

    limit = await fetcher.max_result_size("bars")
    done = 0
    for_day_bars = []
    async for secs in get_secs(limit, n_bars, scope):
        for_day_bars.extend(secs)
        min_bars1 = await fetch_min_bars(secs, end, n_bars)
        min_bars2 = await fetch_min_bars(secs, end, n_bars)
        done += await persistence_daily_calibration(
            min_bars1, min_bars2, secs, fail, queue_min, FrameType.MIN1
        )

    for i in range(0, len(for_day_bars), limit):
        temp = for_day_bars[i : i + limit]
        bars1 = await fetch_day_bars(temp, end)
        bars2 = await fetch_day_bars(temp, end)
        done += await persistence_daily_calibration(
            bars1, bars2, temp, fail, queue_day, FrameType.DAY
        )

    return done // 2


@abnormal_work_report()
async def sync_daily_calibration(params):
    """凌晨2点的数据校准，
    同步分钟线 持久化
    同步日线 持久化"""
    state = params.get("__state")

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
    end, n_bars, fail = params.get("end"), params.get("n_bars", 1), params.get("__fail")
    frame_type = params.get("frame_type")

    limit = await fetcher.max_result_size("bars")
    total = 0
    async for secs in get_secs(limit, n_bars, queue):
        bars1 = await fetch_bars(secs, end, n_bars, frame_type)
        bars2 = await fetch_bars(secs, end, n_bars, frame_type)
        await persistence_daily_calibration(bars1, bars2, secs, fail, data, frame_type)
        total += len(secs)
    return total


@abnormal_work_report()
async def sync_year_quarter_month_week(params):
    state = params.get("__state")
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
            key = f"""cron_{f.__name__}"""
            if await cache.sys.setnx(key, 1):
                await cache.sys.setex(key, 3600 * 2, 1)
                try:
                    ret = await f()
                    await cache.sys.delete(key)
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
    secs = await fetcher.get_fund_list()
    logger.info("%s secs are fetched and saved.", len(secs))
    return secs


@cron_work_report()
async def sync_fund_net_value():
    """更新基金净值数据"""
    now = datetime.datetime.now().date()
    ndays = 8
    n = 0
    while n < ndays:
        await fetcher.get_fund_net_value(
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
        await fetcher.get_fund_share_daily(
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
        await fetcher.get_fund_portfolio_stock(
            pub_date=now - datetime.timedelta(days=n)
        )
        n += 1
    return True


@cron_work_report()
async def sync_calendar():
    """从上游服务器获取所有交易日，并计算出周线帧和月线帧

    Returns:
    """
    trade_days = await fetcher.get_all_trade_days()
    if trade_days is None or len(trade_days) == 0:
        logger.warning("failed to fetch trade days.")
        return None

    await TimeFrame.init()


@cron_work_report()
async def sync_security_list():
    """更新证券列表

    注意证券列表在AbstractQuotesServer取得时就已保存，此处只是触发
    """
    await fetcher.get_security_list()
    logger.info("secs are fetched and saved.")


async def load_cron_task(scheduler: AsyncIOScheduler):
    h, m = map(int, cfg.omega.sync.security_list.split(":"))
    scheduler.add_job(
        sync_calendar,
        "cron",
        hour=h,
        minute=m,
        name="sync_calendar",
    )
    scheduler.add_job(
        sync_security_list,
        "cron",
        name="sync_security_list",
        hour=h,
        minute=m,
    )
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
