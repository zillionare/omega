# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-07 18:28
import asyncio
import datetime
import hashlib
import itertools
import logging
import pickle
import traceback
from functools import wraps
from typing import Awaitable, Dict, List

import async_timeout
import cfg4py
import numpy as np
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from coretypes import FrameType, SecurityType
from omicron import cache
from omicron.extensions.np import numpy_append_fields
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.notify.mail import mail_notify
from retrying import retry
from tomlkit import key

from omega.core.constants import (
    MINIO_TEMPORAL,
    TRADE_PRICE_LIMITS,
    TASK_PREFIX,
    MINIO_TEMPORAL,
)
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


async def collect_sync_failure(secs: List[str], fail: str):
    await cache.sys.lpush(fail, str(secs))


def abnormal_work_report():
    def inner(f):
        @wraps(f)
        async def decorated_function(params):
            """装饰所有worker，统一处理错误信息"""
            scope, state, timeout = (
                params.get("__scope"),
                params.get("__state"),
                params.get("__timeout"),
            )
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
    """初始化缓存 交易日历和证券代码

    当系统第一次启动（cold boot)时，cache中并不存在证券列表和日历。而在`omicron.init`时，又依赖这些信息。因此，我们需要在Omega中加入冷启动处理的逻辑如下。
    """
    if not await cache.security.exists(f"calendar:{FrameType.DAY.value}"):
        await fetcher.get_all_trade_days()
    if not await cache.security.exists("security:stock"):
        await fetcher.get_security_list()


@retry(stop_max_attempt_number=5)
async def fetch_bars(
    secs: List[str], end: datetime.datetime, n_bars: int, frame_type: FrameType
) -> Dict[str, np.ndarray]:
    # todo: omicron可以保存dict[code->bars],无需进行转换。这里的转换只是为了适应底层逻辑，没有特别的功能，在底层可以修改的情况下，可以不做不必要的转换。
    return await fetcher.get_bars_batch(
        secs, end=end, n_bars=n_bars, frame_type=frame_type
    )


async def fetch_min_bars(
    secs: List[str], end: datetime.datetime, n_bars: int
) -> Dict[str, np.ndarray]:
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


async def get_secs_for_sync(limit: int, n_bars: int, name: str):
    """计算并获取可在一批次中进行同步的股票代码

    master将本次参与同步的证券代码存入redis中的一个list，该list的名字为`name`。各个worker从该lister中分批获取证券代码，用以同步。

    Args:
        limit: 调用接口限制条数
        n_bars: 每支股票需要取多少条
        name: 队列名称

    Returns:

    """
    while True:
        step = limit // n_bars  # 根据 单次api允许获取的条数 和一共多少根k线 计算每次最多可以获取多少个股票的
        p = cache.sys.pipeline()
        p.lrange(name, 0, step - 1)
        p.ltrim(name, step, -1)
        secs, _ = await p.execute()
        if not len(secs):
            break
        yield secs


@abnormal_work_report()
async def sync_trade_price_limits(params: Dict):
    """同步涨跌停worker"""
    scope, state, end, fail = (
        params.get("scope"),
        params.get("state"),
        params.get("end"),
        params.get("fail"),
    )

    limit = await fetcher.max_result_size("bars")
    hashmap = []  # todo: bad name
    total = 0
    async for secs in get_secs_for_sync(limit, 1, scope):
        bars = await get_trade_price_limits(secs, end)
        if bars is None:
            await collect_sync_failure(secs, fail)
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

@abnormal_work_report()
async def sync_minute_bars(params):
    """盘中同步分钟
    Args:
        params:

    Returns:

    """
    assert "timeout" in params

    ft = FrameType.MIN1
    tasks = []
    for typ in [SecurityType.STOCK, SecurityType.INDEX]:
        tasks.append(_sync_to_cache(typ, ft, params))

    timeout = params["timeout"]
    await asyncio.wait(tasks, timeout)

    name = params.get("name")
    for t in tasks:
        try:
            t.result()
        except BaseException as e:
            logger.warning("when execute %s, got exception %s", name, e)
            logger.exception(e)
            former_error = await cache.sys.hmget(key, "error")
            await cache.sys.hmset(key, "error", former_error + "\n" + str(e))


async def _sync_to_cache(typ: SecurityType, ft: FrameType, params: Dict):
    """
    同步收盘后的数据
    Args:
        typ:
        ft:
        params:

    Returns:

    """
    task_name = params.get("name")
    queue = f"{TASK_PREFIX}.{task_name}.scope.{typ}"
    done_queue = f"{TASK_PREFIX}.{task_name}.scope.{typ}.done"

    n = 240 if ft == FrameType.MIN1 else 1
    limit = await fetcher.max_result_size("bars")

    async for secs in get_secs_for_sync(limit, n, queue):
        bars = await fetch_bars(secs, params.get("end"), n, ft)
        await Stock.batch_cache_bars(ft, bars)

        # 记录已完成的证券
        await cache.temp.lpush(done_queue, secs)

async def _daily_sync_impl(impl: Awaitable, params: Dict):
    """after-hour 与daily calibration有相似的实现逻辑。

    Args:
        params:

    Returns:

    """
    tasks = []
    for typ, ft in itertools.product(
        [SecurityType.STOCK, SecurityType.INDEX], FrameType.MIN1, FrameType.DAY
    ):
        tasks.append(asyncio.create_task(impl(typ, ft, params)))

    assert "timeout" in params
    await asyncio.wait(tasks, timeout=params.get("timeout"))

    name = params.get("name")
    key = f"{TASK_PREFIX}.{name}.scope.state"

    for t in tasks:
        try:
            t.result()
        except BaseException as e:
            logger.warning("when execute %s, got exception %s", name, e)
            logger.exception(e)
            former_error = await cache.sys.hmget(key, "error")
            await cache.sys.hmset(key, "error", former_error + "\n" + str(e))

@abnormal_work_report()
async def after_hour_sync(params):
    """这是下午三点收盘后的同步，仅写cache，同步分钟线和日线"""
    await _daily_sync_impl(_sync_to_cache, params)


async def checksum(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


async def _sync_for_persist(typ: SecurityType, ft: FrameType, params: Dict):
    """校准同步实现，用以分钟线、日线、周线、月线、年线等

    Args:
        typ : _description_
        ft : _description_
        params : _description_

    Returns:
        _description_
    """
    task_name = params.get("name")
    queue = f"{TASK_PREFIX}.{task_name}.scope.{typ}"
    done_queue = f"{TASK_PREFIX}.{task_name}.scope.{typ}.done"

    n = 240 if ft == FrameType.MIN1 else 1
    limit = await fetcher.max_result_size("bars")

    async for secs in get_secs_for_sync(limit, n, queue):
        # todo: is there better way to do the check?
        bars1 = await fetch_bars(secs, params.get("end"), n, ft)
        bars2 = await fetch_bars(secs, params.get("end"), n, ft)

        if checksum(bars1) == checksum(bars2):
            await Stock.persist_bars(ft, bars1)
            await _cache_bars_for_aggregation(typ, ft, bars1)

            # 记录已完成的证券
            await cache.temp.lpush(done_queue, secs)


async def _cache_bars_for_aggregation(
    typ: SecurityType, ft: FrameType, bars: Dict[str, np.ndarray]
):
    """将bars临时存入redis，以备聚合之用

    行情数据除了需要保存到时序数据库外，还要以块存储的方式，保存到minio中。

    minio中存储的行情数据是按index/stock和frame_type进行分组的。在有多个worker存在的情况下，每个worker都只能拿到这些分组的部分品种数据。因此，我们需要将其写入redis中，由master来执行往minio的的写入工作。

    Args:
        typ: SecurityType of Stock or Index
        ft: FrameType of DAY or MIN1
        bars: dict of bars

    Returns:
        None
    """
    queue = f"{MINIO_TEMPORAL}.{typ.value}.{ft.value}"
    data = pickle.dumps(bars, protocol=cfg.pickle.ver)
    await cache.temp.lpush(queue, data)


@abnormal_work_report()
async def sync_daily_calibration(params: dict):
    """校准同步。在数据经上游服务器核对，本地两次同步比较后，分别存入时序数据库和minio中。

    Args:
        params: contains start, end, name, timeout

    Returns:

    """
    await _daily_sync_impl(_sync_for_persist, params)

@abnormal_work_report()
async def sync_year_quarter_month_week(params):
    assert "frame_type" in params
    assert "timeout" in params
    timeout = params.get("timeout")

    ft = FrameType(params.get("frame_type"))
    tasks = []
    for typ in (SecurityType.INDEX, SecurityType.STOCK):
        tasks.append(asyncio.create_task(_sync_for_persist(typ, ft, params)))

    await asyncio.wait(tasks, timeout=params.get("timeout"))

    name = params.get("name")
    key = f"{TASK_PREFIX}.{name}.scope.state"

    for t in tasks:
        try:
            t.result()
        except BaseException as e:
            logger.warning("when execute %s, got exception %s", name, e)
            logger.exception(e)
            former_error = await cache.sys.hmget(key, "error")
            await cache.sys.hmset(key, "error", former_error + "\n" + str(e))

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
        await fetcher.get_fund_net_value(day=now - datetime.timedelta(days=n))
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
        await fetcher.get_fund_share_daily(day=now - datetime.timedelta(days=n))
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
