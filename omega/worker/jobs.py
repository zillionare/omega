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
from typing import Callable, Dict, List, Tuple, Union

import async_timeout
import cfg4py
import numpy as np
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from coretypes import FrameType, SecurityType
from omicron import cache
from omicron.extensions.np import numpy_append_fields
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.notify.mail import mail_notify
from retrying import retry

from omega.core.constants import MINIO_TEMPORAL, TASK_PREFIX, TRADE_PRICE_LIMITS
from omega.worker import exception
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as fetcher

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


async def worker_exit(state, scope, error=None):
    if error is None:  # pragma: no cover
        error = traceback.format_exc()
    # 删除is_running 并写上错误堆栈信息
    former_error = await cache.sys.hget(state, "error")
    p = cache.sys.pipeline()
    if former_error:
        p.hmset(state, "error", former_error + "\n" + error)
    else:
        p.hmset(state, "error", error)
    p.hdel(state, "is_running")
    p.delete(*scope)  # 删除任务队列，让其他没退出的消费者也退出，错误上报至master
    await p.execute()


def abnormal_work_report():
    """装饰所有worker的装饰器，主要功能有
    1.处理worker的异常，并写入错误信息到redis，由master统一发送邮件上报
    2.统计worker的执行时间，总是比master给的超时时间少5秒，以便于确保worker比master提前退出
    3.处理worker自定义的移除状态
    """

    def inner(f):
        @wraps(f)
        async def decorated_function(params):
            """装饰所有worker，统一处理错误信息"""
            scope, state, timeout = (
                params.get("scope"),
                params.get("state"),
                params.get("timeout"),
            )
            # if isinstance(timeout, int):
            timeout -= 5  # 提前5秒退出
            print("timeout", timeout)
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
                await worker_exit(state, scope, error="消费者超时")
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
) -> Union[Dict[str, np.ndarray], np.ndarray]:
    """
    从上游获取k线数据
    Args:
        secs:待获取的股票代码列表
        end:需要获取的截止时间
        n_bars: 需要获取k线根数
        frame_type:需要获取的k线类型

    Returns:
        返回k线数据，为dict
    """
    # todo: omicron可以保存dict[code->bars],无需进行转换。这里的转换只是为了适应底层逻辑，没有特别的功能，在底层可以修改的情况下，可以不做不必要的转换。

    bars = await fetcher.get_bars_batch(
        secs, end=end, n_bars=n_bars, frame_type=frame_type
    )
    if bars is None:  # pragma: no cover
        raise exception.GotNoneData()
    return bars


@retry(stop_max_attempt_number=5)
async def get_trade_price_limits(secs, end):
    """获取涨跌停价
    由于inflaxdb无法处理浮点数 nan 所以需要提前将nan转换为0
    """
    # todo: 可以重命名为get_trade_limit_price
    bars = await fetcher.get_trade_price_limits(secs, end)
    bars["low_limit"] = np.nan_to_num(bars["low_limit"])
    bars["high_limit"] = np.nan_to_num(bars["high_limit"])
    return bars


async def _sync_params_analysis(
    typ: SecurityType, ft: FrameType, params: Dict
) -> Tuple:
    """
    解析同步的参数
    Args:
        typ: 证券类型 如 stock  index
        ft: k线类型
        params: master发过来的参数

    Returns: Tuple(任务名称，k线根数，任务队列名，完成的队列名，限制单单次查询的数量)

    """
    name = params.get("name")
    n = params.get("n_bars")
    if not n:
        if ft != FrameType.MIN1:
            n = 1
        else:
            n = 240
    queue = f"{TASK_PREFIX}.{name}.scope.{typ.value}.{ft.value}"
    done_queue = f"{TASK_PREFIX}.{name}.scope.{typ.value}.{ft.value}.done"
    limit = await fetcher.result_size_limit("bars")
    return name, n, queue, done_queue, limit


async def _sync_to_cache(typ: SecurityType, ft: FrameType, params: Dict):
    """
    同步收盘后的数据
    Args:
        typ: 证券类型 如 stock  index
        ft: k线类型
        params: master发过来的参数

    Returns:

    """
    print("_sync_to_cache 被调用", ft)
    name, n, queue, done_queue, limit = await _sync_params_analysis(typ, ft, params)
    print(done_queue)
    async for secs in get_secs_for_sync(limit, n, queue):
        # todo 如果是日线，需要调用获取日线的方法，其他的不用变
        bars = await fetch_bars(secs, params.get("end"), n, ft)
        await Stock.batch_cache_bars(ft, bars)
        # 记录已完成的证券
        await cache.sys.lpush(done_queue, *secs)


async def _sync_for_persist(typ: SecurityType, ft: FrameType, params: Dict):
    """校准同步实现，用以分钟线、日线、周线、月线、年线等
    1. 同步完成之后，对两次的bars数据计算做校验和验算，避免数据出错
    2. 校验通过之后将数据写入influxdb
    3. 缓存至redis的3号temp库中，以备master使用写入dfs中
    4. 将完成的证券代码写入完成队列，让master可以知道那些完成了

    Args:
        typ: 证券类型 如 stock  index
        ft: k线类型
        params: master发过来的参数

    """
    name, n, queue, done_queue, limit = await _sync_params_analysis(typ, ft, params)
    print(await _sync_params_analysis(typ, ft, params))
    async for secs in get_secs_for_sync(limit, n, queue):
        # todo: is there better way to do the check? 校验和数据类型问题
        bars1 = await fetch_bars(secs, params.get("end"), n, ft)
        bars2 = await fetch_bars(secs, params.get("end"), n, ft)

        if await checksum(pickle.dumps(bars1, protocol=4)) == await checksum(
            pickle.dumps(bars2, protocol=4)
        ):
            # try:
            await Stock.persist_bars(ft, bars1)
            # except Exception as e:
            #     print(e)
            await _cache_bars_for_aggregation(name, typ, ft, bars1)
            # 记录已完成的证券
            print(secs, ft)
            await cache.sys.lpush(done_queue, *secs)


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
    """同步涨跌停worker
    分别遍历股票和指数，获取涨跌停之后，缓存至temp库中，写入influxdb
    """
    # 涨跌停价目前还是纯数组，需要改成dict的结构
    end = params.get("end")
    for typ in [SecurityType.STOCK, SecurityType.INDEX]:
        name, n, queue, done_queue, limit = await _sync_params_analysis(
            typ, FrameType.DAY, params
        )
        async for secs in get_secs_for_sync(limit, n, queue):
            bars = await get_trade_price_limits(secs, end)
            if bars is None:
                raise exception.GotNoneData()
            await _cache_bars_for_aggregation(name, typ, FrameType.DAY, bars)
            await Stock.save_trade_price_limits(bars, to_cache=False)
            await cache.sys.lpush(done_queue, *secs)
            # 取到到晚上12点还有多少秒


@abnormal_work_report()
async def sync_minute_bars(params):
    """盘中同步分钟
    Args:
        params:

    Returns:

    """
    await _daily_sync_impl(_sync_to_cache, params)


async def _daily_sync_impl(impl: Callable, params: Dict):
    """after-hour 与daily calibration有相似的实现逻辑。

    Args:
        params:

    Returns:

    """
    tasks = []
    assert "frame_type" in params
    frame_type = params.get("frame_type")
    for typ, ft in itertools.product(
        [SecurityType.STOCK, SecurityType.INDEX], frame_type
    ):
        # todo 因为一个worker会同步多种类型的k线，通常情况下，除了分钟线，其他类型的k
        tasks.append(asyncio.create_task(impl(typ, ft, params)))

    assert "timeout" in params
    await asyncio.gather(*tasks)


@abnormal_work_report()
async def after_hour_sync(params):
    """这是下午三点收盘后的同步，仅写cache，同步分钟线和日线"""
    await _daily_sync_impl(_sync_to_cache, params)


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
    """同步周月线"""
    await _daily_sync_impl(_sync_for_persist, params)


async def checksum(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


async def _cache_bars_for_aggregation(
    name: str,
    typ: SecurityType,
    ft: FrameType,
    bars: Union[Dict[str, np.ndarray], np.ndarray],
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
    queue = f"{MINIO_TEMPORAL}.{name}.{typ.value}.{ft.value}"
    data = pickle.dumps(bars, protocol=cfg.pickle.ver)
    await cache.temp.lpush(queue, data)


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
