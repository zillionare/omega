import asyncio
import datetime
import itertools
import logging
import traceback
from functools import wraps
from typing import Callable, Dict, List, Tuple, Union

import async_timeout
import cfg4py
from coretypes import FrameType, SecurityType
from omicron import cache
from omicron.models.security import Security

from omega.worker import exception
from omega.worker.tasks.fetchers import get_trade_price_limits
from omega.worker.tasks.task_utils import (
    cache_bars_for_aggregation,
    get_secs_for_sync,
    sync_for_persist,
    sync_params_analysis,
    sync_to_cache,
)

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


async def worker_exit(state, scope, error=None):
    if error is None:  # pragma: no cover
        error = traceback.format_exc()
        logger.warning("worker exited with error:%s", error)
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


def worker_syncbars_task():
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
            logger.debug("timeout", timeout)
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
                await worker_exit(state, scope, error="worker task timeout")
                return False

        return decorated_function

    return inner


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


@worker_syncbars_task()
async def sync_minute_bars(params):
    """盘中同步分钟
    Args:
        params:
    Returns:
    """
    await _daily_sync_impl(sync_to_cache, params)


@worker_syncbars_task()
async def after_hour_sync(params):
    """这是下午三点收盘后的同步，仅写cache，同步分钟线和日线"""
    await _daily_sync_impl(sync_to_cache, params)


@worker_syncbars_task()
async def sync_daily_calibration(params: dict):
    """校准同步。在数据经上游服务器核对，本地两次同步比较后，分别存入时序数据库和minio中。
    Args:
        params: contains start, end, name, timeout
    Returns:
    """
    await _daily_sync_impl(sync_for_persist, params)


@worker_syncbars_task()
async def sync_year_quarter_month_week(params):
    """同步周月线"""
    await _daily_sync_impl(sync_for_persist, params)


@worker_syncbars_task()
async def sync_min_5_15_30_60(params):
    """同步周月线"""
    await _daily_sync_impl(sync_for_persist, params)


@worker_syncbars_task()
async def sync_trade_price_limits(params: Dict):
    """同步涨跌停worker
    分别遍历股票和指数，获取涨跌停之后，缓存至temp库中，写入influxdb
    """
    # 涨跌停价目前还是纯数组，需要改成dict的结构
    end = params.get("end")
    for typ in [SecurityType.STOCK, SecurityType.INDEX]:
        name, n, queue, done_queue, limit = await sync_params_analysis(
            typ, FrameType.DAY, params
        )
        async for secs in get_secs_for_sync(limit, n, queue):
            bars = await get_trade_price_limits(secs, end)
            if bars is None:
                raise exception.GotNoneData()
            await cache_bars_for_aggregation(name, typ, FrameType.DAY, bars)
            await cache.sys.lpush(done_queue, *secs)
            # 取到到晚上12点还有多少秒
