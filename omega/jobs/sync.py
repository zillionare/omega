#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import datetime
import logging
import os
from typing import List, Tuple, Union

import aiohttp
import arrow
import cfg4py
from dateutil import tz
from omicron import cache
from omicron.core.errors import FetcherQuotaError
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.models.securities import Securities
from pyemit import emit
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError
from sanic.helpers import STATUS_CODES

from omega.config.schema import Config
from omega.core.events import Events
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()
validation_errors = []
no_validation_error_days = set()


async def _start_job_timer(job_name: str):
    key_start = f"jobs.bars_{job_name}.start"

    pl = cache.sys.pipeline()
    pl.delete(f"jobs.bars_{job_name}.*")

    pl.set(key_start, arrow.now(tz=cfg.tz).format("YYYY-MM-DD HH:mm:ss"))
    await pl.execute()


async def _stop_job_timer(job_name: str) -> int:
    key_start = f"jobs.bars_{job_name}.start"
    key_stop = f"jobs.bars_{job_name}.stop"
    key_elapsed = f"jobs.bars_{job_name}.elapsed"

    start = arrow.get(await cache.sys.get(key_start), tzinfo=cfg.tz)
    stop = arrow.now(tz=cfg.tz)
    elapsed = (stop - start).seconds

    pl = cache.sys.pipeline()
    pl.set(key_stop, stop.format("YYYY-MM-DD HH:mm:ss"))
    pl.set(key_elapsed, elapsed)
    await pl.execute()

    return elapsed


def read_sync_params(frame_type: FrameType) -> dict:
    params = {}
    for item in cfg.omega.sync.bars:
        if item.get("frame") == frame_type.value:
            params = item
            params["start"] = arrow.get(item.get("start") or arrow.now()).date()
            if item.get("stop"):
                params["stop"] = arrow.get(item.get("stop")).date()
            else:
                params["stop"] = None
            params["frame_type"] = frame_type
            return params

    return params


async def trigger_bars_sync(
    frame_type: FrameType, sync_params: dict = None, force=False
):
    """初始化bars_sync的任务，发信号给各quotes_fetcher进程以启动同步。

    同步的时间范围指定均为日级别。如果是非交易日，自动对齐到上一个已收盘的交易日，使用两端闭合区
    间（截止frame直到已收盘frame)。

    如果未指定同步结束日期，则同步到当前已收盘的交易日。

    Args:
        frame_type (FrameType): 将同步的周期
        sync_params (dict): 同步需要的参数
            secs (List[str]): 将同步的证券代码,如果为None，则使用sync_sec_type定义的类型来
            选择要同步的证券代码。
            sync_sec_type: List[str]
            start: 起始日
            stop: 截止日。如未指定，同步到已收盘日
            delay: seconds for sync to wait.
        force: 即使当前不是交易日，是否也强行进行同步。
    Returns:

    """
    if not force and not tf.is_trade_day(arrow.now()):
        return

    key_scope = f"jobs.bars_sync.scope.{frame_type.value}"

    if sync_params is None:
        sync_params = read_sync_params(frame_type)

    if not sync_params:
        logger.warning("sync_params is required for sync.")
        return

    codes = sync_params.get("secs")
    if codes is None:
        secs = Securities()
        codes = secs.choose(sync_params.get("type"))
        include = filter(lambda x: x, sync_params.get("include", "").split(","))
        include = map(lambda x: x.strip(" "), include)
        codes.extend(include)
        exclude = sync_params.get("exclude", "")
        exclude = map(lambda x: x.strip(" "), exclude)
        codes = set(codes) - set(exclude)

    if len(codes) == 0:
        logger.warning("no securities are specified for sync %s", frame_type)
        return

    logger.info("add %s securities into sync queue(%s)", len(codes), frame_type)
    pl = cache.sys.pipeline()
    pl.delete(key_scope)
    pl.lpush(key_scope, *codes)
    await pl.execute()

    await asyncio.sleep(sync_params.get("delay", 0))
    await _start_job_timer("sync")
    await emit.emit(
        Events.OMEGA_DO_SYNC,
        {
            "frame_type": frame_type,
            "start": sync_params.get("start"),
            "stop": sync_params.get("stop"),
        },
    )

    logger.info("%s send to fetchers.", Events.OMEGA_DO_SYNC)


def _parse_sync_params(sync_params: dict):
    """
    如果sync_params['start'], sync_params['stop']类型为date：
        如果frame_type为分钟级，则意味着要取到当天的开始帧和结束帧；
        如果frame_type为日期级，则要对齐到已结束的帧日期，并且设置时间为15:00否则某些sdk可能会只取
        到上一周期数据（如jqdatasdk 1.8)
    如果sync_params['start'], sync_params['stop']类型为datetime：
        如果frame_type为分钟级别，则通过tf.floor对齐到已结束的帧
        如果frame_type为日线级别，则对齐到上一个已结束的日帧，时间部分重置为15:00

    Args:
        sync_params:

    Returns:

    """
    frame_type = sync_params.get("frame_type")
    start = sync_params.get("start")
    stop = sync_params.get("stop")

    if start is None:
        raise ValueError("sync_params['start'] must be specified!")

    if type(start) not in [datetime.date, datetime.datetime]:
        raise TypeError(
            "type of sync_params['start'] must be one of ["
            "datetime.datetime, datetime.date]"
        )

    if stop is None:
        stop = tf.floor(arrow.now(cfg.tz), frame_type)
    if type(stop) not in [datetime.datetime, datetime.date]:
        raise TypeError(
            "type of sync_params['stop'] must be one of [None, "
            "datetime.datetime, datetime.date]"
        )

    if frame_type in tf.minute_level_frames:
        if type(start) is datetime.date:
            start = tf.floor(start, FrameType.DAY)
            minutes = tf.ticks[frame_type][0]
            h, m = minutes // 60, minutes % 60
            start = datetime.datetime(
                start.year, start.month, start.day, h, m, tzinfo=tz.gettz(cfg.tz)
            )
        else:
            start = tf.floor(start, frame_type)

        if type(stop) is datetime.date:
            stop = tf.floor(stop, FrameType.DAY)
            stop = datetime.datetime(
                stop.year, stop.month, stop.day, 15, tzinfo=tz.gettz(cfg.tz)
            )
        else:
            stop = tf.floor(stop, frame_type)
    else:
        start = tf.floor(start, frame_type)
        stop = tf.floor(stop, frame_type)

    return frame_type, start, stop


async def sync_bars_worker(sync_params: dict = None, secs: List[str] = None):
    """
    worker's sync job
    """
    logger.info("sync_bars_worker with params: %s, %s", sync_params, secs)

    try:
        frame_type, start, stop = _parse_sync_params(sync_params)
    except Exception as e:
        logger.warning("invalid sync_params: %s", sync_params)
        logger.exception(e)
        return

    key_scope = f"jobs.bars_sync.scope.{frame_type.value}"

    if secs is not None:

        async def get_sec():
            return secs.pop() if len(secs) else None

    else:

        async def get_sec():
            return await cache.sys.lpop(key_scope)

    while code := await get_sec():
        try:
            await sync_bars_for_security(code, frame_type, start, stop)
        except FetcherQuotaError as e:
            logger.warning("Quota exceeded when syncing %s. Sync aborted.", code)
            logger.exception(e)
            return  # stop the sync
        except Exception as e:
            logger.warning("Failed to sync %s", code)
            logger.exception(e)

    elapsed = await _stop_job_timer("sync")
    logger.info("%s finished quotes sync in %s seconds", os.getpid(), elapsed)


async def sync_bars_for_security(
    code: str,
    frame_type: FrameType,
    start: Union[datetime.date, datetime.datetime],
    stop: Union[None, datetime.date, datetime.datetime],
):
    counters = 0
    logger.info("syncing quotes for %s", code)

    # 取数据库中该frame_type下该code的k线起始点
    head, tail = await cache.get_bars_range(code, frame_type)
    if not all([head, tail]):
        await cache.clear_bars_range(code, frame_type)
        n_bars = tf.count_frames(start, stop, frame_type)
        bars = await aq.get_bars(code, stop, n_bars, frame_type)
        counters = len(bars)
        logger.info("finished sync %s(%s), %s bars synced", code, frame_type, counters)
        return

    if start < head:
        n = tf.count_frames(start, head, frame_type) - 1
        if n > 0:
            _end_at = tf.shift(head, -1, frame_type)
            bars = await aq.get_bars(code, _end_at, n, frame_type)
            counters += len(bars)
            logger.debug(
                "sync %s level bars of %s to %s: expected: %s, actual %s",
                frame_type,
                code,
                _end_at,
                n,
                len(bars),
            )
            if len(bars) and bars["frame"][-1] != _end_at:
                logger.warning(
                    "discrete frames found:%s, bars[-1](%s), " "head(%s)",
                    code,
                    bars["frame"][-1],
                    head,
                )

    if stop > tail:
        n = tf.count_frames(tail, stop, frame_type) - 1
        if n > 0:
            bars = await aq.get_bars(code, stop, n, frame_type)
            logger.debug(
                "sync %s level bars of %s to %s: expected: %s, actual %s",
                frame_type,
                code,
                stop,
                n,
                len(bars),
            )
            counters += len(bars)
            if bars["frame"][0] != tf.shift(tail, 1, frame_type):
                logger.warning(
                    "discrete frames found: %s, tail(%s), bars[0](" "%s)",
                    code,
                    tail,
                    bars["frame"][0],
                )

    logger.info("finished sync %s(%s), %s bars synced", code, frame_type, counters)


async def trigger_single_worker_sync(_type: str, params: dict = None):
    """启动只需要单个quotes fetcher进程来完成的数据同步任务

    比如交易日历、证券列表等如果需要同时启动多个quotes fetcher进程来完成数据同步任务，应该通过
    pyemit来发送广播消息。

    Args:
        _type: the type of data to be synced, either ``calendar`` or ``ecurity_list``
    """
    url = cfg.omega.urls.quotes_server
    if _type == "calendar":
        url += "/jobs/sync_calendar"
    elif _type == "security_list":
        url += "/jobs/sync_security_list"
    else:
        raise ValueError(f"{_type} is not supported sync type.")

    async with aiohttp.ClientSession() as client:
        try:
            async with client.post(url, data=params) as resp:
                if resp.status != 200:
                    logger.warning("failed to trigger %s sync", _type)
                else:
                    return await resp.json()
        except Exception as e:
            logger.exception(e)


async def sync_calendar():
    trade_days = await aq.get_all_trade_days()
    if trade_days is None or len(trade_days) == 0:
        logger.warning("failed to fetch trade days.")
        return

    weeks = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.weekday() < last.weekday() or (cur - last).days >= 7:
            weeks.append(last)
        last = cur

    if weeks[-1] < last:
        weeks.append(last)

    await cache.save_calendar("week_frames", map(tf.date2int, weeks))

    months = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.day < last.day:
            months.append(last)
        last = cur
    months.append(last)
    await cache.save_calendar("month_frames", map(tf.date2int, months))
    logger.info("trade_days is updated to %s", trade_days[-1])


async def sync_security_list():
    """更新证券列表"""
    secs = await aq.get_security_list()
    logger.info("%s secs are fetched and saved.", len(secs))
