#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import datetime
import logging
import os
from typing import List, Optional, Tuple, Union

import aiohttp
import arrow
import cfg4py
from dateutil import tz
from omicron import cache
from omicron.core.errors import FetcherQuotaError
from omicron.core.timeframe import tf
from omicron.core.types import Frame, FrameType
from omicron.models.securities import Securities
from pyemit import emit

from omega.config.schema import Config
from omega.core.events import Events
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


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


def load_sync_params(frame_type: FrameType) -> dict:
    """根据指定的frame_type，从配置文件中加载同步参数

    Args:
        frame_type (FrameType): [description]

    Returns:
        dict: see @[omega.jobs.syncjobs.parse_sync_params]
    """
    for item in cfg.omega.sync.bars:
        if item.get("frame") == frame_type.value:
            try:
                secs, frame_type, start, stop, delay = parse_sync_params(**item)
                return item
            except Exception as e:
                logger.exception(e)
                logger.warning("failed to parse %s", item)
                return None

    return None


async def trigger_bars_sync(sync_params: dict = None, force=False):
    """初始化bars_sync的任务，发信号给各quotes_fetcher进程以启动同步。

    Args:
        frame_type (FrameType): 要同步的帧类型
        sync_params (dict): 同步参数
            ```
            {
                start: 起始帧
                stop: 截止帧
                frame: 帧类型
                delay: 延迟启动时间，以秒为单位
                cat: 证券分类，如stock, index等
                delay: seconds for sync to wait.
            }
            ```
            see more @[omega.jobs.syncjobs.parse_sync_params][]
        force: 即使当前不是交易日，是否也强行进行同步。
    Returns:

    """
    if not force and not tf.is_trade_day(arrow.now()):
        return

    codes, frame_type, start, stop, delay = parse_sync_params(**sync_params)
    key_scope = f"jobs.bars_sync.scope.{frame_type.value}"

    if len(codes) == 0:
        logger.warning("no securities are specified for sync %s", frame_type)
        return

    fmt_str = "sync from %s to %s in frame_type(%s) for %s secs"
    logger.info(fmt_str, start, stop, frame_type, len(codes))

    # secs are stored into cache, so each fetcher can polling it
    pl = cache.sys.pipeline()
    pl.delete(key_scope)
    pl.lpush(key_scope, *codes)
    await pl.execute()

    await asyncio.sleep(delay)
    await _start_job_timer("sync")

    await emit.emit(
        Events.OMEGA_DO_SYNC, {"frame_type": frame_type, "start": start, "stop": stop}
    )

    fmt_str = "send trigger sync event to fetchers: from %s to %s in frame_type(%s) for %s secs"
    logger.info(fmt_str, start, stop, frame_type, len(codes))


def parse_sync_params(
    frame: Union[str, Frame],
    cat: List[str] = None,
    start: Union[str, datetime.date] = None,
    stop: Union[str, Frame] = None,
    delay: int = 0,
    include: str = "",
    exclude: str = "",
) -> Tuple:
    """按照[使用手册](usage.md#22-如何同步K线数据)中的规则，解析和补全同步参数。

    如果`frame_type`为分钟级，则当`start`指定为`date`类型时，自动更正为对应交易日的起始帧；
    当`stop`为`date`类型时，自动更正为对应交易日的最后一帧。
    Args:
        frame (Union[str, Frame]): frame type to be sync.  The word ``frame`` is used
            here for easy understand by end user. It actually implies "FrameType".
        cat (List[str]): which catetories is about to be synced. Should be one of
            ['stock', 'index']. Defaults to None.
        start (Union[str, datetime.date], optional): [description]. Defaults to None.
        stop (Union[str, Frame], optional): [description]. Defaults to None.
        delay (int, optional): [description]. Defaults to 5.
        include (str, optional): which securities should be included, seperated by
            comma, for example, "000001.XSHE,000004.XSHE". Defaults to empty string.
        exclude (str, optional):  which securities should be excluded, seperated by
            comma. Defaults to empty string.

    Returns:
        - codes (List[str]): 待同步证券列表
        - frame_type (FrameType):
        - start (Frame):
        - stop (Frame):
        - delay (int):
    """
    frame_type = FrameType(frame)

    if frame_type in tf.minute_level_frames:
        if stop:
            stop = arrow.get(stop, tzinfo=cfg.tz)
            if stop.hour == 0:  # 未指定有效的时间帧，使用当日结束帧
                stop = tf.last_min_frame(tf.day_shift(stop.date(), 0), frame_type)
            else:
                stop = tf.floor(stop, frame_type)
        else:
            stop = tf.floor(arrow.now(tz=cfg.tz).datetime, frame_type)

        if stop > arrow.now(tz=cfg.tz):
            raise ValueError(f"请勿将同步截止时间设置在未来: {stop}")

        if start:
            start = arrow.get(start, tzinfo=cfg.tz)
            if start.hour == 0:  # 未指定有效的交易帧，使用当日的起始帧
                start = tf.first_min_frame(tf.day_shift(start.date(), 0), frame_type)
            else:
                start = tf.floor(start, frame_type)
        else:
            start = tf.shift(stop, -999, frame_type)
    else:
        stop = (stop and arrow.get(stop).date()) or arrow.now().date()
        if stop == arrow.now().date():
            stop = arrow.now(tz=cfg.tz)

        stop = tf.floor(stop, frame_type)
        start = tf.floor((start and arrow.get(start).date()), frame_type) or tf.shift(
            stop, -1000, frame_type
        )

    secs = Securities()
    codes = secs.choose(cat or [])

    exclude = map(lambda x: x, exclude.split(" "))
    codes = list(set(codes) - set(exclude))

    include = list(filter(lambda x: x, include.split(" ")))
    codes.extend(include)

    return codes, frame_type, start, stop, int(delay)


async def sync_bars(params: dict):
    """sync bars on signal OMEGA_DO_SYNC received

    Args:
        params (dict): composed of the following:
            ```
            {
                secs (List[str]): 待同步的证券标的.如果为None或者为空，则从数据库中轮询
                frame_type (FrameType):k线的帧类型
                start (Frame): k线起始时间
                stop (Frame): k线结束时间
            }
            ```
    Returns:
        [type]: [description]
    """
    secs, frame_type, start, stop = (
        params.get("secs"),
        params.get("frame_type"),
        params.get("start"),
        params.get("stop"),
    )

    if secs is not None:
        logger.info(
            "sync bars with %s(%s ~ %s) for given %s secs",
            frame_type,
            start,
            stop,
            len(secs),
        )

        async def get_sec():
            return secs.pop() if len(secs) else None

    else:
        logger.info(
            "sync bars with %s(%s ~ %s) in polling mode", frame_type, start, stop
        )

        async def get_sec():
            return await cache.sys.lpop(key_scope)

    key_scope = f"jobs.bars_sync.scope.{frame_type.value}"

    if start is None or frame_type is None:
        raise ValueError("you must specify a start date/frame_type for sync")

    if stop is None:
        stop = tf.floor(arrow.now(tz=cfg.tz), frame_type)

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

    # 取数据库中该frame_type下该code的k线起始点
    head, tail = await cache.get_bars_range(code, frame_type)
    if not all([head, tail]):
        await cache.clear_bars_range(code, frame_type)
        n_bars = tf.count_frames(start, stop, frame_type)

        bars = await aq.get_bars(code, stop, n_bars, frame_type)
        if bars is not None and len(bars):
            logger.debug(
                "sync %s(%s), from %s to %s: actual got %s ~ %s (%s)",
                code,
                frame_type,
                start,
                head,
                bars[0]["frame"],
                bars[-1]["frame"],
                len(bars),
            )
            counters = len(bars)
        return

    if start < head:
        n = tf.count_frames(start, head, frame_type) - 1
        if n > 0:
            _end_at = tf.shift(head, -1, frame_type)
            bars = await aq.get_bars(code, _end_at, n, frame_type)
            if bars is not None and len(bars):
                counters += len(bars)
                logger.debug(
                    "sync %s(%s), from %s to %s: actual got %s ~ %s (%s)",
                    code,
                    frame_type,
                    start,
                    head,
                    bars[0]["frame"],
                    bars[-1]["frame"],
                    len(bars),
                )
                if bars["frame"][-1] != _end_at:
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
            if bars is not None and len(bars):
                logger.debug(
                    "sync %s(%s), from %s to %s: actual got %s ~ %s (%s)",
                    code,
                    frame_type,
                    tail,
                    stop,
                    bars[0]["frame"],
                    bars[-1]["frame"],
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
    """从上游服务器获取所有交易日，并计算出周线帧和月线帧

    Returns:
    """
    trade_days = await aq.get_all_trade_days()
    if trade_days is None or len(trade_days) == 0:
        logger.warning("failed to fetch trade days.")
        return None

    tf.day_frames = [tf.date2int(x) for x in trade_days]
    weeks = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.weekday() < last.weekday() or (cur - last).days >= 7:
            weeks.append(last)
        last = cur

    if weeks[-1] < last:
        weeks.append(last)

    tf.week_frames = [tf.date2int(x) for x in weeks]
    await cache.save_calendar("week_frames", map(tf.date2int, weeks))

    months = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.day < last.day:
            months.append(last)
        last = cur
    months.append(last)

    tf.month_frames = [tf.date2int(x) for x in months]
    await cache.save_calendar("month_frames", map(tf.date2int, months))
    logger.info("trade_days is updated to %s", trade_days[-1])


async def sync_security_list():
    """更新证券列表

    注意证券列表在AbstractQuotesServer取得时就已保存，此处只是触发
    """
    secs = await aq.get_security_list()
    logger.info("%s secs are fetched and saved.", len(secs))


def load_bars_sync_jobs(scheduler):
    frame_type = FrameType.MIN1
    params = load_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 5
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=9,
            minute="31-59",
            args=(params,),
            name=f"{frame_type.value}:9:31-59",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=10,
            minute="*",
            args=(params,),
            name=f"{frame_type.value}:10:*",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=11,
            minute="0-30",
            args=(params,),
            name=f"{frame_type.value}:11:0-30",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="13-14",
            minute="*",
            args=(params,),
            name=f"{frame_type.value}:13-14:*",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="15",
            args=(params,),
            name=f"{frame_type.value}:15:00",
        )

    frame_type = FrameType.MIN5
    params = load_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 60
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=9,
            minute="35-55/5",
            args=(params,),
            name=f"{frame_type.value}:9:35-55/5",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=10,
            minute="*/5",
            args=(params,),
            name=f"{frame_type.value}:10:*/5",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=11,
            minute="0-30/5",
            args=(params,),
            name=f"{frame_type.value}:11:0-30/5",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="13-14",
            minute="*/5",
            args=(params,),
            name=f"{frame_type.value}:13-14:*/5",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="15",
            args=(params,),
            name=f"{frame_type.value}:15:00",
        )

    frame_type = FrameType.MIN15
    params = load_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 60
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=9,
            minute="45",
            args=(params,),
            name=f"{frame_type.value}:9:45",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=10,
            minute="*/15",
            args=(params,),
            name=f"{frame_type.value}:10:*/5",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour=11,
            minute="15,30",
            args=(params,),
            name=f"{frame_type.value}:11:15,30",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="13-14",
            minute="*/15",
            args=(params,),
            name=f"{frame_type.value}:13-14:*/15",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="15",
            args=(params,),
            name=f"{frame_type.value}:15:00",
        )

    frame_type = FrameType.MIN30
    params = load_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 60
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="10-11",
            minute="*/30",
            args=(params,),
            name=f"{frame_type.value}:10-11:*/30",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="13",
            minute="30",
            args=(params,),
            name=f"{frame_type.value}:13:30",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="14-15",
            minute="*/30",
            args=(params,),
            name=f"{frame_type.value}:14-15:*/30",
        )

    frame_type = FrameType.MIN60
    params = load_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 60
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="10",
            minute="30",
            args=(params,),
            name=f"{frame_type.value}:10:30",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="11",
            minute="30",
            args=(params,),
            name=f"{frame_type.value}:11:30",
        )
        scheduler.add_job(
            trigger_bars_sync,
            "cron",
            hour="14-15",
            minute=0,
            args=(params,),
            name=f"{frame_type.value}:14-15:00",
        )

    for frame_type in tf.day_level_frames:
        params = load_sync_params(frame_type)
        if params:
            params["delay"] = params.get("delay") or 60
            scheduler.add_job(
                trigger_bars_sync,
                "cron",
                hour=15,
                args=(params,),
                name=f"{frame_type.value}:15:00",
            )
