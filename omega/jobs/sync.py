#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import datetime
import logging
import os
from collections import ChainMap
from typing import List, Union

import arrow
import cfg4py
from arrow import Arrow
from omicron.core.errors import FetcherQuotaError
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.dal import cache, security_cache
from omicron.dal import security_cache as sc
from omicron.models.securities import Securities
from pyemit import emit

from omega.config.cfg4py_auto_gen import Config
from omega.core.events import Events
from omega.fetcher.abstract_quotes_fetcher import (
    AbstractQuotesFetcher as aq,
    AbstractQuotesFetcher)

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()
validation_errors = []
no_validation_error_days = set()


async def _start_job_timer(job_name: str):
    key_start = f"jobs.bars_{job_name}.start"

    pl = cache.sys.pipeline()
    pl.delete(f"jobs.bars_{job_name}.*")

    pl.set(key_start, arrow.now(tz=cfg.tz).format('YYYY-MM-DD HH:mm:ss'))
    await pl.execute()


async def _stop_job_timer(job_name: str) -> int:
    """
    stop timer, and return elapsed time in seconds
    Args:
        job_name:

    Returns:

    """
    key_start = f"jobs.bars_{job_name}.start"
    key_stop = f"jobs.bars_{job_name}.stop"
    key_elapsed = f"jobs.bars_{job_name}.elapsed"

    start = arrow.get(await cache.sys.get(key_start), tzinfo=cfg.tz)
    stop = arrow.now(tz=cfg.tz)
    elapsed = (stop - start).seconds

    pl = cache.sys.pipeline()
    pl.set(key_stop, stop.format('YYYY-MM-DD HH:mm:ss'))
    pl.set(key_elapsed, elapsed)
    await pl.execute()

    return elapsed


async def sync_bars(secs: List[str] = None, frames_to_sync: dict = None):
    """
    初始化bars_sync的任务，并发信号给各后台quotes_fetcher进程以启动同步。

    同步的时间范围指定均为日级别。如果是非交易日，自动对齐到上一个已收盘的交易日，使用两端闭合区
    间（截止frame直到已收盘frame)。

    如果未指定同步结束日期，则同步到当前已收盘的交易日。

    Args:
        secs (List[str]): 将同步的证券代码
            如果为None，则使用``omega.sync.type``定义的类型来选择要同步的证券代码。
        frames_to_sync (dict): frames and range(start, end)
            ::
                {
                    '30m': '2018-01-01,2019-01-01'
                }
            表明要同步从2018年1月1日到2019年1月1日的30分钟线数据。

    Returns:

    """
    key_scope = f"jobs.bars_sync.scope"

    if secs is None:
        secs = Securities()
        secs = secs.choose(cfg.omega.sync.type)

    if frames_to_sync is None:
        frames_to_sync = dict(ChainMap(*cfg.omega.sync.frames))

    logger.info("add %s securities into sync queue", len(secs))
    pl = cache.sys.pipeline()
    pl.delete(key_scope)
    pl.lpush(key_scope, *secs)
    await pl.execute()

    await _start_job_timer('sync')
    await emit.emit(Events.OMEGA_DO_SYNC, frames_to_sync)
    logger.info("%s send to fetchers.", Events.OMEGA_DO_SYNC)


async def sync_bars_worker(sync_frames: dict = None, secs: List[str] = None):
    """
    worker's sync job
    """
    logger.info("sync_bars_worker with params: %s, %s", secs, sync_frames)

    key_scope = "jobs.bars_sync.scope"

    if secs is not None:
        async def get_sec():
            return secs.pop() if len(secs) else None
    else:
        async def get_sec():
            return await cache.sys.lpop(key_scope)

    # noinspection PyPep8
    while code := await get_sec():
        try:
            await sync_bars_for_security(code, sync_frames)
        except FetcherQuotaError as e:
            logger.warning("Quota exceeded when syncing %s. Sync borted.", code)
            logger.exception(e)
            return  # stop the sync
        except Exception as e:
            logger.warning("Failed to sync %s", code)
            logger.exception(e)

    elapsed = await _stop_job_timer('sync')
    logger.info('%s finished quotes sync in %s seconds', os.getpid(), elapsed)


def _get_closed_frame(stop: Union[Arrow, datetime.date, datetime.datetime],
                      frame_type: FrameType):
    """
    同步时间范围按天来指定，以符合人类习惯。但具体操作时，需要确定到具体的时间帧上。对截止时间，
    如果是周线、月线、日线，取上一个收盘交易日所在的日、周、月线。如果是分钟线，取上一个收盘交
    易日的最后一根K线。
    Args:
        stop: 用户指定的截止日期
        frame_type:

    Returns:

    """
    stop = arrow.get(stop).date()
    now = arrow.now()
    if stop == now.date() and tf.is_trade_day(now.date()) \
            and now.hour * 60 + now.minute <= 900:
        # 如果截止日期正处在交易时间，则只能同步到上一交易日
        stop = tf.day_shift(stop, -1)

    if frame_type in [FrameType.WEEK, FrameType.MONTH]:
        stop = tf.shift(stop, 0, frame_type)

    if frame_type in tf.minute_level_frames:
        stop = tf.ceil(stop, frame_type)

    return stop


async def sync_bars_for_security(code: str, sync_frames: dict = None):
    counters = {frame: 0 for frame in sync_frames.keys()}
    logger.info("syncing quotes for %s", code)

    for frame, start_stop in sync_frames.items():
        frame_type = FrameType(frame)
        now = arrow.now()
        if ',' in start_stop:
            start, stop = map(lambda x: x.strip(' '), start_stop.split(','))
        else:
            start, stop = start_stop, now.date()

        start = tf.floor(start, frame_type)
        stop = _get_closed_frame(stop, frame_type)

        # 取数据库中该frame_type下该code的k线起始点
        head, tail = await sc.get_bars_range(code, frame_type)
        if not all([head, tail]):
            await sc.clear_bars_range(code, frame_type)
            n_bars = tf.count_frames(start, stop, frame_type)
            bars = await aq.get_bars(code, stop, n_bars, frame_type)
            counters[frame_type.value] = len(bars)
            logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                         frame_type, code, stop, n_bars, len(bars))
            continue

        if start < head:
            n = tf.count_frames(start, head, frame_type) - 1
            if n > 0:
                _end_at = tf.shift(head, -1, frame_type)
                bars = await aq.get_bars(code, _end_at, n, frame_type)
                counters[frame_type.value] += len(bars)
                logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                             frame_type, code, _end_at, n, len(bars))
                if len(bars) and bars['frame'][-1] != _end_at:
                    logger.warning("discrete frames found:%s, bars[-1](%s), "
                                   "head(%s)", code, bars['frame'][-1], head)

        if stop > tail:
            n = tf.count_frames(tail, stop, frame_type) - 1
            if n > 0:
                bars = await aq.get_bars(code, stop, n, frame_type)
                logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                             frame_type, code, stop, n, len(bars))
                counters[frame_type.value] += len(bars)
                if bars['frame'][0] != tf.shift(tail, 1, frame_type):
                    logger.warning("discrete frames found: %s, tail(%s), bars[0]("
                                   "%s)", code, tail, bars['frame'][0])

    logger.info("finished sync %s, %s", code,
                ",".join([f"{k}:{v}" for k, v in counters.items()]))


async def sync_calendar():
    trade_days = await AbstractQuotesFetcher.get_all_trade_days()
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

    await security_cache.save_calendar('week_frames', map(tf.date2int, weeks))

    months = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.day < last.day:
            months.append(last)
        last = cur
    months.append(last)
    await security_cache.save_calendar('month_frames', map(tf.date2int, months))
    logger.info("trade_days is updated to %s", trade_days[-1])


async def sync_securities():
    """更新证券列表"""
    secs = await aq.get_security_list()
    logger.info("%s secs are fetched and saved.", len(secs))
