#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import asyncio
import logging
from typing import List

import arrow
import cfg4py
from arrow import Arrow
from omicron import Events
from omicron.core.errors import FetcherQuotaError
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.dal import cache
from omicron.dal import security_cache
from omicron.models.securities import Securities
from pyemit import emit

from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)

from omega.config.cfg4py_auto_gen import Config

cfg: Config = cfg4py.get_instance()


async def init_sync_scope(sec_types: List[str], frame_type: FrameType):
    key_scope = f"jobs.sync_bars_{frame_type.value}.scope"
    key_start = f"jobs.sync_bars_{frame_type.value}.start"
    key_stop = f"jobs.sync_bars_{frame_type.value}.stop"
    key_elapsed = f"jobs.sync_bars_{frame_type.value}.elapsed"

    secs = Securities()
    await cache.sys.delete(key_scope)

    codes = secs.choose(sec_types)
    logger.info("add %s securities into sync queue", len(codes))
    pl = cache.sys.pipeline()
    pl.delete(key_scope)
    pl.lpush(key_scope, *codes)
    pl.set(key_start, arrow.now().format('YYYY-MM-DD HH:mm:ss'))
    pl.set(key_stop, '')
    pl.set(key_elapsed, '')
    await pl.execute()


async def sync_all_bars():
    for frame_type in map(FrameType, cfg.omega.sync.frames):
        await init_sync_scope(cfg.omega.sync.type, frame_type)
        await sync_bars(frame_type, max_bars=cfg.omega.sync.max_bars)
        await emit.emit(Events.OMEGA_DO_SYNC)


async def sync_bars(frame_type: FrameType, sync_to: Arrow = None, max_bars: int = 1000):
    logger.info("syncing %s", frame_type)
    key_scope = f"jobs.sync_bars_{frame_type.value}.scope"
    key_start = f"jobs.sync_bars_{frame_type.value}.start"
    key_stop = f"jobs.sync_bars_{frame_type.value}.stop"
    key_elapsed = f"jobs.sync_bars_{frame_type.value}.elapsed"

    if sync_to is None:
        sync_to = arrow.now().date()
        if frame_type in tf.minute_level_frames:
            sync_to = arrow.get(sync_to).replace(hour=15)

    end = tf.shift(sync_to, 0, frame_type)
    # noinspection PyPep8
    while code := await cache.sys.lpop(key_scope):
        try:
            await sync_for_sec(code, end, max_bars)
        except FetcherQuotaError as e:
            logger.warning("When syncing %s, quota is reached", code)
            logger.exception(e)
            return  # stop the sync
        except Exception as e:
            logger.warning("Failed to sync %s", code)
            logger.exception(e)

    sync_start = arrow.get(await cache.sys.get(key_start), cfg.tz)
    sync_stop = arrow.now()
    elapsed = (sync_stop - sync_start).seconds
    pl = cache.sys.pipeline()
    pl.set(key_elapsed, elapsed)
    pl.set(key_stop, sync_stop.format('YYYY-MM-DD HH:mm:ss'))
    await pl.execute()
    logger.info("syncing %s finished in %s seconds", frame_type, elapsed)


async def sync_for_sec(code: str, end: Arrow, max_bars: int):
    for frame_type in cfg.omega.sync.frames:
        logger.debug("syncing %s for %s", frame_type, code)
        head, tail = await security_cache.get_bars_range(code, frame_type)
        if not all([head, tail]):
            await security_cache.clear_bars_range(code, frame_type)
            bars = await AbstractQuotesFetcher.get_bars(code, end, max_bars,
                                                        frame_type)
            logger.debug("sync %s to %s: expected: %s, actual %s", code, end,
                         max_bars, len(bars))
            continue

        start = tf.shift(end, -max_bars + 1, frame_type)
        if start < head:
            n = tf.count_frames(start, head, frame_type) - 1
            if n > 0:
                _end_at = tf.shift(head, -1, frame_type)
                bars = await AbstractQuotesFetcher.get_bars(code, _end_at, n,
                                                            frame_type)
                logger.debug("sync %s to %s: expected: %s, actual %s", code,
                             _end_at,
                             n, len(bars))
        if end > tail:
            n = tf.count_frames(tail, end, frame_type) - 1
            if n > 0:
                bars = await AbstractQuotesFetcher.get_bars(code, end, n,
                                                            frame_type)
                logger.debug("sync %s to %s: expected: %s, actual %s", code, end,
                             n, len(bars))
                # todo: remove me
                assert bars['frame'][0] == tf.shift(tail, 1, frame_type)



async def validate():
    """
    SYS validation {
        sec:frame_type:start:end checksum
    }
    日线 按年 2005, 2006, ...
    周线 按年
    60m 按半年 250 * 4
    30m 按季  250 * 8 / 4
    Returns:

    """
    pass
