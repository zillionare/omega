#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging

import arrow
import cfg4py
from arrow import Arrow
from omicron import cache
from omicron import security_cache
from omicron.core import FrameType
from omicron.core.timeframe import tf
from omicron.models.securities import Securities

from omega.app import Application as app
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)

cfg = cfg4py.get_instance()


async def init_sync_scope(frame_type: FrameType):
    key_scope = "jobs.sync_day_bars.scope"

    secs = Securities()
    await cache.sys.delete(key_scope)
    await cache.sys.lpush(key_scope, *secs.choose())


async def sync_all_bars():
    for frame_type in [FrameType.DAY, FrameType.WEEK, FrameType.MONTH,
                       FrameType.QUARTER, FrameType.YEAR,
                       FrameType.MIN60, FrameType.MIN30, FrameType.MIN15,
                       FrameType.MIN5, FrameType.MIN1]:
        if app.is_leader:
            await init_sync_scope(frame_type)
            await sync_bars(frame_type, max_bars=cfg.omega.sync.max_bars)


async def sync_bars(frame_type: FrameType, sync_to: Arrow = None, max_bars: int = 1000):
    key_scope = "jobs.sync_day_bars.scope"
    end = tf.shift(sync_to or arrow.now(), 0, frame_type)
    while code := await cache.sys.lpop(key_scope):
        head, tail = await security_cache.get_bars_range(code, frame_type)
        if not all([head, tail]):
            await security_cache.clear_bars_range(code, frame_type)
            await AbstractQuotesFetcher.get_bars(code, end, max_bars, frame_type)
            continue

        start = tf.shift(end, -max_bars + 1, frame_type)
        if start < head:
            n = tf.count_frames(start, head, frame_type) - 1
            if n > 0:
                _end_at = tf.shift(head, -1, frame_type)
                await AbstractQuotesFetcher.get_bars(code, _end_at, n, frame_type)
        if end > tail:
            n = tf.count_frames(tail, end, frame_type) - 1
            if n > 0:
                bars = await AbstractQuotesFetcher.get_bars(code, end, n, frame_type)
                # todo: remove me
                assert bars['frame'][0] == tf.shift(tail, 1, frame_type)
