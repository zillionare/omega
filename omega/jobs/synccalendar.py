#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging

import aiohttp
import cfg4py
from omicron.dal import security_cache

from omega.config.cfg4py_auto_gen import Config
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)

cfg: Config = cfg4py.get_instance()


async def sync_calendar():
    async with aiohttp.ClientSession() as client:
        async with client.get(cfg.omega.urls.calendar) as resp:
            if resp.status != 200:
                logger.warning("failed to fetch calendar from %s",
                               cfg.omega.urls.calendar)
                return

            calendar = await resp.json()

    trade_days = await AbstractQuotesFetcher.get_all_trade_days()
    if trade_days is None or len(trade_days) == 0:
        if calendar.get('day_frames') is not None:
            logger.info("save day_frames from %s", cfg.omega.urls.calendar)
            await security_cache.save_calendar('day_frames', calendar['day_frames'])

    for name in ['week_frames', 'month_frames']:
        if calendar.get(name):
            logger.info("save %s from %s", name, cfg.omega.urls.calendar)
            await security_cache.save_calendar(name, calendar.get(name))
