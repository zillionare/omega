#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import importlib
import logging
import time
from typing import Optional

import arrow
import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron.dal import cache
from pyemit import emit
from sanic import Sanic, response

import omega.jobs.syncquotes as sq
from omega.config.cfg4py_auto_gen import Config
from omega.core import get_config_dir
from omega.jobs.synccalendar import sync_calendar

app = Sanic('Omega-jobs')
logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()
scheduler: Optional[AsyncIOScheduler] = None


async def init(app, loop):
    global scheduler

    config_dir = get_config_dir()
    cfg4py.init(get_config_dir(), False)
    logger.info("init omega-jobs process with config at %s", config_dir)

    await omicron.init()
    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)

    scheduler = AsyncIOScheduler(timezone=cfg.tz)

    # do validation daily
    # todo: don't start at non-trade day
    h, m = map(int, cfg.omega.validation.time.split(":"))
    scheduler.add_job(sq.start_validation, 'cron', hour=h, minute=m)

    # sync bars
    h, m = map(int, cfg.omega.sync.time.split(":"))
    scheduler.add_job(sq.start_sync, 'cron', hour=h, minute=m)

    # sync calendar daily
    scheduler.add_job(sync_calendar, 'cron', hour=h, minute=m)
    scheduler.start()

    last_sync = await cache.sys.get("jobs.bars_sync.stop")
    if last_sync: last_sync = arrow.get(last_sync, tzinfo=cfg.tz).timestamp
    if not last_sync or time.time() - last_sync >= 24 * 3600:
        logger.info("start catch-up quotes sync")
        app.add_task(sq.start_sync())

    logger.info("omega jobs finished initialization")


@app.route('/jobs/start_sync')
async def start_sync(request):
    logger.info("received http command start_sync")
    secs = request.json.get('secs', None)
    sync_to = request.json.get('sync_to', None)
    if sync_to:
        sync_to = arrow.get(sync_to, 'YYYY-MM-DD')

    app.add_task(sq.start_sync(secs, sync_to))
    return response.text('sync task scheduled')


def load_additional_jobs():
    """从配置文件创建自定义的插件任务, for example, zillionare-checksum
    """
    global scheduler
    for job in cfg.omega.jobs:
        try:
            module = importlib.import_module(job['module'])
            entry = getattr(module, job['entry'])
            scheduler.add_job(entry, trigger=job['trigger'], args=job['params'],
                              **job['trigger_params'])
        except Exception as e:
            logger.exception(e)
            logger.info("failed to create job: %s", job)


def start(host: str = '0.0.0.0', port: int = 3180):
    logger.info("staring omega jobs...")
    app.register_listener(init, 'before_server_start')
    app.run(host=host, port=port, register_sys_signals=True)
