#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import functools
import itertools
import logging
import os
import time
from typing import Optional

import arrow
import cfg4py
import fire
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron import cache
from omicron.core.timeframe import tf
from pyemit import emit
from jobs import trigger_bars_sync, load_sync_params, load_bars_sync_jobs, trigger_single_worker_sync

from omega.config import get_config_dir
from omega.logreceivers.redis import RedisLogReceiver

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()
scheduler: Optional[AsyncIOScheduler] = None
receiver: RedisLogReceiver = None


class WorkerState:
    """用来记录worker的状态"""
    all = 0
    normal = 0
    error = 0


async def start_logging():
    global receiver
    if getattr(cfg, "logreceiver") is None:
        return

    if cfg.logreceiver.klass == "omega.logging.receiver.redis.RedisLogReceiver":
        dsn = cfg.logreceiver.dsn
        channel = cfg.logreceiver.channel
        filename = cfg.logreceiver.filename
        backup_count = cfg.logreceiver.backup_count
        max_bytes = cfg.logreceiver.max_bytes
        receiver = RedisLogReceiver(dsn, channel, filename, backup_count, max_bytes)
        await receiver.start()

        logger.info("%s is working now", cfg.logreceiver.klass)


async def heartbeat():
    global scheduler

    pid = os.getpid()
    key = "process.master"
    await omicron.cache.sys.hmset(key, "pid", pid, "heartbeat", time.time())


async def init():  # noqa
    global scheduler

    config_dir = get_config_dir()
    cfg4py.init(get_config_dir(), False)

    await start_logging()
    logger.info("init omega-master process with config at %s", config_dir)
    try:
        await omicron.init()
    except Exception:
        pass
    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)
    scheduler = AsyncIOScheduler(timezone=cfg.tz)
    await heartbeat()
    scheduler.add_job(heartbeat, "interval", seconds=5)

    # sync securities daily
    h, m = map(int, cfg.omega.sync.security_list.split(":"))
    scheduler.add_job(
        trigger_single_worker_sync,
        "cron",
        hour=h,
        minute=m,
        args=("calendar",),
        name="sync_calendar",
    )
    scheduler.add_job(
        trigger_single_worker_sync,
        "cron",
        args=("security_list",),
        name="sync_security_list",
        hour=h,
        minute=m,
    )

    load_bars_sync_jobs(scheduler)

    # sync bars at startup
    last_sync = await cache.sys.get("master.bars_sync.stop")

    if last_sync:
        try:
            last_sync = arrow.get(last_sync, tzinfo=cfg.tz).timestamp
        except ValueError:
            logger.warning("failed to parse last_sync: %s", last_sync)
            last_sync = None

    if not last_sync or time.time() - last_sync >= 24 * 3600:
        next_run_time = arrow.now(cfg.tz).shift(minutes=5).datetime
        logger.info("start catch-up quotes sync at %s", next_run_time)

        for frame_type in itertools.chain(tf.day_level_frames, tf.minute_level_frames):
            params = load_sync_params(frame_type)
            if params:
                scheduler.add_job(
                    trigger_bars_sync,
                    args=(params, True),
                    name=f"catch-up sync for {frame_type}",
                    next_run_time=next_run_time,
                )
    else:
        logger.info("%s: less than 24 hours since last sync", last_sync)
    await omicron.init()
    scheduler.start()
    logger.info("omega master finished initialization")


def start():  # pragma: no cover
    logger.info("starting omega master ...")
    loop = asyncio.get_event_loop()
    loop.create_task(init())
    print("omega 启动")
    loop.run_forever()
    logger.info("omega master exited.")


if __name__ == "__main__":
    fire.Fire({"start": start})
