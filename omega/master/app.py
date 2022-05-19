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
from pyemit import emit

from omega.config import get_config_dir
from omega.core.events import Events
from omega.logreceivers.redis import RedisLogReceiver
from omega.master.jobs import load_cron_task
from omega.master.tasks.quota_utils import QuotaMgmt

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()
scheduler: Optional[AsyncIOScheduler] = None
receiver: RedisLogReceiver = None


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


async def handle_work_heart_beat(params: dict):
    QuotaMgmt.update_quota(params)
    account = params.get("account")
    logger.info("update worker state: %s -> %s", account, params)


async def init():  # noqa
    global scheduler
    config_dir = get_config_dir()
    cfg4py.init(get_config_dir(), False)
    emit.register(Events.OMEGA_HEART_BEAT, handle_work_heart_beat)
    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)

    await start_logging()
    logger.info("init omega-master process with config at %s", config_dir)
    try:
        await omicron.init()
    except Exception:
        pass

    # await omicron.init()

    scheduler = AsyncIOScheduler(timezone=cfg.tz)
    await heartbeat()
    scheduler.add_job(heartbeat, "interval", seconds=5)
    # sync securities daily
    await load_cron_task(scheduler)
    scheduler.start()
    logger.info("omega master finished initialization")


def start():  # pragma: no cover
    logger.info("starting omega master ...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init())
    logger.info("omega 启动")
    loop.run_forever()
    logger.info("omega master exited.")


if __name__ == "__main__":
    fire.Fire({"start": start})
