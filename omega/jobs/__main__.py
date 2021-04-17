#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import functools
import itertools
import logging
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
from sanic import Sanic, response

import omega.jobs.syncjobs as syncjobs
from omega.config import check_env, get_config_dir
from omega.config.schema import Config
from omega.logreceivers.redis import RedisLogReceiver

app = Sanic("Omega-jobs")
logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()
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


async def init(app, loop):  # noqa
    global scheduler

    config_dir = get_config_dir()
    cfg4py.init(get_config_dir(), False)

    await start_logging()
    logger.info("init omega-jobs process with config at %s", config_dir)

    await omicron.init()
    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)

    scheduler = AsyncIOScheduler(timezone=cfg.tz)

    # sync securities daily
    h, m = map(int, cfg.omega.sync.security_list.split(":"))
    scheduler.add_job(
        syncjobs.trigger_single_worker_sync,
        "cron",
        hour=h,
        minute=m,
        args=("calendar",),
        name="sync_calendar",
    )
    scheduler.add_job(
        syncjobs.trigger_single_worker_sync,
        "cron",
        args=("security_list",),
        name="sync_security_list",
        hour=h,
        minute=m,
    )

    syncjobs.load_bars_sync_jobs(scheduler)

    # sync bars at startup
    last_sync = await cache.sys.get("jobs.bars_sync.stop")
    if last_sync:
        last_sync = arrow.get(last_sync, tzinfo=cfg.tz).timestamp
    if not last_sync or time.time() - last_sync >= 24 * 3600:
        logger.info("start catch-up quotes sync")
        for frame_type in itertools.chain(tf.day_level_frames, tf.minute_level_frames):
            params = syncjobs.load_sync_params(frame_type)
            if params:
                asyncio.create_task(syncjobs.trigger_bars_sync(params, force=True))
    else:
        logger.info("%s: less than 24 hours since last sync", last_sync)

    scheduler.start()
    logger.info("omega jobs finished initialization")


@app.route("/jobs/sync_bars")
async def start_sync(request):  # pragma: no cover :they're in another process
    logger.info("received http command sync_bars")
    sync_params = request.json

    app.add_task(syncjobs.trigger_bars_sync(sync_params, True))
    return response.text("sync task scheduled")


@app.route("/jobs/status")  # pragma: no cover
async def get_status(request):
    return response.empty(status=200)


@app.listener("after_server_stop")
async def on_shutdown(app, loop):  # pragma: no cover
    global receiver
    logger.info("omega jobs is shutting down...")
    try:
        if receiver:
            await receiver.stop()
    except Exception:
        pass

    await omicron.shutdown()


def start(host: str = "0.0.0.0", port: int = 3180):  # pragma: no cover
    check_env()
    logger.info("starting omega jobs ...")
    app.register_listener(init, "before_server_start")
    app.run(host=host, port=port, register_sys_signals=True)
    logger.info("omega jobs exited.")


if __name__ == "__main__":
    fire.Fire({"start": start})
