#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import asyncio
import functools
import itertools
import logging
import time
from typing import Optional

import arrow
import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron import cache
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from pyemit import emit
from sanic import Sanic, response

import omega.jobs.sync as sq
from omega.config import check_env, get_config_dir
from omega.config.schema import Config
from omega.logging.receiver.redis import RedisLogReceiver

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


async def init(app, loop):  # noqa
    global scheduler

    config_dir = get_config_dir()
    cfg4py.init(get_config_dir(), False)
    logger.info("init omega-jobs process with config at %s", config_dir)

    await omicron.init()
    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)

    scheduler = AsyncIOScheduler(timezone=cfg.tz)

    # sync securities daily
    h, m = map(int, cfg.omega.sync.security_list.split(":"))
    scheduler.add_job(
        functools.partial(sq.trigger_single_worker_sync, "calendar"),
        "cron",
        hour=h,
        minute=m,
    )
    scheduler.add_job(
        functools.partial(sq.trigger_single_worker_sync, "security_list"),
        "cron",
        hour=h,
        minute=m,
    )

    # sync bars
    _add_bars_sync_job()

    last_sync = await cache.sys.get("jobs.bars_sync.stop")
    if last_sync:
        last_sync = arrow.get(last_sync, tzinfo=cfg.tz).timestamp
    if not last_sync or time.time() - last_sync >= 24 * 3600:
        logger.info("start catch-up quotes sync")
        for frame_type in itertools.chain(tf.day_level_frames, tf.minute_level_frames):
            params = sq.read_sync_params(frame_type)
            if params:
                asyncio.create_task(
                    sq.trigger_bars_sync(frame_type, params, force=True)
                )
    else:
        logger.info("%s: less than 24 hours since last sync", last_sync)

    scheduler.start()
    logger.info("omega jobs finished initialization")


def _add_bars_sync_job():
    frame_type = FrameType.MIN1
    params = sq.read_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 5
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour=9,
            minute="31-59",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync, "cron", hour=10, minute="*", args=(frame_type, params)
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour=11,
            minute="0-30",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="13-14",
            minute="*",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync, "cron", hour="15", args=(frame_type, params)
        )

    frame_type = FrameType.MIN5
    params = sq.read_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 60
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour=9,
            minute="35-55/5",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour=10,
            minute="*/5",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour=11,
            minute="0-30/5",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="13-14",
            minute="*/5",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync, "cron", hour="15", args=(frame_type, params)
        )

    frame_type = FrameType.MIN15
    params = sq.read_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 60
        scheduler.add_job(
            sq.trigger_bars_sync, "cron", hour=9, minute="45", args=(frame_type, params)
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour=10,
            minute="*/15",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour=11,
            minute="15,30",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="13-14",
            minute="*/15",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync, "cron", hour="15", args=(frame_type, params)
        )

    frame_type = FrameType.MIN30
    params = sq.read_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 60
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="10-11",
            minute="*/30",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="13",
            minute="30",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="14-15",
            minute="*/30",
            args=(frame_type, params),
        )

    frame_type = FrameType.MIN60
    params = sq.read_sync_params(frame_type)
    if params:
        params["delay"] = params.get("delay") or 60
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="10",
            minute="30",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="11",
            minute="30",
            args=(frame_type, params),
        )
        scheduler.add_job(
            sq.trigger_bars_sync,
            "cron",
            hour="14-15",
            minute=0,
            args=(frame_type, params),
        )

    for frame_type in tf.day_level_frames:
        params = sq.read_sync_params(frame_type)
        if params:
            params["delay"] = params.get("delay") or 60
            scheduler.add_job(
                sq.trigger_bars_sync, "cron", hour=15, args=(frame_type, params)
            )


@app.route("/jobs/sync_bars")
async def start_sync(request):
    logger.info("received http command sync_bars")
    secs = request.json.get("secs", None)
    sync_to = request.json.get("sync_to", None)
    if sync_to:
        sync_to = arrow.get(sync_to, "YYYY-MM-DD")

    app.add_task(sq.trigger_bars_sync(secs, sync_to))
    return response.text("sync task scheduled")


@app.listener("after_server_stop")
async def on_shutdown(app, loop):
    global receiver
    await receiver.stop()
    await omicron.shutdown()


def start(host: str = "0.0.0.0", port: int = 3180):
    check_env()
    logger.info("staring omega jobs...")
    app.register_listener(init, "before_server_start")
    app.run(host=host, port=port, register_sys_signals=True)
    logger.info("omega jobs exited.")
