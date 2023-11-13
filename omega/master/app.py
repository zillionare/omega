#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import functools
import logging
import os
import signal
import time

import arrow
import cfg4py
import fire
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyemit import emit

from omega import scripts
from omega.config import get_config_dir
from omega.core.events import Events
from omega.logreceivers.redis import RedisLogReceiver
from omega.master.jobs import load_cron_task
from omega.master.tasks.quota_utils import QuotaMgmt
from omega.master.tasks.rebuild_unclosed import rebuild_unclosed_bars

logger = logging.getLogger(__name__)
config_dir = get_config_dir()
cfg = cfg4py.init(get_config_dir(), False)
scheduler: AsyncIOScheduler = AsyncIOScheduler(timezone=cfg.tz)
receiver: RedisLogReceiver = None


async def start_logging():
    """enable receive logging from redis"""
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
        return receiver


async def on_logger_exit():
    print("omega logger service is shutdowning...")

    if receiver is not None:
        await receiver.stop()

    print("omega logger service shutdowned successfully")


async def heartbeat():
    global scheduler

    pid = os.getpid()
    key = "process.master"
    mp = {"pid": pid, "heartbeat": time.time()}
    await omicron.cache.sys.hset(key, mapping=mp)


async def handle_work_heart_beat(params: dict):
    QuotaMgmt.update_state(params)
    account = params.get("account")
    logger.info("update worker state: %s -> %s", account, params)


async def init():
    global scheduler

    # logger receiver使用单独的redis配置项，可以先行启动
    # await start_logging()
    logger.info("init omega-master process with config at %s", config_dir)

    # try to load omicron and init redis connection pool
    try:
        await omicron.init()
        await scripts.load_lua_script()

        # 延时执行未收盘数据的重建。如果omega是在交易时间启动的，这将允许omega先完成分钟线的同步，再进行未收盘数据的重建，从而避免因缺失部分分钟线数据导致的误差。当然，这里的方法并不是完全精确的。
        execution_time = (arrow.now().shift(minutes=5)).datetime
        scheduler.add_job(rebuild_unclosed_bars, "date", run_date=execution_time)
    except Exception as e:
        print(
            'No calendar and securities in cache, make sure you have called "omega init" first:\n',
            e,
        )
        os._exit(1)

    emit.register(Events.OMEGA_HEART_BEAT, handle_work_heart_beat)
    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)

    await heartbeat()
    scheduler.add_job(heartbeat, "interval", seconds=10)
    # sync securities daily
    await load_cron_task(scheduler)
    scheduler.start()
    logger.info("omega master finished initialization")


async def on_exit():
    """退出omega master进程，释放非锁资源。"""
    print("omega master is shutdowning...")

    if receiver is not None:
        await receiver.stop()

    await emit.stop()
    await omicron.close()
    print("omega master shutdowned successfully")


def stop(loop):
    loop.stop()


def start():  # pragma: no cover
    logger.info("starting omega master ...")
    loop = asyncio.get_event_loop()

    for s in {signal.SIGINT, signal.SIGTERM, signal.SIGQUIT}:
        loop.add_signal_handler(s, functools.partial(stop, loop))

    loop.create_task(init())
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(on_exit())

    loop.close()


def start_logger():
    logger.info("starting omega logger service ...")
    loop = asyncio.get_event_loop()

    for s in {signal.SIGINT, signal.SIGTERM, signal.SIGQUIT}:
        loop.add_signal_handler(s, functools.partial(stop, loop))

    loop.create_task(start_logging())
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(on_logger_exit())

    loop.close()


if __name__ == "__main__":
    fire.Fire(
        {
            "start": start,
            "logger": start_logger,
        }
    )
