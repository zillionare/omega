#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    管理应用程序生命期、全局对象、任务、全局消息响应
        """
import asyncio
import atexit
import logging
import os
import platform
import socket
from concurrent.futures.thread import ThreadPoolExecutor

import arrow
import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron.core.events import Events
from omicron.dal import cache
from pyemit import emit

from omega import app_name
from omega.config.cfg4py_auto_gen import Config
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.jobs.synccalendar import sync_calendar
from omega.jobs.syncquotes import sync_all_bars

logger = logging.getLogger(__name__)

cfg: Config = cfg4py.get_instance()


class Application:
    scheduler = None
    executors = None
    worker_id = None
    is_leader = False
    check_worker_interval = 10

    @classmethod
    def add_job(cls, func, trigger=None, *args, **kwargs):
        return cls.scheduler.add_job(func, trigger, *args, **kwargs)

    @classmethod
    def get_config_path(cls):
        user_home = os.path.expanduser('~/.omega')
        if os.path.exists(user_home):
            return os.path.join(user_home)
        else:
            src_dir = os.path.dirname(__file__)
            return os.path.join(src_dir, 'config')

    @classmethod
    async def create_quotes_fetcher(cls):
        for fetcher in cfg.quotes_fetchers:
            await AbstractQuotesFetcher.create_instance(fetcher,
                                                        executors=cls.executors)

    @classmethod
    async def start(cls):
        atexit.register(cls.on_exit)
        logger.info("starting solo quotes server...")
        cfg4py.init(cls.get_config_path(), False)
        cls.executors = ThreadPoolExecutor(max_workers=cfg.concurrency.threads)
        cls.scheduler = AsyncIOScheduler(timezone=cfg.tz)

        # 启动 emits 事件监听
        emit.register(Events.OMEGA_WORKER_LEAVE, cls.on_worker_leave, app_name)
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)

        # 创建fetchers
        await cls.create_quotes_fetcher()
        await omicron.init(cfg)

        # 每日清理一次过期股吧排名数据
        # scheduler.add_job(guba_crawler.purge_aged_records, 'cron', hour=2)
        # 每2小时取一次贴吧数据
        # scheduler.add_job(guba_crawler.start_crawl, 'cron', hour='1-23/2',
        # minute=0, misfire_grace_time=300)
        # scheduler.add_job(ths_realtime_news.start_crawl, 'interval', seconds=7)
        # scheduler.add_job(eastmoney_news.start_crawl, 'interval', seconds = 7)
        # scheduler.add_job(xsg.start_crawl)

        # 启动任务
        await cls.create_sync_jobs()
        cls.scheduler.start()

        await cls.join()
        logger.info("%s server started.", app_name)

    @classmethod
    async def create_sync_jobs(cls):
        hour, minute = map(int, cfg.omega.sync.time.split(":"))
        logger.info("quotes sync is scheduled at %s:%s", hour, minute)
        cls.add_job(sync_all_bars, 'cron', [cls], hour=hour, minute=minute)

        asyncio.create_task(sync_calendar())

    @classmethod
    async def check_worker_status(cls):
        logger.debug("checking worker status %s", cls.worker_id)
        await cache.sys.hmset_dict(f"workers:{app_name}:{cls.worker_id}", {
            "last_seen": arrow.now(cfg.tz).timestamp,
            "pid":       os.getpid(),
            "host":      socket.gethostname()
        })

        await cache.sys.sadd(f"workers:{app_name}", cls.worker_id)

        workers = await cache.sys.smembers(f"workers:{app_name}")
        pl = cache.sys.pipeline()
        # clean worker that no response
        alive_workers = [cls.worker_id]
        for worker_id in workers:
            status = await cache.sys.hgetall(f"workers:{app_name}:{worker_id}")
            if len(status) == 0:
                pl.srem(f"workers:{app_name}", worker_id)
                pl.delete(f"workers:{app_name}:{worker_id}")
                logger.warning("%s is removed due to status is not clear", worker_id)
                continue

            since_last_seen = arrow.now(cfg.tz).timestamp - int(status['last_seen'])
            if since_last_seen > cls.check_worker_interval * 1.3:
                # if the worker failed to update its status in an interval or more
                logger.warning("%s is removed, due to last_seen is %s seconds earlier",
                               worker_id, since_last_seen)
                pl.srem(f"workers:{app_name}", worker_id)
                pl.delete(f"workers:{app_name}:{worker_id}")
                continue
            alive_workers.append(worker_id)

        await pl.execute()

        # check if this is leader
        workers = [int(x) for x in set(alive_workers)]
        workers.sort()

        if cls.worker_id == str(workers[0]):
            if not cls.is_leader:
                msg = f"worker({cls.worker_id})[{os.getpid()}] becomes leader, " \
                      f"{len(workers)} workers alive"
                logger.info(msg)
            else:
                msg = f"this({cls.worker_id})[{os.getpid()}] is leader, " \
                      f"{len(workers)} workers alive"
            cls.is_leader = True
        else:
            cls.is_leader = False
            msg = f"this ({cls.worker_id})[{os.getpid()}] is not a leader, " \
                  f"{len(workers)} workers alive"
            logger.info(msg)


    @classmethod
    async def join(cls):
        cls.worker_id = str(await cache.sys.incr(f"id:workers:{app_name}"))
        logger.info("%s joined, worker_id is %s", os.getpid(), cls.worker_id)
        cls.scheduler.add_job(cls.check_worker_status, 'interval',
                              seconds=cls.check_worker_interval,
                              next_run_time=arrow.now(cfg.tz).naive)

    @classmethod
    async def on_worker_leave(cls, worker_id):
        """
        if other workers leave, we need to select the new leader
        """
        logger.info("%s worker(%s) is leaving us", app_name, worker_id)
        if worker_id != cls.worker_id:
            await cls.check_worker_status()

    @classmethod
    async def leave(cls):
        await cache.sys.srem(f"workers:{app_name}", cls.worker_id)
        await emit.emit(Events.OMEGA_WORKER_LEAVE, cls.worker_id, exchange=app_name)

    @classmethod
    def on_exit(cls):
        logger.info("on_exit called")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cls.leave())
        loop.run_until_complete(emit.stop())
        logger.info("application %s exited", app_name)


def main():
    # patch the loop driver
    if platform.system() in "Linux":
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ModuleNotFoundError:
            logger.warning(
                'uvloop is required for better performance, continuing with degraded '
                'service.')

    loop = asyncio.get_event_loop()
    loop.create_task(Application.start())
    loop.run_forever()


if __name__ == "__main__":
    main()
