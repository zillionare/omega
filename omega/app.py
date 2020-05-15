#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import asyncio
import logging
import os
import signal
import socket
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import Process, Queue
from threading import Thread
from typing import Callable, Iterable

import arrow
import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron import Events
from omicron.core.lang import singleton
from omicron.dal import cache
from pyemit import emit

from . import app_name
from .config.cfg4py_auto_gen import Config
from .fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher
from .jobs.synccalendar import sync_calendar
from .jobs.syncquotes import sync_all_bars

logger = logging.getLogger(__name__)

cfg: Config = cfg4py.get_instance()


@singleton
class Application:
    def __init__(self):
        self.scheduler = None
        self.thread_executors = None
        self.procs = []
        self.queue = None
        self.is_worker = False
        self.worker_id = 0

    def add_job(self, func, trigger=None, *args, **kwargs):
        return self.scheduler.add_job(func, trigger, *args, **kwargs)

    def get_config_path(self):
        user_home = os.path.expanduser('~/.omega')
        if os.path.exists(user_home):
            return os.path.join(user_home)
        else:
            src_dir = os.path.dirname(__file__)
            return os.path.join(src_dir, 'config')

    async def create_quotes_fetcher(self):
        for fetcher in cfg.quotes_fetchers:
            await AbstractQuotesFetcher.create_instance(fetcher,
                                                        executors=self.thread_executors)

    async def sub_main(self):
        role = "worker" if self.is_worker else "main"
        logger.info("starting zillionare-omega %s process: (%s) ...", role, os.getpid())
        if self.is_worker:
            self.register_exit_handler((signal.SIGINT, signal.SIGTERM), self.stop)

        cfg4py.init(self.get_config_path(), False)
        self.thread_executors = ThreadPoolExecutor(
            max_workers=cfg.omega.concurrency.threads)
        self.scheduler = AsyncIOScheduler(timezone=cfg.tz)

        # 启动 emits 事件监听
        emit.register(Events.OMEGA_WORKER_LEAVE, self.on_worker_leave, app_name)
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)

        # 创建fetchers
        await self.create_quotes_fetcher()
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
        await self.create_sync_jobs()
        self.scheduler.start()

        await self.join()
        logger.info("zillionare-omega %s process(%s) started.", role, os.getpid())

    async def create_sync_jobs(self):
        hour, minute = map(int, cfg.omega.sync.time.split(":"))
        logger.info("quotes sync is scheduled at %s:%s", hour, minute)
        self.add_job(sync_all_bars, 'cron', hour=hour, minute=minute)

        if not self.is_worker:
            asyncio.create_task(sync_calendar())

    async def check_worker_status(self):
        logger.debug("checking worker status %s", self.worker_id)
        await cache.sys.hmset_dict(f"workers:{app_name}:{self.worker_id}", {
            "last_seen": arrow.now(cfg.tz).timestamp,
            "pid":       os.getpid(),
            "host":      socket.gethostname()
        })

        await cache.sys.sadd(f"workers:{app_name}", self.worker_id)

        workers = await cache.sys.smembers(f"workers:{app_name}")
        pl = cache.sys.pipeline()
        # clean worker that no response
        alive_workers = [self.worker_id]
        for worker_id in workers:
            status = await cache.sys.hgetall(f"workers:{app_name}:{worker_id}")
            if len(status) == 0:
                pl.srem(f"workers:{app_name}", worker_id)
                pl.delete(f"workers:{app_name}:{worker_id}")
                logger.warning("%s is removed due to status is not clear", worker_id)
                continue

            since_last_seen = arrow.now(cfg.tz).timestamp - int(status['last_seen'])
            if since_last_seen > cfg.omega.concurrency.heartbeat_interval * 1.3:
                # if the worker failed to update its status in an interval or more
                logger.warning("%s is removed, due to last_seen is %s seconds earlier",
                               worker_id, since_last_seen)
                pl.srem(f"workers:{app_name}", worker_id)
                pl.delete(f"workers:{app_name}:{worker_id}")
                continue
            alive_workers.append(worker_id)

        await pl.execute()

    async def join(self):
        self.worker_id = str(await cache.sys.incr(f"id:workers:{app_name}"))
        logger.info("%s joined, worker_id is %s", os.getpid(), self.worker_id)
        self.scheduler.add_job(self.check_worker_status, 'interval',
                               seconds=cfg.omega.concurrency.heartbeat_interval,
                               next_run_time=arrow.now(cfg.tz).naive)

    async def on_worker_leave(self, worker_id):
        """
        if other workers leave, we need to select the new leader
        """
        logger.info("omega worker(%s) is leaving us", worker_id)
        if worker_id != self.worker_id:
            await self.check_worker_status()

    async def leave(self):
        await cache.sys.srem(f"workers:{app_name}", self.worker_id)
        await emit.emit(Events.OMEGA_WORKER_LEAVE, self.worker_id, exchange=app_name)

    async def stop(self):
        """
        common task that both main/child process need to perform before exit
        Returns:

        """
        try:
            role = "worker" if self.is_worker else "main"

            logger.info("stopping zillionare-omega %s process", role)

            if not self.is_worker:
                # notify child to exit
                for proc in self.procs:
                    logger.info("Notify child process %s to exit", proc.pid)
                    os.kill(proc.pid, signal.SIGTERM)

            await self.leave()
            await emit.stop()
            asyncio.get_event_loop().stop()
            logger.info("zillionare-omega %s process stopped.", role)
        except Exception as e:
            logger.exception(e)

    def create_workers(self, n_workers: int):
        self.queue = Queue()
        for i in range(n_workers):
            proc = Process(target=self._sub_entry)
            self.procs.append(proc)
            proc.start()

        for proc in self.procs:
            proc.join()

    def register_exit_handler(self, signals: Iterable, handler: Callable):
        loop = asyncio.get_event_loop()
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(handler()))

    def _sub_entry(self):
        self.is_worker = True
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        logger.info("%s: loop is %s", os.getpid(), loop)
        loop.create_task(self.sub_main())
        loop.run_forever()

    async def start(self):
        cfg4py.init(self.get_config_path(), False)
        self.register_exit_handler((signal.SIGINT, signal.SIGTERM, signal.SIGHUP),
                                   self.stop)
        t = Thread(target=self.create_workers, args=(cfg.omega.concurrency.processes
                                                     - 1,))
        t.start()

        await self.sub_main()
