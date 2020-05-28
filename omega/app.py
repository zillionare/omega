#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import asyncio
import importlib
import logging
import os
import platform
import signal
import socket
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Iterable, Optional

import arrow
import cfg4py
import omicron
from aiomultiprocess import Process
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron.core.events import Events
from omicron.core.lang import singleton
from omicron.dal import cache
from pyemit import emit

from . import app_name
from .config.cfg4py_auto_gen import Config
from .fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from .jobs import syncquotes as sq

logger = logging.getLogger(__name__)

cfg: Config = cfg4py.get_instance()


@singleton
class Application:
    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.thread_executors = None
        self.procs = []
        self.queue = None
        self.is_worker = False
        self.worker_id = 0

    def add_job(self, func, trigger=None, *args, **kwargs):
        return self.scheduler.add_job(func, trigger, *args, **kwargs)

    def get_config_path(self):
        if os.environ.get('dev_mode'):
            src_dir = os.path.dirname(__file__)
            return os.path.join(src_dir, 'config')
        else:
            path = Path('~/zillionare/omega/config').expanduser()
            if path.exists():
                return path
            else:
                print("Please init omega by running command `omega setup`")
                sys.exit(-1)

    async def update_worker_status(self):
        """for long-lived worker process, it should update self-status periodically
        so that main process can manage it.
        :param"""
        logger.info("update: %s %s", self.worker_id, os.getpid())
        await cache.sys.hmset_dict(f"workers:{app_name}:{self.worker_id}", {
            "last_seen": arrow.now(cfg.tz).timestamp,
            "pid":       os.getpid(),
            "host":      socket.gethostname()
        })

        await cache.sys.sadd(f"workers:{app_name}", self.worker_id)

    async def check_worker_status(self):
        """
        main process to check if subprocess are online
        :param"""
        try:
            logger.info("checking worker status %s", self.worker_id)
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
                    logger.warning("%s is removed due to status is not clear",
                                   worker_id)
                    continue

                since_last_seen = arrow.now(cfg.tz).timestamp - int(status['last_seen'])
                if since_last_seen > cfg.omega.heartbeat * 1.3:
                    # if the worker failed to update its status in an interval or more
                    try:
                        pid = int(status['pid'])
                        os.kill(pid, signal.SIGTERM)
                    except (ValueError, KeyError, ProcessLookupError):
                        pass

                    logger.warning("worker %s is removed, last seen is %s second ago",
                                   worker_id, since_last_seen)
                    pl.srem(f"workers:{app_name}", worker_id)
                    pl.delete(f"workers:{app_name}:{worker_id}")
                    continue
                alive_workers.append(worker_id)

            await pl.execute()
            return len(alive_workers)
        except Exception as e:
            logger.exception(e)

    async def join(self):
        self.worker_id = str(await cache.sys.incr(f"id:workers:{app_name}"))
        logger.info("worker %s joined, process id is %s", self.worker_id, os.getpid())
        try:
            self.scheduler.add_job(self.check_worker_status, 'interval',
                                   seconds=cfg.omega.heartbeat,
                                   next_run_time=arrow.now(cfg.tz).naive)
            await emit.emit(Events.OMEGA_WORKER_JOIN, exchange=app_name)
        except Exception as e:
            logger.exception(e)

    async def on_worker_join(self, msg=None):
        alive = await self.check_worker_status()
        logger.info("%s workers online", alive)

    async def on_worker_leave(self, worker_id):
        """
        if other workers leave, we need to select the new leader
        """
        logger.info("omega worker(%s) is leaving us", worker_id)
        if worker_id != self.worker_id:
            alive = await self.check_worker_status()
            logger.info("%s workers online", alive)

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
            # todo: remove me
            # asyncio.get_event_loop().stop()
            logger.info("zillionare-omega %s process %s stopped.", role, os.getpid())
            sys.exit(-1)
        except Exception as e:
            logger.exception(e)

    def register_exit_handler(self, signals: Iterable, handler: Callable):
        loop = asyncio.get_event_loop()
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(handler()))

    def global_exception_handler(self, loop, context):
        msg = context.get("exception", context["message"])
        logger.error(f"Unhandled exception:%s", msg)
        if context.get('exception'):
            logger.exception(context.get('exception'))

        logger.info("shutting down worker %s(%s)...", self.worker_id, os.getpid())

        loop.create_task(self.stop())

    def load_additional_jobs(self):
        """从配置文件创建自定义的插件任务。
        :param
        """
        for job in cfg.omega.jobs:
            try:
                module = importlib.import_module(job['module'])
                entry = getattr(module, job['entry'])
                self.add_job(entry, trigger=job['trigger'], args=job['params'],
                             **job['trigger_params'])
            except Exception as e:
                logger.exception(e)
                logger.info("failed to create job: %s", job)

    def set_uv_loop(self):
        if platform.system() in "Linux":
            try:
                # noinspection PyPackageRequirements
                import uvloop

                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            except ModuleNotFoundError:
                logger.warning(
                        'uvloop is required to achieve better performance, continuing '
                        'with degraded service.')

    async def init_fetcher_process(self, module, **kwargs):
        """
        quotes fetcher进程入口
        """
        logger.info(">>> init fetcher process %s", os.getpid())
        self.scheduler = AsyncIOScheduler(timezone=cfg.tz)

        self.thread_executors = ThreadPoolExecutor(kwargs.get("sessions"))
        await omicron.init(cfg)

        # 实例化 fetcherImpl
        await aq.create_instance(module, executors=self.thread_executors, **kwargs)
        await self.join()

        # 等待主进程初始化任务后再同步
        emit.register(Events.OMEGA_DO_SYNC, sq.do_sync, app_name)
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
        self.scheduler.add_job(self.update_worker_status, 'interval',
                               seconds=cfg.omega.heartbeat)
        self.scheduler.start()
        logger.info("<<< leave fetcher_entry %s", os.getpid())

    async def init(self):
        """
        管理子进程，任务调度
        :param"""
        logger.info(">>> main process %s initializing", os.getpid())
        self.thread_executors = ThreadPoolExecutor()
        self.scheduler = AsyncIOScheduler(timezone=cfg.tz)

        # subprocess管理
        emit.register(Events.OMEGA_WORKER_JOIN, self.on_worker_join, app_name)
        emit.register(Events.OMEGA_WORKER_LEAVE, self.on_worker_leave, app_name)
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)

        await omicron.init(cfg)

        self.scheduler.add_job(sq.start_job, 'cron', args=('validation',), hour=2)
        h, m = map(int, cfg.omega.sync.time.split(":"))
        self.scheduler.add_job(sq.start_job, 'cron', args=('sync',), hour=h, minute=m)
        self.scheduler.start()

        # load additional jobs
        self.load_additional_jobs()
        # check if we need sync quotes right now
        last_sync = await cache.sys.get("jobs.bars_sync.stop")
        if last_sync: last_sync = arrow.get(last_sync, tzinfo=cfg.tz).timestamp
        if not last_sync or time.time() - last_sync >= 24 * 3600:
            logger.info("start catch-up quotes sync")
            asyncio.create_task(sq.start_job('sync'))

        logger.info("<<< main process init")

    async def start_worker(self):
        # create quotes fetcher dedicated process
        for fetcher_info in cfg.quotes_fetchers:
            module = fetcher_info['module']
            for params in fetcher_info.get('workers'):
                proc = Process(target=self.init_fetcher_process,
                               args=(module,),
                               kwargs=params)
                self.procs.append(proc)
                await proc

    async def start(self):
        cfg4py.init(self.get_config_path(), False)
        self.register_exit_handler((signal.SIGINT, signal.SIGTERM, signal.SIGHUP),
                                   self.stop)

        await self.start_worker()
        await self.init()

        logger.info("Zillionare-omega started")
