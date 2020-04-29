#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    管理应用程序生命期、全局对象、任务、全局消息响应
        """
import asyncio
import logging
import os
import platform
from concurrent.futures.thread import ThreadPoolExecutor
from typing import TYPE_CHECKING

import cfg4py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron.dal import cache
from pyemit import emit

from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

if TYPE_CHECKING:
    from omega.config.cfg4py_auto_gen import Config

logger = logging.getLogger(__name__)


class Application:
    exit_calls = []
    scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
    executors = None

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
    async def start_quotes_fetchers(cls):
        cfg: Config = cfg4py.get_instance()
        for fetcher in cfg.quotes_fetchers:
            await AbstractQuotesFetcher.create_instance(fetcher, executors=cls.executors)

    @classmethod
    async def start(cls):
        logger.info("starting solo quotes server...")
        cfg4py.init(cls.get_config_path(), False)
        cfg: Config = cfg4py.get_instance()
        cls.executors = ThreadPoolExecutor(max_workers=cfg.concurrency.threads)
        await cache.init()

        # 启动 emits 事件监听
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, exchange='zillionare-omega', start_server=True)
        await cls.start_quotes_fetchers()

        # 每日清理一次过期股吧排名数据
        # scheduler.add_job(guba_crawler.purge_aged_records, 'cron', hour=2)
        # 每2小时取一次贴吧数据
        # scheduler.add_job(guba_crawler.start_crawl, 'cron', hour='1-23/2', minute=0, misfire_grace_time=300)
        # scheduler.add_job(ths_realtime_news.start_crawl, 'interval', seconds=7)
        # scheduler.add_job(eastmoney_news.start_crawl, 'interval', seconds = 7)
        # scheduler.add_job(xsg.start_crawl)

        # 启动任务
        cls.scheduler.start()
        logger.info("solo quotes server started.")


def main():
    # patch the loop driver
    if platform.system() in "Linux":
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ModuleNotFoundError:
            logger.warning('uvloop is required for better performance, continuing with degraded service.')

    loop = asyncio.get_event_loop()
    loop.create_task(Application.start())
    loop.run_forever()


if __name__ == "__main__":
    main()
