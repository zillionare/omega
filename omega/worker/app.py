#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:
"""
import asyncio
import datetime
import logging
import os
import time
from typing import List

import cfg4py
import fire
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyemit import emit

from omega.config import get_config_dir
from omega.core.events import Events
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.worker.jobs import load_cron_task
from omega.worker.tasks import sec_synctask, synctask
from omega.worker.tasks.task_utils import cache_init

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


class Omega(object):
    def __init__(self, fetcher_impl: str, cfg: dict = None, **kwargs):
        self.gid = kwargs.get("account")

        self.fetcher_impl = fetcher_impl
        self.params = kwargs
        self.inherit_cfg = cfg or {}
        self.scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")

    async def first_load_data(self, *args):  # 从零开始初始化数据
        logger.info("first_load_data %s", self.__class__.__name__)

        cfg4py.init(get_config_dir(), False)
        cfg4py.update_config(self.inherit_cfg)

        await AbstractQuotesFetcher.create_instance(self.fetcher_impl, **self.params)
        await omicron.cache.init()
        await cache_init()
        logger.info("<<< init %s process done", self.__class__.__name__)

    async def init(self, *args):
        logger.info("init %s", self.__class__.__name__)

        cfg4py.init(get_config_dir(), False)
        cfg4py.update_config(self.inherit_cfg)

        await AbstractQuotesFetcher.create_instance(self.fetcher_impl, **self.params)
        # listen on omega events
        await omicron.cache.init()
        try:
            await omicron.init()
        except Exception as e:
            print(
                'No calendar and securities in cache, make sure you have called "omega init" first:\n',
                e,
            )
            logger.error(
                'No calendar and securities in cache, make sure you have called "omega init" first:\n'
            )
            time.sleep(5)
            os._exit(1)

        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)
        self.scheduler.add_job(self.heart_beat, trigger="interval", seconds=5)
        await load_cron_task(self.scheduler)
        self.scheduler.start()

        logger.info("<<< init %s process done", self.__class__.__name__)

    async def heart_beat(self):
        quota = await AbstractQuotesFetcher.get_quota()

        await emit.emit(
            Events.OMEGA_HEART_BEAT,
            {
                "account": self.gid,
                "quota": quota.get("spare"),  # 剩余额度
                "total": quota.get("total"),  # 总额度
                "impl": self.fetcher_impl,
                "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        )


def get_fetcher_info(fetchers: List, impl: str):
    for fetcher_info in fetchers:
        if fetcher_info.get("impl") == impl:
            return fetcher_info
    return None


def start(impl: str, cfg: dict = None, **fetcher_params):
    """启动一个Omega fetcher进程

    使用本函数来启动一个Omega fetcher进程。该进程可能与其它进程一样，使用相同的impl和账号，因此构成一组进程。

    通过多次调用本方法，传入不同的quotes worker impl参数，即可启动多组Omega服务。

    如果指定了`fetcher_params`，则`start`将使用impl, fetcher_params来启动单个Omega服务，使
    用impl指定的fetcher。否则，将使用`cfg.quotes_fetcher`中提供的信息来创建Omega.

    如果`cfg`不为None，则应该指定为合法的json string，其内容将覆盖本地cfg。这个设置目前的主要
    要作用是方便单元测试。


    Args:
        impl (str): quotes worker implementor
        cfg: the cfg in json string
        fetcher_params: contains info required by creating quotes worker
    """
    emit.register(Events.OMEGA_DO_SYNC_SECURITIES, sec_synctask.sync_security_list)
    emit.register(
        Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, synctask.sync_trade_price_limits
    )
    emit.register(
        Events.OMEGA_DO_SYNC_DAILY_CALIBRATION, synctask.sync_daily_calibration
    )
    emit.register(Events.OMEGA_DO_SYNC_DAY, synctask.after_hour_sync)
    emit.register(Events.OMEGA_DO_SYNC_MIN, synctask.sync_minute_bars)
    emit.register(
        Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
        synctask.sync_year_quarter_month_week,
    )
    emit.register(
        Events.OMEGA_DO_SYNC_OTHER_MIN,
        synctask.sync_min_5_15_30_60,
    )

    omega = Omega(impl, cfg, **fetcher_params)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(omega.init())
    logger.info("omega worker 启动")
    loop.run_forever()


async def init_data(impl: str, cfg: dict = None, **fetcher_params):
    omega = Omega(impl, cfg, **fetcher_params)
    await omega.first_load_data()


if __name__ == "__main__":
    fire.Fire({"start": start})
