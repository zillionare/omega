#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:
"""
import logging
import os
import time
from time import timezone
from typing import List

import cfg4py
import fire
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyemit import emit
from sanic import Blueprint, Sanic
from sanic.websocket import WebSocketProtocol

from omega.config import get_config_dir
from omega.core.events import Events
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.interfaces import jobs, quotes, sys
from omega.jobs import syncjobs

cfg = cfg4py.get_instance()

app = Sanic("Omega")

logger = logging.getLogger(__name__)


class Omega(object):
    def __init__(self, fetcher_impl: str, cfg: dict = None, **kwargs):
        self.port = kwargs.get("port")
        self.gid = kwargs.get("account")

        self.fetcher_impl = fetcher_impl
        self.params = kwargs
        self.inherit_cfg = cfg or {}
        self.scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")

    async def init(self, *args):
        logger.info("init %s", self.__class__.__name__)

        cfg4py.init(get_config_dir(), False)
        cfg4py.update_config(self.inherit_cfg)

        await aq.create_instance(self.fetcher_impl, **self.params)

        await omicron.init(aq)

        interfaces = Blueprint.group(jobs.bp, quotes.bp, sys.bp)
        app.blueprint(interfaces)

        # listen on omega events
        emit.register(Events.OMEGA_DO_SYNC, syncjobs.sync_bars)
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)
        await self.heart_beat()
        self.scheduler.add_job(self.heart_beat, trigger="interval", seconds=3)
        self.scheduler.start()

        logger.info("<<< init %s process done", self.__class__.__name__)

    async def heart_beat(self):
        pid = os.getpid()
        key = f"process.fetchers.{pid}"
        logger.debug("send heartbeat from omega fetcher: %s", pid)
        await omicron.cache.sys.hmset(
            key,
            "impl", self.fetcher_impl,
            "gid", self.gid,
            "port", self.port,
            "pid", pid,
            "heartbeat", time.time(),
        )


def get_fetcher_info(fetchers: List, impl: str):
    for fetcher_info in fetchers:
        if fetcher_info.get("impl") == impl:
            return fetcher_info
    return None


def start(impl: str, cfg: dict = None, **fetcher_params):
    """启动一个Omega fetcher进程

    使用本函数来启动一个Omega fetcher进程。该进程可能与其它进程一样，使用相同的impl和账号，因此构成一组进程。

    通过多次调用本方法，传入不同的quotes fetcher impl参数，即可启动多组Omega服务。

    如果指定了`fetcher_params`，则`start`将使用impl, fetcher_params来启动单个Omega服务，使
    用impl指定的fetcher。否则，将使用`cfg.quotes_fetcher`中提供的信息来创建Omega.

    如果`cfg`不为None，则应该指定为合法的json string，其内容将覆盖本地cfg。这个设置目前的主要
    要作用是方便单元测试。


    Args:
        impl (str): quotes fetcher implementor
        cfg: the cfg in json string
        fetcher_params: contains info required by creating quotes fetcher
    """
    port = fetcher_params.get("port", 3181)
    omega = Omega(impl, cfg, **fetcher_params)

    app.register_listener(omega.init, "before_server_start")

    logger.info("starting sanic group listen on %s with %s workers", port, 1)
    app.run(
        host="0.0.0.0",
        port=port,
        workers=1,
        register_sys_signals=True,
        protocol=WebSocketProtocol,
    )
    logger.info("sanic stopped.")


if __name__ == "__main__":
    fire.Fire({"start": start})
