#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:
"""
import logging
from typing import List

import cfg4py
import fire
import omicron
from pyemit import emit
from sanic import Blueprint, Sanic
from sanic.websocket import WebSocketProtocol

from omega.config import get_config_dir
from omega.config.schema import Config
from omega.core.events import Events
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.interfaces import jobs, quotes, sys
from omega.jobs import syncjobs

cfg: Config = cfg4py.get_instance()

app = Sanic("Omega")

logger = logging.getLogger(__name__)


class Omega(object):
    def __init__(self, fetcher_impl: str, cfg: dict = None, **kwargs):
        self.fetcher_impl = fetcher_impl
        self.params = kwargs
        self.inherit_cfg = cfg or {}

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

        logger.info("<<< init %s process done", self.__class__.__name__)


def get_fetcher_info(fetchers: List, impl: str):
    for fetcher_info in fetchers:
        if fetcher_info.get("impl") == impl:
            return fetcher_info
    return None


def start(impl: str, cfg: dict = None, **fetcher_params):
    """启动一组Omega进程

    使用本函数来启动一组Omega进程。这一组进程使用同样的quotes fetcher,但可能有1到多个session
    (限制由quotes fetcher给出)。它们共享同一个port。Sanic在内部实现了load-balance机制。

    通过多次调用本方法，传入不同的quotes fetcher impl参数，即可启动多组Omega服务。

    如果指定了`fetcher_params`，则`start`将使用impl, fetcher_params来启动单组Omega服务，使
    用impl指定的fetcher。否则，将使用`cfg.quotes_fetcher`中提供的信息来创建Omega.

    如果`cfg`不为None，则应该指定为合法的json string，其内容将覆盖本地cfg。这个设置目前的主要
    要作用是方便单元测试。


    Args:
        impl (str): quotes fetcher implementor
        cfg: the cfg in json string
        fetcher_params: contains info required by creating quotes fetcher
    """
    sessions = fetcher_params.get("sessions", 1)
    port = fetcher_params.get("port", 3181)
    omega = Omega(impl, cfg, **fetcher_params)

    app.register_listener(omega.init, "before_server_start")

    logger.info("starting sanic group listen on %s with %s workers", port, sessions)
    app.run(
        host="0.0.0.0",
        port=port,
        workers=sessions,
        register_sys_signals=True,
        protocol=WebSocketProtocol,
    )


if __name__ == "__main__":
    fire.Fire({"start": start})
