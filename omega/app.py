#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging
import pickle

import arrow
import cfg4py
import fire
from omicron.core.types import FrameType
from omicron.dal import cache
from pyemit import emit
from sanic import Sanic, response

from omega.config.cfg4py_auto_gen import Config
from omega.core import get_config_dir
from omega.core.events import Events
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.jobs import syncquotes as sq

cfg: Config = cfg4py.get_instance()

app = Sanic('Omega')

logger = logging.getLogger(__name__)


class Application(object):
    def __init__(self, fetcher_impl: str, config_dir: str, **kwargs):
        self.fetcher_impl = fetcher_impl
        self.config_dir = config_dir
        self.params = kwargs

    async def init(self, *args):
        logger.info("init %s", self.__class__.__name__)

        cfg4py.init(self.config_dir)
        await aq.create_instance(self.fetcher_impl, **self.params)
        await cache.init()

        # listen on omega events
        emit.register(Events.OMEGA_DO_SYNC, sq.do_sync)
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)

        # register route here
        app.add_route(self.get_security_list, '/quotes/security_list')
        app.add_route(self.get_bars, '/quotes/bars')
        app.add_route(self.get_all_trade_days, '/quotes/all_trade_days')

        logger.info("<<< init %s process done", self.__class__.__name__)

    async def get_security_list(self, request):
        secs = await aq.get_security_list()

        body = pickle.dumps(secs, protocol=cfg.pickle.ver)
        return response.raw(body)

    async def get_bars(self, request):
        sec = request.json.get('sec')
        end = arrow.get(request.json.get("end"), tzinfo=cfg.tz)
        n_bars = request.json.get("n_bars")
        frame_type = FrameType(request.json.get("frame_type"))

        bars = await aq.get_bars(sec, end, n_bars, frame_type)

        body = pickle.dumps(bars, protocol=cfg.pickle.ver)
        return response.raw(body)

    async def get_all_trade_days(self, request):
        days = await aq.get_all_trade_days()

        body = pickle.dumps(days, protocol=cfg.pickle.ver)
        return response.raw(body)

    async def sync_bars(self, request):
        secs = request.json.get('secs')
        frames_to_sync = request.json.get('frames_to_sync')

        await sq.start_sync(secs, frames_to_sync)


def start(port: int, impl: str, workers: int = 1, config_dir: str = None, **kwargs):
    config_dir = config_dir or get_config_dir()
    fetcher = Application(config_dir=config_dir, fetcher_impl=impl, **kwargs)
    app.register_listener(fetcher.init, 'before_server_start')
    app.run(host='0.0.0.0', port=port, workers=workers, register_sys_signals=True)


if __name__ == "__main__":
    fire.Fire({
        'start': start
    })
