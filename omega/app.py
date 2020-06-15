#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging
import pickle
from typing import List

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
    def __init__(self, fetcher_impl: str, **kwargs):
        self.fetcher_impl = fetcher_impl
        self.params = kwargs

    async def init(self, *args):
        logger.info("init %s", self.__class__.__name__)

        cfg4py.init(get_config_dir(), False)
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


def get_fetcher_groups(fetchers: List, impl: str):
    for fetcher_info in fetchers:
        if fetcher_info.get('impl') == impl:
            return fetcher_info.get('groups')
    return None


def start(impl: str, port: int = 3181, group_id: int = 0):
    config_dir = get_config_dir()
    cfg = cfg4py.init(config_dir)

    groups = get_fetcher_groups(cfg.quotes_fetchers, impl)
    assert groups != None
    assert len(groups) > group_id

    fetcher = Application(fetcher_impl=impl, **groups[group_id])
    app.register_listener(fetcher.init, 'before_server_start')

    port = port + group_id
    sessions = groups[group_id].get('sessions', 1)
    app.run(host='0.0.0.0', port=port, workers=sessions, register_sys_signals=True)


if __name__ == "__main__":
    fire.Fire({
        'start': start
    })
