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
import omicron
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from pyemit import emit
from sanic import Sanic, response

from omega.config.cfg4py_auto_gen import Config
from omega.core import get_config_dir
from omega.core.events import Events
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.jobs import sync as sq

cfg: Config = cfg4py.get_instance()

app = Sanic("Omega")

logger = logging.getLogger(__name__)


class Application(object):
    def __init__(self, fetcher_impl: str, **kwargs):
        self.fetcher_impl = fetcher_impl
        self.params = kwargs

    async def init(self, *args):
        logger.info("init %s", self.__class__.__name__)

        cfg4py.init(get_config_dir(), False)
        await aq.create_instance(self.fetcher_impl, **self.params)
        await omicron.init(aq)

        # listen on omega events
        emit.register(Events.OMEGA_DO_SYNC, sq.sync_bars_worker)
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)

        # register route here
        app.add_route(self.get_security_list_handler, "/quotes/security_list")
        app.add_route(self.get_bars_handler, "/quotes/bars")
        app.add_route(self.get_bars_batch_handler, "/quotes/bars_batch")
        app.add_route(self.get_all_trade_days_handler, "/quotes/all_trade_days")
        app.add_route(self.bars_sync_handler, "/jobs/sync_bars", methods=["POST"])
        app.add_route(
            self.sync_calendar_handler, "/jobs/sync_calendar", methods=["POST"]
        )
        app.add_route(
            self.sync_seurity_list_handler, "jobs/sync_security_list", methods=["POST"]
        )
        app.add_route(self.get_valuation, "quotes/valuation")

        logger.info("<<< init %s process done", self.__class__.__name__)

    async def get_valuation(self, request):
        try:
            secs = request.json.get("secs")
            date = arrow.get(request.json.get("date")).date()
            fields = request.json.get("fields")
            n = request.json.get("n", 1)
        except Exception as e:
            logger.exception(e)
            logger.error("problem params:%s", request.json)
            return response.empty(status=400)
        try:
            valuation = await aq.get_valuation(secs, date, fields, n)
            body = pickle.dumps(valuation, protocol=cfg.pickle.ver)
            return response.raw(body)
        except Exception as e:
            logger.exception(e)
            return response.raw(pickle.dumps(None, protocol=cfg.pickle.ver))

    async def sync_calendar_handler(self, request):
        try:
            await sq.sync_calendar()
            return response.json(body=None, status=200)
        except Exception as e:
            logger.exception(e)
            return response.json(e, status=500)

    async def sync_seurity_list_handler(self, request):
        try:
            await sq.sync_security_list()
            return response.json(body=None, status=200)
        except Exception as e:
            logger.exception(e)
            return response.json(body=e, status=500)

    async def get_security_list_handler(self, request):
        secs = await aq.get_security_list()

        body = pickle.dumps(secs, protocol=cfg.pickle.ver)
        return response.raw(body)

    async def get_bars_batch_handler(self, request):
        try:
            secs = request.json.get("secs")
            frame_type = FrameType(request.json.get("frame_type"))

            end = arrow.get(request.json.get("end"), tzinfo=cfg.tz)
            end = end.date() if frame_type in tf.day_level_frames else end.datetime

            n_bars = request.json.get("n_bars")
            include_unclosed = request.json.get("include_unclosed", False)

            bars = await aq.get_bars_batch(
                secs, end, n_bars, frame_type, include_unclosed
            )

            body = pickle.dumps(bars, protocol=cfg.pickle.ver)
            return response.raw(body)
        except Exception as e:
            logger.exception(e)
            return response.raw(pickle.dumps(None, protocol=cfg.pickle.ver))

    async def get_bars_handler(self, request):
        try:
            sec = request.json.get("sec")
            frame_type = FrameType(request.json.get("frame_type"))

            end = arrow.get(request.json.get("end"), tzinfo=cfg.tz)
            end = end.date() if frame_type in tf.day_level_frames else end.datetime
            n_bars = request.json.get("n_bars")
            include_unclosed = request.json.get("include_unclosed", False)

            bars = await aq.get_bars(sec, end, n_bars, frame_type, include_unclosed)

            body = pickle.dumps(bars, protocol=cfg.pickle.ver)
            return response.raw(body)
        except Exception as e:
            logger.exception(e)
            return response.raw(pickle.dumps(None, protocol=cfg.pickle.ver))

    async def get_all_trade_days_handler(self, request):
        days = await aq.get_all_trade_days()

        body = pickle.dumps(days, protocol=cfg.pickle.ver)
        return response.raw(body)

    async def bars_sync_handler(self, request):
        if request.json:
            secs = request.json.get("secs")
            frames_to_sync = request.json.get("frames_to_sync")
        else:
            secs = None
            frames_to_sync = None

        app.add_task(sq.trigger_bars_sync(secs, frames_to_sync))
        return response.text(f"sync_bars with {secs}, {frames_to_sync} is scheduled.")


def get_fetcher_info(fetchers: List, impl: str):
    for fetcher_info in fetchers:
        if fetcher_info.get("impl") == impl:
            return fetcher_info
    return None


def start(impl: str, group_id: int = 0, **kwargs):
    """launch omega process.

    if kwargs present, then group_id is ignored. Otherwise, `start` will read group
    settings indexed by that group_id from config file.

    Args:
        impl (str): [description]
        group_id (int, optional): [description]. Defaults to 0.
    """
    if kwargs:
        workers = kwargs.get("sessions", 1)
        port = kwargs.get("port", 3181)
        if "port" in kwargs:
            del kwargs["port"]
        fetcher = Application(fetcher_impl=impl, **kwargs)
    else:
        config_dir = get_config_dir()
        cfg = cfg4py.init(config_dir, False)

        info = get_fetcher_info(cfg.quotes_fetchers, impl)
        groups = info.get("groups")
        port = info.get("port") + group_id

        assert groups is not None
        assert len(groups) > group_id

        workers = groups[group_id].get("sessions", 1)

        fetcher = Application(fetcher_impl=impl, **groups[group_id])

    app.register_listener(fetcher.init, "before_server_start")

    logger.info("starting omega group %s with %s workers", group_id, workers)
    app.run(host="0.0.0.0", port=port, workers=workers, register_sys_signals=True)


if __name__ == "__main__":
    fire.Fire({"start": start})
