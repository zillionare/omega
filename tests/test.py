#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors: 

"""
import asyncio
import logging
import os

import cfg4py
import jqdatasdk as jq
import omicron
from omicron.models.securities import Securities
from omicron.models.security import Security
from pyemit import emit

logger = logging.getLogger(__name__)


def get_config_path():
    src_dir = os.path.dirname(__file__)
    return os.path.join(src_dir, '../omega/config')


async def setUp():
    os.environ[cfg4py.envar] = 'TEST'

    logger.info("starting solo quotes server...")
    cfg4py.init(get_config_path(), False)
    cfg = cfg4py.get_instance()
    if len(cfg.quotes_fetchers) == 0:
        raise ValueError("please config quotes fetcher before test.")

    # 启动 emits 事件监听
    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, exchange='omega',
                     start_server=True)
    await omicron.init()


def test_30_bars():
    asyncio.run(setUp())
    secs = Securities()
    jq.auth('18694978299', '8Bu8tcDpEAHJRn')
    for code in secs.choose(['stock']):

        bars = jq.get_bars(code, 4, '30m', include_now=True, df=True)
        if (bars['close'] > bars['open']).all():
            sec = Security(code)
            print("三连阳:", code, sec.display_name)


if __name__ == "__main__":
    test_30_bars()
