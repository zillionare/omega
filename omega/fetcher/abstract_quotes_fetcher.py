#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This is a awesome
        python script!"""
import importlib
import logging

import numpy as np
from arrow import Arrow
from omicron.core.lang import static_vars
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.dal import cache
from omicron.dal import security_cache

from omega.fetcher.quotes_fetcher import QuotesFetcher

logger = logging.getLogger(__file__)


class AbstractQuotesFetcher(QuotesFetcher):
    _instances = []

    @classmethod
    async def create_instance(cls, fetcher: dict, **kwargs):
        # todo: check if implementor has implemented all the required methods
        # todo: check duplicates

        module = importlib.import_module(fetcher['module'])
        factory_method = getattr(module, 'create_instance')
        if not callable(factory_method):
            raise TypeError(f"Bad omega adaptor implementation {fetcher}")

        kwargs.update(fetcher['parameters'])
        impl: QuotesFetcher = await factory_method(**kwargs)
        cls._instances.append(impl)
        logger.info('add one quotes fetcher implementor: %s', fetcher['name'])

    @classmethod
    @static_vars(i=0)
    def get_instance(cls):
        if len(cls._instances) == 0:
            raise IndexError("No fetchers available")

        i = (cls.get_instance.i + 1) % len(cls._instances)

        return cls._instances[i]

    @classmethod
    async def get_security_list(cls) -> np.ndarray:
        """
           code         display_name name 	start_date 	end_date 	type
        000001.XSHE 	平安银行 	PAYH 	1991-04-03 	2200-01-01 	stock
        :return:
        """
        securities = await cls.get_instance().get_security_list()
        key = 'securities'
        await cache.security.delete(key)
        pipeline = cache.security.pipeline()
        for code, display_name, name, start, end, _type in securities:
            pipeline.rpush(key,
                           f"{code},{display_name},{name},{start},"
                           f"{end},{_type}")
        await pipeline.execute()
        return securities

    @classmethod
    async def get_bars(cls, sec: str, end: Arrow, n_bars: int,
                       frame_type: FrameType) -> np.ndarray:
        bars = await cls.get_instance().get_bars(sec, end, n_bars, frame_type)
        await security_cache.save_bars(sec, bars, frame_type)
        return bars

    @classmethod
    async def get_all_trade_days(cls):
        days = await cls.get_instance().get_all_trade_days()
        await security_cache.save_calendar('day_frames', map(tf.date2int, days))
        return days
