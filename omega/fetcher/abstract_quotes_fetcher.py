#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This is a awesome
        python script!"""
import datetime
import importlib
import logging
from typing import Union

import cfg4py
import numpy as np
from omicron.core.lang import static_vars
from omicron.core.timeframe import tf
from omicron.core.types import FrameType, Frame
from omicron.dal import cache
from omicron.dal import security_cache

from omega.core.accelerate import merge
from omega.fetcher.quotes_fetcher import QuotesFetcher

logger = logging.getLogger(__file__)

cfg = cfg4py.get_instance()


class AbstractQuotesFetcher(QuotesFetcher):
    _instances = []

    @classmethod
    async def create_instance(cls, module_name, **kwargs):
        # todo: check if implementor has implemented all the required methods
        # todo: check duplicates

        module = importlib.import_module(module_name)
        factory_method = getattr(module, 'create_instance')
        if not callable(factory_method):
            raise TypeError(f"Bad omega adaptor implementation {module_name}")

        impl: QuotesFetcher = await factory_method(**kwargs)
        cls._instances.append(impl)
        logger.info('add one quotes fetcher implementor: %s', module_name)

    @classmethod
    @static_vars(i=0)
    def get_instance(cls):
        if len(cls._instances) == 0:
            raise IndexError("No fetchers available")

        i = (cls.get_instance.i + 1) % len(cls._instances)

        return cls._instances[i]

    @classmethod
    async def get_security_list(cls) -> Union[None, np.ndarray]:
        """
           code         display_name name 	start_date 	end_date 	type
        000001.XSHE 	平安银行 	PAYH 	1991-04-03 	2200-01-01 	stock
        :return:
        """
        securities = await cls.get_instance().get_security_list()
        if securities is None or len(securities) == 0:
            logger.warning("failed to update securities. %s is returned.", securities)
            return securities

        key = 'securities'
        pipeline = cache.security.pipeline()
        pipeline.delete(key)
        for code, display_name, name, start, end, _type in securities:
            pipeline.rpush(key,
                           f"{code},{display_name},{name},{start},"
                           f"{end},{_type}")
        await pipeline.execute()
        return securities

    @classmethod
    async def get_bars(cls, sec: str,
                       end: Union[datetime.date, datetime.date],
                       n_bars: int,
                       frame_type: FrameType,
                       include_unclosed=True) -> np.ndarray:
        bars = await cls.get_instance().get_bars(sec, end, n_bars, frame_type, include_unclosed)

        closed = tf.floor(end, frame_type)
        if closed != end:
            filled = cls._fill_na(bars, n_bars - 1, closed, frame_type)
            if bars[-1]['frame'] == end:
                remainder = [bars[-1]]
            else:
                remainder = np.empty(1, dtype=bars.dtype)
                remainder[:] = np.nan
                remainder['frame'] = end
        else:
            filled = cls._fill_na(bars, n_bars, closed, frame_type)
            remainder = None
        # 只保存已结束的frame数据到数据库
        await security_cache.save_bars(sec, filled, frame_type)
        if remainder is None:
            return filled

        return np.concatenate([filled, remainder])

    @classmethod
    def _fill_na(cls, bars: np.array, n: int, end: Frame, frame_type) -> np.ndarray:
        if frame_type in tf.minute_level_frames:
            convert = tf.int2time
        else:
            convert = tf.int2date

        frames = [convert(x) for x in tf.get_frames_by_count(end, n, frame_type)]
        filled = np.empty(n, dtype=bars.dtype)
        filled[:] = np.nan
        filled['frame'] = frames

        return merge(filled, bars, 'frame')

    @classmethod
    async def get_all_trade_days(cls):
        days = await cls.get_instance().get_all_trade_days()
        await security_cache.save_calendar('day_frames', map(tf.date2int, days))
        return days
