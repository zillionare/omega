#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This is a awesome
        python script!"""
import importlib
import logging
from typing import List

import numpy as np
from arrow import Arrow
from omicron.core import FrameType, tf
from omicron.core.lang import static_vars
from omicron.dal import cache, RedisDB

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
                    display_name 	name 	start_date 	end 	type
        000001.XSHE 	平安银行 	PAYH 	1991-04-03 	2200-01-01 	stock
        000002.XSHE 	万科A 	    WKA 	1991-01-29 	2200-01-01 	stock
        :return:
        """
        securities = await cls.get_instance().get_security_list()
        db = cache.get_db(RedisDB.SECURITY.name)
        key = 'securities'
        await db.delete(key)
        pipeline = db.pipeline()
        for code, display_name, name, start, end, _type in securities:
            pipeline.rpush(key, f"{code},{display_name},{name},{start.date()},{end.date()},{_type}")
        await pipeline.execute()
        return securities

    @classmethod
    async def get_bars(cls, sec: str, end: Arrow, n_bars: int, frame_type: FrameType) -> np.ndarray:
        bars = await cls.get_instance().get_bars(sec, end, n_bars, frame_type)
        await cls.save_bars(sec, bars, frame_type)
        return bars

    @classmethod
    async def save_bars(cls, sec: str, bars: np.ndarray, frame_type: FrameType, sync_mode: int = 1):
        """
        为每条k线记录生成一个ID，将时间：id存入该sec对应的ordered set
        code -> {
                20191204: [date, o, l, h, c, v]::json
                20191205: [date, o, l, h, c, v]::json
                head: date or datetime
                tail: date or datetime
                }
        :param sec: the full qualified code of a security or index
        :param bars: the data to save
        :param frame_type: use this to decide which store to use
        :param sync_mode: 1 for update, 2 for rewrite
        :return:
        """
        if bars is None or len(bars) == 0:
            return
        pipeline = cache.get_db(frame_type.name).pipeline()

        pipeline.hget(sec, "head")
        pipeline.hget(sec, "tail")
        head, tail = await pipeline.execute()

        if not (head and tail) or sync_mode == 2:
            await cls._save_bars(sec, bars, frame_type)

        convert_to_time = tf.int2time if frame_type in [FrameType.MIN1, FrameType.MIN5, FrameType.MIN15,
                                                        FrameType.MIN30,
                                                        FrameType.MIN60] else tf.int2date
        dt_head, dt_tail = convert_to_time(head), convert_to_time(tail)

        if tf.shift(bars['frame'][-1], 1, frame_type) < dt_head or tf.shift(bars['frame'][0], -1, frame_type) > dt_tail:
            # don't save to database, otherwise the data is not continuous
            return

        # both head and tail exist, only save bars out of database's range
        bars_to_save = bars[(bars['frame'] < dt_head) | (bars['frame'] > dt_tail)]
        if len(bars_to_save) == 0:
            return

        convert_to_int = tf.time2int if frame_type in [FrameType.MIN1, FrameType.MIN5, FrameType.MIN15, FrameType.MIN30,
                                                       FrameType.MIN60] else tf.date2int
        await cls._save_bars(sec, bars_to_save, frame_type,
                             min(int(head), convert_to_int(bars['frame'][0])),
                             max(int(tail), convert_to_int(bars['frame'][-1])))

    @classmethod
    async def _save_bars(cls, code: str, bars: np.ndarray, frame_type: FrameType, head: int = None,
                         tail: int = None):
        if frame_type not in [FrameType.MIN1, FrameType.MIN5, FrameType.MIN15, FrameType.MIN30, FrameType.MIN60]:
            head = head or tf.date2int(bars['frame'][0])
            tail = tail or tf.date2int(bars['frame'][-1])
            key_convert_func = tf.date2int
        else:
            head = head or tf.time2int(bars['frame'][0])
            tail = tail or tf.time2int(bars['frame'][-1])
            key_convert_func = tf.time2int

        pipeline = cache.get_db(frame_type.name).pipeline()
        # the cache is empty or error during syncing, save all bars
        for row in bars:
            frame, o, h, l, c, v, a, fq = row
            key = key_convert_func(frame)
            value = f"{frame} {o:.2f} {h:.2f} {l:.2f} {c:.2f} {v} {a:.2f} {fq:.2f}"
            pipeline.hset(code, key, value)

        pipeline.hset(code, 'head', head)
        pipeline.hset(code, 'tail', tail)
        await pipeline.execute()

    @staticmethod
    def construct_frame_keys(end: Arrow, n: int, frame_type: FrameType) -> List[int]:
        if frame_type == FrameType.DAY:
            days = tf.trade_days[tf.trade_days <= tf.date2int(end)]
            return days[-n:]

        if frame_type not in {FrameType.MIN1, FrameType.MIN5, FrameType.MIN15, FrameType.MIN30, FrameType.MIN60}:
            raise ValueError(f"{frame_type} not support yet")

        n_days = n // len(tf.ticks[frame_type]) + 2
        ticks = tf.ticks[frame_type] * n_days

        days = tf.trade_days[tf.trade_days <= tf.date2int(end)][-n_days:]
        days = np.repeat(days, len(tf.ticks[frame_type]))

        ticks = [day * 10000 + int(tm) for day, tm in zip(days, ticks)]

        pos = ticks.index(tf.time2int(end)) + 1
        return ticks[pos - n: pos]
