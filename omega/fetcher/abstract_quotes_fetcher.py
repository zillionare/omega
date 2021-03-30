#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This is a awesome
        python script!"""
import datetime
import importlib
import logging
from typing import List, Union

import arrow
import cfg4py
import numpy as np
from numpy.lib import recfunctions as rfn
from omicron import cache
from omicron.core.lang import static_vars
from omicron.core.timeframe import tf
from omicron.core.types import Frame, FrameType
from omicron.models.valuation import Valuation

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
        factory_method = getattr(module, "create_instance")
        if not callable(factory_method):
            raise TypeError(f"Bad omega adaptor implementation {module_name}")

        impl: QuotesFetcher = await factory_method(**kwargs)
        cls._instances.append(impl)
        logger.info("add one quotes fetcher implementor: %s", module_name)

    @classmethod
    @static_vars(i=0)
    def get_instance(cls):
        if len(cls._instances) == 0:
            raise IndexError("No fetchers available")

        i = (cls.get_instance.i + 1) % len(cls._instances)

        return cls._instances[i]

    @classmethod
    async def get_security_list(cls) -> Union[None, np.ndarray]:
        """按如下格式返回证券列表。

        code         display_name   name   start_date   end_date   type
        000001.XSHE   平安银行       PAYH   1991-04-03   2200-01-01 stock

        Returns:
            Union[None, np.ndarray]: [description]
        """
        securities = await cls.get_instance().get_security_list()
        if securities is None or len(securities) == 0:
            logger.warning("failed to update securities. %s is returned.", securities)
            return securities

        key = "securities"
        pipeline = cache.security.pipeline()
        pipeline.delete(key)
        for code, display_name, name, start, end, _type in securities:
            pipeline.rpush(
                key, f"{code},{display_name},{name},{start}," f"{end},{_type}"
            )
        await pipeline.execute()
        return securities

    @classmethod
    async def get_bars_batch(
        cls,
        secs: List[str],
        end: Frame,
        n_bars: int,
        frame_type: FrameType,
        include_unclosed=True,
    ) -> np.ndarray:
        return await cls.get_instance().get_bars_batch(
            secs, end, n_bars, frame_type.value, include_unclosed
        )

    @classmethod
    async def get_bars(
        cls,
        sec: str,
        end: Frame,
        n_bars: int,
        frame_type: FrameType,
        include_unclosed=True,
    ) -> np.ndarray:
        """获取行情数据，并将已结束的周期数据存入缓存。

        各种情况：
        1. 假设现在时间是2021-2-24日，盘中。此时请求上证指数日线，且`include_unclosed`为
        `True`：
        ```python
        get_bars("000001.XSHE", None, 1, FrameType.DAY)
        ```
        得到的数据可能如下：
        ```
        [(datetime.date(2021, 2, 24), 3638.9358, 3645.5288, 3617.44, 3620.3542, ...)]
        ```
        在收盘前不同时间调用，得到的数据除开盘价外，其它都实时在变动。

        2. 假设现在时间是2021-2-23日，盘后，此时请求上证指数日线，将得到收盘后固定的价格。

        3. 上述请求中，`include_unclosed`参数使用默认值(`True`)。如果取为`False`，仍以示例1
        指定的场景为例，则:
        ```python
        get_bars("000001.XSHG", None, 1, FrameType.DAY, False)
        ```
        因为2021-2-24日未收盘，所以获取的最后一条数据是2021-2-23日的。

        4. 同样假设现在时间是2021-2-24日盘中，周三。此时获取周K线。在`include_unclosed`分别为
        `True`和`False`的情况下：
        ```
        [(datetime.date(2021, 2, 24), 3707.19, 3717.27, 3591.3647, 3592.3977, ...)]
        [(datetime.date(2021, 2, 19), 3721.09, 3731.69, 3634.01, 3696.17, ...)]
        ```
        注意这里当`include_unclosed`为True时，返回的周K线是以2021-2-24为Frame的。同样，在盘中
        的不同时间取这个数据，除了`open`数值之外，其它都是实时变化的。

        5. 如果在已结束的周期中，包含停牌数据，则会对停牌期间的数据进行nan填充，以方便数据使用
        者可以较容易地分辨出数据不连贯的原因：哪些是停牌造成的，哪些是非交易日造成的。这种处理
        会略微降低数据获取速度，并增加存储空间。

        比如下面的请求:
        ```python
        get_bars("000029.XSHE", datetime.date(2020,8,18), 10, FrameType.DAY)
        ```
        将获取到2020-8-5到2020-8-18间共10条数据。但由于期间000029这支股票处于停牌期，所以返回
        的10条数据中，数值部分全部填充为np.nan。

        注意如果取周线和月线数据，如果当天停牌，但只要周线有数据，则仍能取到。周线（或者月线）的
        `frame`将是停牌前一交易日。比如，
        ```python
        sec = "600721.XSHG"
        frame_type = FrameType.WEEK

        end = arrow.get("2020-4-29 15:00").datetime
        bars = await aq.get_bars(sec, end, 3, FrameType.WEEK)
        print(bars)
        ```
        2020年4月30日是该周的最后一个交易日。股票600721在4月29日停牌一天。上述请求将得到如下数
        据：
        ```
        [(datetime.date(2020, 4, 17), 6.02, 6.69, 5.84, 6.58, ...)
         (datetime.date(2020, 4, 24), 6.51, 6.57, 5.68, 5.72, ...)
         (datetime.date(2020, 4, 28), 5.7, 5.71, 5.17, 5.36, ...)]
        ```
        停牌发生在日线级别上，但我们的请求发生在周线级别上，所以不会对4/29日进行填充，而是返回
        截止到4月29日的数据。

        args:
            sec: 证券代码
            end: 数据截止日
            n_bars: 待获取的数据条数
            frame_type: 数据所属的周期
            include_unclosed: 如果为真，则会包含当end所处的那个Frame的数据，即使当前它还未结束
        """
        now = arrow.now(tz=cfg.tz)
        end = end or now.datetime

        # 如果end超出当前时间，则认为是不合法的。如果用户想取到最新的数据，应该传入None
        if type(end) == datetime.date:
            if end > now.date():
                return None
        elif type(end) == datetime.datetime:
            if end > now:
                return None

        bars = await cls.get_instance().get_bars(
            sec, end, n_bars, frame_type.value, include_unclosed
        )

        if len(bars) == 0:
            return

        # 根据指定的end，计算结束时的frame
        last_closed_frame = tf.floor(end, frame_type)

        last_frame = bars[-1]["frame"]

        # 计算有多少根k线是已结束的
        n_closed = n_bars - 1

        if frame_type == FrameType.DAY:
            # 盘后取日线，返回的一定是全部都已closed的数据
            # 盘中取日线，返回的last_frame会是当天的日期，但该日线并未结束
            if now.datetime.hour >= 15 or last_frame < now.date():
                n_closed = n_bars
        else:
            # 如果last_frame <= end的上限，则返回的也一定是全部都closed的数据
            if last_frame <= tf.floor(end, frame_type):
                n_closed = n_bars

        remainder = [bars[-1]] if n_closed < n_bars else None

        closed_bars = cls._fill_na(bars, n_closed, last_closed_frame, frame_type)

        # 只保存已结束的bar
        await cache.save_bars(sec, closed_bars, frame_type)
        if remainder is None:
            return closed_bars
        else:
            return np.concatenate([closed_bars, remainder])

    @classmethod
    def _fill_na(cls, bars: np.array, n: int, end: Frame, frame_type) -> np.ndarray:
        if frame_type in tf.minute_level_frames:
            convert = tf.int2time
        else:
            convert = tf.int2date

        frames = [convert(x) for x in tf.get_frames_by_count(end, n, frame_type)]
        filled = np.empty(n, dtype=bars.dtype)
        filled[:] = np.nan
        filled["frame"] = frames

        return merge(filled, bars, "frame")

    @classmethod
    async def get_all_trade_days(cls):
        days = await cls.get_instance().get_all_trade_days()
        await cache.save_calendar("day_frames", map(tf.date2int, days))
        return days

    @classmethod
    async def get_valuation(
        cls,
        code: Union[str, List[str]],
        day: datetime.date,
        fields: List[str] = None,
        n: int = 1,
    ) -> np.ndarray:

        valuation = await cls.get_instance().get_valuation(code, day, n)

        await Valuation.save(valuation)

        if fields is None:
            return valuation

        if isinstance(fields, str):
            fields = [fields]

        mapping = dict(valuation.dtype.descr)
        fields = [(name, mapping[name]) for name in fields]
        return rfn.require_fields(valuation, fields)
