#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This is a awesome
        python script!"""
import datetime
import importlib
import logging
import random
from typing import List, Optional, Union

import arrow
import cfg4py
import numpy as np
from numpy.lib import recfunctions as rfn
from omicron.core.types import Frame, FrameType
from omicron.models.calendar import Calendar as cal
from omicron.models.stock import Stock
from scipy import rand

from omega.worker.quotes_fetcher import QuotesFetcher

logger = logging.getLogger(__file__)

cfg = cfg4py.get_instance()


class AbstractQuotesFetcher(QuotesFetcher):
    _instances = []
    quota = 4000

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
        logger.info("add one quotes worker implementor: %s", module_name)

    @classmethod
    def get_instance(cls):
        if len(cls._instances) == 0:
            raise IndexError("No fetchers available")

        i = random.randint(0, len(cls._instances) - 1)

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

        await Stock.save_securities(securities)

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
        unclosed=True,
    ) -> np.ndarray:
        # todo: 接口也可能要改，以区分盘中实时同步分钟线和校准同步分钟线、日线情况
        raise NotImplementedError

    @classmethod
    async def get_all_trade_days(cls):
        days = await cls.get_instance().get_all_trade_days()
        await cal.save_calendar(days)
        return days

    @classmethod
    async def get_high_limit_price(
        cls, sec: Union[List, str], dt: Union[str, datetime.datetime, datetime.date]
    ) -> np.ndarray:
        params = {
            "sec": sec,
            "dt": dt,
        }
        bars = await cls.get_instance().get_high_limit_price(**params)

        if len(bars) == 0:
            return None
        return bars

    @classmethod
    async def get_quota(cls):
        quota = await cls.get_instance().get_query_count()
        return quota.get("spare")

    @classmethod
    async def get_quota(cls):
        quota = await cls.get_instance().get_query_count()
        return quota.get("spare")
