#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This is a awesome
        python script!"""
import datetime
import importlib
import logging
from typing import List, Union, Optional

import arrow
import cfg4py
import numpy as np
from numpy.lib import recfunctions as rfn
from omicron.core.types import Frame, FrameType
from omicron.models.calendar import Calendar as cal
from omicron.models.stock import Stock
import random

from scipy import rand

from omega.worker.quotes_fetcher import QuotesFetcher

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
        logger.info("add one quotes worker implementor: %s", module_name)

    @classmethod
    def get_instance(cls):
        if len(cls._instances) == 0:
            raise IndexError("No fetchers available")

        i = random.randint(0, len(cls._instances - 1))

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
        unclosed =True,
    ) -> np.ndarray:
        # todo: 接口也可能要改，以区分盘中实时同步分钟线和校准同步分钟线、日线情况
        raise NotImplementedError



    @classmethod
    async def get_all_trade_days(cls):
        days = await cls.get_instance().get_all_trade_days()
        await cal.save_calendar(days)
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

    @classmethod
    async def get_price(
        cls,
        sec: Union[List, str],
        end_date: Union[str, datetime.datetime],
        n_bars: Optional[int],
        start_date: Optional[Union[str, datetime.datetime]] = None,
    ) -> np.ndarray:
        fields = ['open', 'close', 'high', 'low', 'volume', 'money', 'high_limit', 'low_limit', 'pre_close', 'avg', 'factor']
        params = {
            "security": sec,
            "end_date": end_date,
            "fields": fields,
            "fq": None,
            "fill_paused": False,
            "frequency": FrameType.MIN1.value,
        }
        if start_date:
            params.update({"start_date": start_date})
        if n_bars is not None:
            params.update({"count": start_date})
        if "start_date" in params and "count" in params:
            raise ValueError("start_date and count cannot appear at the same time")

        bars = await cls.get_instance().get_price(**params)

        if len(bars) == 0:
            return

    @classmethod
    async def get_quota(cls):
        quota = await cls.get_instance().get_query_count()
        return quota.get("spare")
