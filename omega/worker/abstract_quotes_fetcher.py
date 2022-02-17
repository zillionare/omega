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
from coretypes import Frame, FrameType
from numpy.lib import recfunctions as rfn
from omicron.models.funds import FundNetValue, FundPortfolioStock, Funds, FundShareDaily
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
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
    async def get_all_trade_days(cls):
        days = await cls.get_instance().get_all_trade_days()
        await TimeFrame.save_calendar(days)
        return days

    @classmethod
    async def get_trade_price_limits(
        cls, sec: Union[List, str], dt: Union[str, Frame]
    ) -> np.ndarray:
    # fixme: 函数名未能正确反映函数功能，建议改为get_trade_limit_price(s)
        params = {
            "sec": sec,
            "dt": dt,
        }
        bars = await cls.get_instance().get_high_limit_price(**params)

        if len(bars) == 0:
            return None
        return bars

    @classmethod
    async def get_quota_spare(cls):
        quota = await cls.get_instance().get_quota()
        return quota.get("spare")

    @classmethod
    async def get_quota(cls):
        return cls.get_instance().get_quota()

    @classmethod
    async def get_fund_list(
        cls, code: Union[str, List[str]] = None, fields: List[str] = None
    ) -> Union[None, np.ndarray]:

        funds = await cls.get_instance().get_fund_list(code)

        if len(funds) == 0:
            logger.warning(f"failed to update funds. {funds} is returned")
            return funds
        await Funds.save(funds)

        if not fields:
            return funds
        if isinstance(fields, str):
            fields = [fields]
        mapping = dict(funds.dtype.descr)
        fields = [(name, mapping[name]) for name in fields]
        return rfn.require_fields(funds, fields)

    @classmethod
    async def get_fund_net_value(
        cls, code: Union[str, List[str]] = None, day=None, fields: List[str] = None
    ) -> Union[None, np.ndarray]:

        fund_net_values = await cls.get_instance().get_fund_net_value(code, day)

        if len(fund_net_values) == 0:
            logger.warning(f"failed to update funds. {fund_net_values} is returned")
            return fund_net_values
        await FundNetValue.save(fund_net_values, day=day)

        if not fields:
            return fund_net_values
        if isinstance(fields, str):
            fields = [fields]
        mapping = dict(fund_net_values.dtype.descr)
        fields = [(name, mapping[name]) for name in fields]
        return rfn.require_fields(fund_net_values, fields)

    @classmethod
    async def get_fund_share_daily(
        cls,
        code: Union[str, List[str]] = None,
        day: Union[str, datetime.date] = None,
        fields: List[str] = None,
    ) -> Union[None, np.ndarray]:

        fund_net_values = await cls.get_instance().get_fund_share_daily(code, day)

        if len(fund_net_values) == 0:
            logger.warning(f"failed to update funds. {fund_net_values} is returned")
            return fund_net_values
        await FundShareDaily.save(fund_net_values)

        if not fields:
            return fund_net_values
        if isinstance(fields, str):
            fields = [fields]
        mapping = dict(fund_net_values.dtype.descr)
        fields = [(name, mapping[name]) for name in fields]
        return rfn.require_fields(fund_net_values, fields)

    @classmethod
    async def get_fund_portfolio_stock(
        cls,
        code: Union[str, List[str]] = None,
        pub_date: Union[str, datetime.date] = None,
        fields: List[str] = None,
    ) -> Union[None, np.ndarray]:
        fund_net_values = await cls.get_instance().get_fund_portfolio_stock(
            code, pub_date
        )

        if len(fund_net_values) == 0:
            logger.warning(f"failed to update funds. {fund_net_values} is returned")
            return fund_net_values
        await FundPortfolioStock.save(fund_net_values)

        if not fields:
            return fund_net_values
        if isinstance(fields, str):
            fields = [fields]
        mapping = dict(fund_net_values.dtype.descr)
        fields = [(name, mapping[name]) for name in fields]
        return rfn.require_fields(fund_net_values, fields)

    @classmethod
    def max_result_size(cls, op:str)->int:
        return cls.get_instance().max_result_size(op)
