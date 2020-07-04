#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Interface for quotes fetcher
"""
from abc import ABC

import numpy
from omicron.core.types import FrameType, Frame


class QuotesFetcher(ABC):
    async def get_security_list(self) -> numpy.ndarray:
        """
        fetch security list from server. The returned list is a numpy.ndarray,
        which each elements
        should look like:
        code           display_name name 	start_date 	end_date 	type
        000001.XSHE 	平安银行 	PAYH 	1991-04-03 	2200-01-01 	stock
        000002.XSHE 	万科A 	    WKA 	1991-01-29 	2200-01-01 	stock

        all fields are string type
        Returns:

        """
        raise NotImplementedError

    async def get_bars(self, sec: str, end: Frame, n_bars: int,
                       frame_type: FrameType) -> numpy.ndarray:
        """
        fetch quotes of sec. Return a numpy rec array with n_bars length, and last
        frame is end。
        取n个单位的k线数据，k线周期由frame_type指定。最后结束周期为end。股票停牌期间的数据
        会使用None填充。
        Args:
            sec:
            end:
            n_bars:
            frame_type:

        Returns:
            a numpy.ndarray, with each element is:
            'frame': datetime.date or datetime.datetime, depends on frame_type.
            Denotes which time frame the data
            belongs .
            'open, high, low, close': float
            'volume': double
            'amount': the buy/sell amount in total, double
            'factor': float, may exist or not
        """
        raise NotImplementedError

    async def create_instance(self, **kwargs):
        raise NotImplementedError

    async def get_all_trade_days(self):
        """
        返回交易日历。不同的服务器可能返回的时间跨度不一样，但相同跨度内的时间应该一样。对已
        经过去的交易日，可以用上证指数来验证。
        """
        raise NotImplementedError
