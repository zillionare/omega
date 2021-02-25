#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Interface for quotes fetcher
"""
from abc import ABC
from typing import List, Union

import numpy
from omicron.core.types import Frame, FrameType


class QuotesFetcher(ABC):
    async def get_security_list(self) -> numpy.ndarray:
        """
        fetch security list from server. The returned list is a numpy.ndarray,
        which each elements
        should look like:
        code         display_name name  start_date  end_date     type
        000001.XSHE  平安银行      PAYH  1991-04-03  2200-01-01   stock
        000002.XSHE   万科A        WKA   1991-01-29  2200-01-01   stock

        all fields are string type
        Returns:

        """
        raise NotImplementedError

    async def get_bars(
        self,
        sec: str,
        end: Frame,
        n_bars: int,
        frame_type: FrameType,
        allow_unclosed=True,
    ) -> numpy.ndarray:
        """取n个单位的k线数据。

        k线周期由frame_type指定。最后结束周期为end。股票停牌期间的数据会使用None填充。
        Args:
            sec (str): 证券代码
            end (Frame):
            n_bars (int):
            frame_type (FrameType):
            allow_unclosed (bool): 为真时，当前未结束的帧数据也获取

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

    async def get_valuation(
        self, code: Union[str, List[str]], day: Frame
    ) -> numpy.ndarray:
        """读取code指定的股票在date指定日期的市值数据。

        返回数据包括：
            code: 股票代码
            day: 日期
            captialization: 总股本
            circulating_cap: 流通股本（万股）
            market_cap: 总市值（亿元）
            circulating_market_cap： 流通市值（亿元）
            turnover_ration: 换手率（%）
            pe_ratio: 市盈率（PE,TTM）每股市价为每股收益的倍数，反映投资人对每元净利润所愿支付的价
            格，用来估计股票的投资报酬和风险
            pe_ratio_lyr: 市盈率（PE），以上一年度每股盈利计算的静态市盈率. 股价/最近年度报告EPS
            pb_ratio: 市净率（PB）
            ps_ratio: 市销率(PS)
            pcf_ratio: 市现率（PCF）

        Args:
            code (Union[str, List[str]]): [description]
            day (Frame): [description]

        Returns:
            numpy.ndarray: [description]
        """
        raise NotImplementedError
