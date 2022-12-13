import datetime
import logging

import numpy as np
import talib as ta
from omicron.extensions.decimals import math_round
from omicron.models.timeframe import TimeFrame
from omicron.talib import moving_average

logger = logging.getLogger(__name__)


def _convert_nparray(arr):
    newarr = []
    for item in arr:
        if np.isnan(item):
            newarr.append(0)
        else:
            newarr.append(math_round(item.item(), 2))
    return newarr


async def calculate_rsi_list(bars, param: int = 6):
    if bars is None:
        return []

    bars_n = len(bars)
    if bars_n < 120:
        return []

    _closed = bars["close"]
    if _closed.dtype != np.float64:
        _closed = _closed.astype(np.float64)

    _data = ta.RSI(_closed, param)
    new_data = np.nan_to_num(_data)
    return new_data.tolist()


async def calculate_ma_list(bars, more_data: bool = False):
    ma_price_list = bars["close"]
    ma_len = len(ma_price_list)

    price_ma_list = {}
    if ma_len >= 5:
        ma5 = moving_average(ma_price_list, 5)
        price_ma_list["ma5"] = _convert_nparray(ma5)
    if ma_len >= 10:
        ma10 = moving_average(ma_price_list, 10)
        price_ma_list["ma10"] = _convert_nparray(ma10)
    if ma_len >= 20:
        ma20 = moving_average(ma_price_list, 20)
        price_ma_list["ma20"] = _convert_nparray(ma20)
    if ma_len >= 30:
        ma30 = moving_average(ma_price_list, 30)
        price_ma_list["ma30"] = _convert_nparray(ma30)
    if ma_len >= 60:
        ma60 = moving_average(ma_price_list, 60)
        price_ma_list["ma60"] = _convert_nparray(ma60)

    if more_data:
        if ma_len >= 120:
            ma120 = moving_average(ma_price_list, 120)
            price_ma_list["ma120"] = _convert_nparray(ma120)
        if ma_len >= 250:
            ma250 = moving_average(ma_price_list, 250)
            price_ma_list["ma250"] = _convert_nparray(ma250)

    return price_ma_list
