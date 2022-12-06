import datetime
import logging

import numpy as np
import talib as ta
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.serialize import DataframeDeserializer
from omicron.extensions.decimals import math_round
from omicron.models import get_influx_client
from omicron.models.timeframe import TimeFrame
from omicron.talib import moving_average

from omega.boards.board import ConceptBoard, IndustryBoard

logger = logging.getLogger(__name__)


board_bars_dtype = np.dtype(
    [
        ("code", "O"),
        ("frame", "datetime64[D]"),
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
        ("volume", "f8"),
        ("amount", "f8"),
    ]
)

board_bars_dtype2 = np.dtype(
    [
        ("_time", "datetime64[s]"),
        ("code", "O"),
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
        ("volume", "f8"),
        ("amount", "f8"),
    ]
)


async def get_latest_date_from_db(_code: str):
    # 行业板块回溯1年的数据，概念板块只取当年的数据
    code = f"{_code}.THS"

    client = get_influx_client()
    measurement = "board_bars_1d"

    now = datetime.datetime.now()
    dt_end = TimeFrame.day_shift(now, 0)
    # 250 + 60: 可以得到60个MA250的点, 默认K线图120个节点
    dt_start = TimeFrame.day_shift(now, -310)

    flux = (
        Flux()
        .measurement(measurement)
        .range(dt_start, dt_end)
        .bucket(client._bucket)
        .tags({"code": code})
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return dt_start

    ds = DataframeDeserializer(
        sort_values="_time", usecols=["_time"], time_col="_time", engine="c"
    )
    actual = ds(data)
    secs = actual.to_records(index=False).astype("datetime64[s]")

    _dt = secs[-1].item()
    return _dt.date()


async def save_board_bars(bars):
    client = get_influx_client()
    measurement = "board_bars_1d"

    logger.info("persisting bars to influxdb: %s, %d secs", measurement, len(bars))

    await client.save(bars, measurement, tag_keys=["code"], time_key="frame")

    return True


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


async def get_bars_in_range(board_id: str, start: datetime.date, end: datetime.date):
    start = datetime.datetime.combine(start, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(end, datetime.time(23, 59, 59))

    client = get_influx_client()
    measurement = "board_bars_1d"

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["open", "high", "low", "close", "volume", "amount"])
        .tags({"code": [board_id]})
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return []

    ds = DataframeDeserializer(
        sort_values="_time",
        usecols=["_time", "code", "open", "high", "low", "close", "volume", "amount"],
        time_col="_time",
        engine="c",
    )
    actual = ds(data)
    secs = actual.to_records(index=False).astype(board_bars_dtype2)
    return secs
