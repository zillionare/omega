import datetime
import logging
from typing import Dict, List, Union

import cfg4py
import numpy as np
from coretypes import FrameType, SecurityType
from retrying import retry

from omega.worker import exception
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as fetcher

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


@retry(stop_max_attempt_number=5)
async def fetch_price(
    secs: List[str],
    end: datetime.datetime,
    n_bars: int,
    frame_type: FrameType,
) -> Union[Dict[str, np.ndarray], np.ndarray]:
    """
    从上游获取k线数据
    Args:
        secs:待获取的股票代码列表
        end:需要获取的截止时间
        n_bars: 需要获取k线根数
        frame_type:需要获取的k线类型
        impl: 需要用什么方法取获取

    Returns:
        返回k线数据，为dict
    """
    # todo: omicron可以保存dict[code->bars],无需进行转换。这里的转换只是为了适应底层逻辑，没有特别的功能，在底层可以修改的情况下，可以不做不必要的转换。

    bars = await fetcher.get_price(secs, end=end, n_bars=n_bars, frame_type=frame_type)
    if bars is None:  # pragma: no cover
        raise exception.GotNoneData()
    for k in list(bars.keys()):
        if not len(bars[k]):
            del bars[k]
    return bars


@retry(stop_max_attempt_number=5)
async def fetch_bars(
    secs: List[str],
    end: datetime.datetime,
    n_bars: int,
    frame_type: FrameType,
) -> Union[Dict[str, np.ndarray], np.ndarray]:
    """
    从上游获取k线数据
    Args:
        secs:待获取的股票代码列表
        end:需要获取的截止时间
        n_bars: 需要获取k线根数
        frame_type:需要获取的k线类型

    Returns:
        返回k线数据，为dict
    """
    # todo: omicron可以保存dict[code->bars],无需进行转换。这里的转换只是为了适应底层逻辑，没有特别的功能，在底层可以修改的情况下，可以不做不必要的转换。

    bars = await fetcher.get_bars_batch(
        secs, end=end, n_bars=n_bars, frame_type=frame_type
    )
    if bars is None:  # pragma: no cover
        raise exception.GotNoneData()
    for k in list(bars.keys()):
        if not len(bars[k]):
            del bars[k]
            continue
        if np.any(np.isnan(bars[k]["amount"])) or np.any(np.isnan(bars[k]["volume"])):
            del bars[k]
            continue
        # 判断日期是否是end
        frame = bars[k]["frame"][0]
        if hasattr(frame, "date"):
            frame = frame.date()
        if hasattr(end, "date"):
            end = end.date()
        if frame != end:
            del bars[k]
            continue
    return bars


@retry(stop_max_attempt_number=5)
async def get_trade_price_limits(secs, end):
    """获取涨跌停价
    由于inflaxdb无法处理浮点数 nan 所以需要提前将nan转换为0
    """
    # todo: 可以重命名为get_trade_limit_price
    bars = await fetcher.get_trade_price_limits(secs, end)
    bars["low_limit"] = np.nan_to_num(bars["low_limit"])
    bars["high_limit"] = np.nan_to_num(bars["high_limit"])
    return bars
