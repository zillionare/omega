import datetime
import logging
from typing import Dict, List, Union

import cfg4py
import numpy as np
from coretypes import FrameType, SecurityType
from omicron.models.timeframe import TimeFrame
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

    if hasattr(end, "date"):
        d0 = end.date()
        d1 = end.date()
    else:
        d0 = end
        d1 = end
    if frame_type in (FrameType.WEEK, FrameType.MONTH):
        d0, d1 = TimeFrame.get_frame_scope(end, frame_type)

    for k in list(bars.keys()):
        if not len(bars[k]):
            del bars[k]
            continue
        if np.any(np.isnan(bars[k]["amount"])) or np.any(np.isnan(bars[k]["volume"])):
            del bars[k]
            continue

        # 周线和月线，数据必须在范围内，日线只需要判断是否为end即可
        frame = bars[k]["frame"][0]
        if hasattr(frame, "date"):
            frame = frame.date()
        if frame_type == FrameType.WEEK or frame_type == FrameType.MONTH:
            if frame < d0 or frame > d1:
                del bars[k]
                continue
        else:
            if frame != d0:
                del bars[k]
                continue
    return bars


@retry(stop_max_attempt_number=5)
async def get_trade_price_limits(secs, end):
    """获取涨跌停价
    由于inflaxdb无法处理浮点数 nan 所以需要提前将nan转换为0
    """
    # end必须为datetime.date
    bars = await fetcher.get_trade_price_limits(secs, end)

    # 滤掉无效数据
    if isinstance(end, datetime.datetime):
        target_date = end.date()
    else:
        target_date = end

    bars = bars[~np.isnan(bars["low_limit"])]
    bars = bars[~np.isnan(bars["high_limit"])]
    bars = bars[bars["frame"] == target_date]

    # 放弃以前的做法
    # bars["low_limit"] = np.nan_to_num(bars["low_limit"])
    # bars["high_limit"] = np.nan_to_num(bars["high_limit"])

    return bars
