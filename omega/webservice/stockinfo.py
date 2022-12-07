import datetime
import logging
from typing import List

from coretypes import Frame, FrameType
from omicron.dal.cache import cache
from omicron.models.security import Security
from omicron.models.timeframe import TimeFrame

logger = logging.getLogger(__name__)


def convert_ft_from_str(ft_str: str):
    if ft_str == "1d":
        ft = FrameType.DAY
    elif ft_str == "30m":
        ft = FrameType.MIN30
    elif ft_str == "1w":
        ft = FrameType.WEEK
    elif ft_str == "1m":
        ft = FrameType.MIN1
    elif ft_str == "5m":
        ft = FrameType.MIN5
    elif ft_str == "60m":
        ft = FrameType.MIN60
    elif ft_str == "1M":
        ft = FrameType.MONTH
    else:
        raise ValueError("not supported")

    return ft


# 今日股票清单
class GlobalStockInfo:
    _stocks = None

    @classmethod
    async def load_all_securities(cls):
        # "000001.XSHE,\xe5\xb9\xa1\x8c,PAYH,1991-04-03,2200-01-01,stock"
        sec_list = {}
        _tmplist = await cache.security.lrange("security:all", 0, -1)
        for _item in _tmplist:
            secs = _item.split(",")
            _type = secs[5]
            if _type == "stock":
                _code = secs[0]
                sec_list[_code.split(".")[0]] = secs[1]

        cls._stocks = sec_list
        logger.info("%d securities loaded (security info)", len(sec_list))

    @classmethod
    def get_stock_name(cls, code: str):
        return cls._stocks.get(code, "")


async def frame_shift(dt: datetime.datetime, ft_str: str, n_count: int):
    ft = convert_ft_from_str(ft_str)
    if ft >= FrameType.DAY:
        _tmp_dt = datetime.date(dt.year, dt.month, dt.day)
    else:
        _tmp_dt = TimeFrame.floor(dt, ft)
    rc = TimeFrame.shift(_tmp_dt, n_count, ft)
    if hasattr(rc, "date"):
        return {"dt": rc.strftime("%Y-%m-%d %H:%M:%S")}
    else:
        return {"dt": rc.strftime("%Y-%m-%d")}


async def frame_count(start: datetime.datetime, end: datetime.datetime, ft_str: str):
    ft = convert_ft_from_str(ft_str)
    if ft >= FrameType.DAY:
        dt_start = datetime.date(start.year, start.month, start.day)
        dt_end = datetime.date(end.year, end.month, end.day)
    else:
        dt_start = TimeFrame.floor(start, ft)
        dt_end = TimeFrame.floor(end, ft)

    rc = TimeFrame.count_frames(dt_start, dt_end, ft)
    return {"count": rc}


async def get_stock_info(security: str):
    info = await Security.info(security)
    _start = info["start"]
    _end = info["end"]
    info["start"] = _start.strftime("%Y-%m-%d")
    info["end"] = _end.strftime("%Y-%m-%d")
    return info
