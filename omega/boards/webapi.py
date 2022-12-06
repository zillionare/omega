import datetime
import json
import logging
import os
import re
import sys
import time
from typing import Any, List, Optional

from coretypes import Frame, FrameType
from omicron.extensions.decimals import math_round
from omicron.models.timeframe import TimeFrame

from omega.boards.board import ConceptBoard, IndustryBoard
from omega.boards.storage import (
    calculate_ma_list,
    calculate_rsi_list,
    get_bars_in_range,
)
from omega.webservice.stockinfo import GlobalStockInfo

logger = logging.getLogger(__name__)


def new_boards(days: int = 10):
    cb = ConceptBoard()
    cb.init()
    result = cb.find_new_concept_boards(days)
    if result is None or len(result) == 0:
        print(f"近{days}天内没有新的概念板块")
    else:
        print(result)


def latest_boards(n: int = 3):
    cb = ConceptBoard()
    cb.init()
    df = cb.find_latest_n_concept_boards(n)
    print(df)


def new_members(days: int = 10, prot: int = None):
    cb = ConceptBoard()
    cb.init()
    try:
        results = cb.new_members_in_board(days)
        if len(results) == 0:
            print(f"近{days}天内没有板块有新增成员")
        else:
            for board, stocks in results.items():
                print(cb.get_name(board) + ":")
                aliases = [cb.get_stock_alias(stock) for stock in stocks]
                print(" ".join(aliases))
    except Exception as e:
        print(e)


def combined_filter(
    industry: str = None, with_concepts: Optional[List[str]] = None, without=[]
) -> List[str]:
    """针对行业板块与概念板块的联合筛选

    Args:
        industry: 返回代码必须包含在这些行业板块内
        with_concepts: 返回代码必须包含在这些概念内
        without: 返回代码必须不在这些概念内

    Returns:
        股票代码列表
    """
    if with_concepts is not None:
        cb = ConceptBoard()
        cb.init()

        if isinstance(with_concepts, str):
            with_concepts = [with_concepts]

        if isinstance(without, str):
            without = [without]
        concepts_codes = set(cb.filter(with_concepts, without=without))
    else:
        concepts_codes = None

    codes = None
    if industry is not None:
        ib = IndustryBoard()
        ib.init()

        codes = ib.filter([industry])
        if codes is not None:
            codes = set(codes)
    else:
        codes = None

    final_results = []
    if codes is None or concepts_codes is None:
        final_results = codes or concepts_codes
    else:
        final_results = codes.intersection(concepts_codes)

    return final_results


def filter(industry=None, with_concepts: Optional[List[str]] = None, without=[]):
    if industry is not None and isinstance(industry, int):
        industry = str(industry)

    if with_concepts is not None and isinstance(with_concepts, list):
        with_concepts = [str(item) for item in with_concepts]
    elif isinstance(with_concepts, str):
        with_concepts = re.split(r"[，,]", with_concepts)

    if without is not None and isinstance(without, list):
        without = [str(item) for item in without]
    elif isinstance(without, str):
        without = re.split(r"[,，]", without)

    results = combined_filter(industry, with_concepts, without)

    if industry is None:
        board = IndustryBoard()
        board.init()
    else:
        board = ConceptBoard()
        board.init()

    for code in results:
        name = board.get_stock_alias(code)
        print(code, name)


def list_boards(sub: str):
    result = []

    if sub == "concept":
        cb = ConceptBoard()
        for i, (_, code, name, count) in enumerate(cb.boards):
            result.append((code, name, count.item()))
    elif sub == "industry":
        ib = IndustryBoard()
        for i, (code, name, count) in enumerate(ib.boards):
            result.append((code, name, count.item()))

    return result


def concepts_info_by_sec(security: str):
    # 给定股票名称，返回所属概念信息
    bl = []
    result = {"security": security, "bl": bl}

    cb = ConceptBoard()
    sec = security.split(".")[0]
    for board in cb.get_boards(sec):
        bl.append((board, cb.get_name(board)))

    return result


def industry_info_by_sec(security: str):
    # 给定股票名称，返回所属行业信息
    bl = []
    result = {"security": security, "bl": bl}

    ib = IndustryBoard()
    sec = security.split(".")[0]
    for board in ib.get_boards(sec):
        bl.append((board, ib.get_name(board)))

    return result


def board_fuzzy_match(board_type: str, pattern: str):
    if board_type == "industry":
        handler = IndustryBoard()
    else:
        handler = ConceptBoard()

    codes = handler.fuzzy_match_board_name(pattern)
    if not codes:
        return []

    results = []
    for _item in codes:
        _name = handler.get_name(_item)
        if not _name:
            continue
        results.append(f"{_item} {_name}")

    return results


def get_board_info_by_id(board_type: str, board_id: str, _mode: int = 0):
    if board_type == "industry":
        handler = IndustryBoard()
    else:
        handler = ConceptBoard()

    _info = handler.get_board_info(board_id)
    if not _info:
        return {}

    if _mode == 0:
        return {"code": board_id, "name": _info[0], "stocks": _info[1].item()}

    _list = handler.get_members(board_id, with_name=True)
    if not _list:
        return {"code": board_id, "name": _info[0], "stocks": _info[1].item()}
    else:
        return {"code": board_id, "name": _info[0], "stocks": _list}


def get_boards_by_sec(board_type: str, security: str):
    if board_type == "industry":
        handler = IndustryBoard()
    else:
        handler = ConceptBoard()

    bl = handler.get_boards(security)
    if len(bl) == 0:
        return []

    result = []
    for board_id in bl:
        _info = handler.get_board_info(board_id)
        if not _info:
            continue
        result.append({"code": board_id, "name": _info[0], "stocks": _info[1].item()})

    return result


def board_filter_members(
    board_type: str, included: List[str], excluded: List[str] = []
):
    if board_type == "industry":
        handler = IndustryBoard()
    else:
        handler = ConceptBoard()

    codes = handler.filter(included, without=excluded)
    if not codes:
        return []

    stock_list = []
    for _item in codes:
        _stock_name = GlobalStockInfo.get_stock_name(_item)
        if not _stock_name:  # 退市或者北交所的股票忽略
            continue
        stock_list.append([_item, _stock_name])

    return stock_list


async def get_board_bars_bycount(board_id: str, dt_end: datetime.date, n_bars: int):
    now = datetime.datetime.now()
    if not TimeFrame.is_trade_day(now):
        dt = TimeFrame.day_shift(now, 0)
    else:
        dt = now.date()

    # 为了计算MA250，取250+60根
    _end = dt_end
    if _end > dt:
        _end = dt
    if n_bars >= 120:  # 约定最大n_bars为250
        _start = TimeFrame.shift(_end, -310, FrameType.DAY)
    else:
        _start = TimeFrame.shift(_end, -n_bars - 30, FrameType.DAY)

    board_info = {}
    sec_data = await get_bars_in_range(board_id, _start, _end)
    ma_list = await calculate_ma_list(sec_data, more_data=True)
    rsi_list = await calculate_rsi_list(sec_data)
    ma_list["rsi6"] = rsi_list

    # 只取最后120个节点
    for _key in ma_list:
        _raw_data = ma_list[_key]
        _count = len(_raw_data)
        if _count > n_bars:
            ma_list[_key] = _raw_data[_count - n_bars :]

    k_bars = []
    for item in sec_data:
        _date = item["_time"].item()
        _data = {
            "frame": _date.strftime("%Y-%m-%d %H:%M:%S"),
            "data": [
                math_round(item["open"].item(), 2),
                math_round(item["close"].item(), 2),
                math_round(item["low"].item(), 2),
                math_round(item["high"].item(), 2),
                math_round(item["volume"].item() / 100, 0),
                math_round(item["amount"].item() / 10000, 0),
            ],
        }
        k_bars.append(_data)
    _count = len(k_bars)
    if _count > n_bars:
        board_info["bars"] = k_bars[_count - n_bars :]
    else:
        board_info["bars"] = k_bars
    board_info.update(ma_list)

    return board_info
