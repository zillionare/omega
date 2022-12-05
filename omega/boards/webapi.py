"""Console script for boards."""

import json
import logging
import os
import re
import sys
import time
from typing import Any, List, Optional

from omega.boards.board import ConceptBoard, IndustryBoard

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


def concepts(code: str):
    cb = ConceptBoard()
    cb.init()

    for board in cb.get_boards(code):
        print(board, cb.get_name(board))


def industry(code: str):
    ib = IndustryBoard()
    ib.init()

    for board in ib.get_boards(code):
        print(board, ib.get_name(board))


def list_boards(sub: str):
    if sub == "concepts":
        cb = ConceptBoard()
        cb.init()
        for i, (date, code, name, *_) in enumerate(cb.boards):
            print(date, code, name)
    elif sub == "industry":
        ib = IndustryBoard()
        ib.init()
        for i, (date, code, name, *_) in enumerate(ib.boards):
            print(date, code, name)
