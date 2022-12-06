import datetime
import os
import shutil
import unittest
from unittest import mock

import arrow
import omicron
import pandas as pd
from numpy.testing import assert_array_equal

from omega.boards.board import ConceptBoard, IndustryBoard
from omega.boards.webapi import (
    board_filter_members,
    board_fuzzy_match,
    combined_filter,
    concepts_info_by_sec,
    get_board_info_by_id,
    get_boards_by_sec,
    industry_info_by_sec,
    list_boards,
)
from omega.webservice.stockinfo import GlobalStockInfo
from tests import init_test_env
from tests.boards import (
    concept_members,
    concept_names,
    industry_members,
    industry_names,
)


class BoardsWebAPITest(unittest.IsolatedAsyncioTestCase):
    @mock.patch(
        "omega.boards.board.stock_board_industry_name_ths", return_value=industry_names
    )
    @mock.patch(
        "omega.boards.board.stock_board_industry_cons_ths", side_effect=industry_members
    )
    @mock.patch(
        "omega.boards.board.stock_board_concept_name_ths", return_value=concept_names
    )
    @mock.patch(
        "omega.boards.board.stock_board_concept_cons_ths", side_effect=concept_members
    )
    async def asyncSetUp(self, mock_0, mock_1, mock_2, mock_3) -> None:
        await init_test_env()
        shutil.rmtree("/tmp/boards.zarr", ignore_errors=True)
        await omicron.init()

        with mock.patch("arrow.now", return_value=arrow.get("2022-05-26")):
            IndustryBoard.close()
            ConceptBoard.close()

            IndustryBoard.init()
            IndustryBoard.fetch_board_list()
            IndustryBoard.fetch_board_members()

            ConceptBoard.init()
            ConceptBoard.fetch_board_list()
            ConceptBoard.fetch_board_members()

            ib = IndustryBoard()
            cb = ConceptBoard()
            assert_array_equal(ib.boards["name"], ["种植业与林业", "种子生产", "其他种植业"])
            assert_array_equal(cb.boards["name"], ["农业种植", "粮食概念", "转基因"])

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def test_combined_filter(self):
        actual = combined_filter("种植业与林业", ["粮食概念", "转基因"])
        self.assertSetEqual({"000998", "002041"}, actual)

    async def test_list_boards(self):
        rc = list_boards("industry")
        self.assertTrue(len(rc) > 0)

    async def test_industry_info_by_sec(self):
        rc = industry_info_by_sec("002041.XSHE")
        self.assertTrue(len(rc) > 0)

    async def test_concept_info_by_sec(self):
        rc = concepts_info_by_sec("002041.XSHE")
        self.assertTrue(len(rc) > 0)

    async def test_board_fuzzy_match(self):
        rc = board_fuzzy_match("concept", "种")
        self.assertTrue(len(rc) > 0)

        rc = board_fuzzy_match("industry", "种")
        self.assertTrue(len(rc) > 0)

    async def test_get_board_info_by_id(self):
        rc = get_board_info_by_id("concept", "308016")
        self.assertTrue(len(rc) > 0)
        rc = get_board_info_by_id("concept", "308016", _mode=1)
        self.assertTrue(len(rc) > 0)

        rc = get_board_info_by_id("industry", "884001")
        self.assertTrue(len(rc) > 0)
        rc = get_board_info_by_id("industry", "884001", _mode=1)
        self.assertTrue(len(rc) > 0)

    async def test_get_boards_by_sec(self):
        rc = get_boards_by_sec("concept", "002041")
        self.assertTrue(len(rc) > 0)

        rc = get_boards_by_sec("industry", "002041")
        self.assertTrue(len(rc) > 0)

    async def test_board_filter_members(self):
        await GlobalStockInfo.load_all_securities()
        GlobalStockInfo._stocks = {
            "002041": "登海种业",
            "600265": "ST景谷",
            "000998": "隆平高科",
            "601118": "海南橡胶",
            "600598": "北大荒",
            "002582": "好想你",
            "600108": "亚盛集团",
        }

        rc = board_filter_members("concept", ["308016"], ["300435"])
        self.assertTrue(len(rc) > 0)

        rc = board_filter_members("industry", ["881101"], ["884003"])
        self.assertTrue(len(rc) > 0)
