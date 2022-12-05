import datetime
import os
import shutil
import unittest
from unittest import mock

import arrow
import pandas as pd
from numpy.testing import assert_array_equal

from omega.boards.board import ConceptBoard, IndustryBoard
from omega.boards.webapi import combined_filter
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

    async def test_combined_filter(self):
        actual = combined_filter("种植业与林业", ["粮食概念", "转基因"])
        self.assertSetEqual({"000998", "002041"}, actual)
