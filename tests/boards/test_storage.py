import datetime
import os
import shutil
import unittest
from unittest import mock

import cfg4py
import omicron
from omicron.models import get_influx_client
from omicron.models.board import Board

from omega.boards.board import ConceptBoard
from omega.boards.server import fetch_concept_day_bars
from omega.boards.webapi import get_board_bars_bycount
from tests import init_test_env
from tests.boards import concept_names, industry_item_bars, industry_names


class BoardsStorageTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def init_storage(self):
        name = "board_bars_1d"
        client = get_influx_client()
        await client.drop_measurement(name)

        cfg = cfg4py.get_instance()
        _tmp_path = cfg.zarr.store_path
        shutil.rmtree(_tmp_path, ignore_errors=True)

        ConceptBoard.init()

        # loading data
        with mock.patch(
            "omega.boards.board.stock_board_concept_name_ths",
            return_value=concept_names,
        ):
            ConceptBoard.fetch_board_list()

        dt2 = datetime.date(2022, 12, 2)
        with mock.patch("omega.boards.board.ConceptBoard.get_concept_bars") as f2:
            f2.return_value = industry_item_bars
            await fetch_concept_day_bars(dt2, delay=0)

    async def test_get_latest_date_from_db(self):
        # 不存在
        rc = await Board.get_last_date_of_bars("301715")
        self.assertIsNotNone(rc)

        # 存在，2022-12-01
        rc = await Board.get_last_date_of_bars("300435")
        self.assertEqual(rc, datetime.date(2022, 12, 5))

    async def test_get_bars_in_range(self):
        await self.init_storage()

        dt1 = datetime.date(2022, 1, 1)
        dt2 = datetime.date(2022, 12, 31)
        bars = await Board.get_bars_in_range("300435", dt1, dt2)
        self.assertIsNotNone(bars)

        dt3 = datetime.date(2022, 12, 5)
        rc = await get_board_bars_bycount("300435", dt3, 251)
        self.assertIsNotNone(rc)
