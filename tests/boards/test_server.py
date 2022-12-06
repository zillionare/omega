import datetime
import os
import shutil
import unittest
from unittest import mock

import cfg4py
import omicron
from freezegun import freeze_time
from omicron.models import get_influx_client

from omega.boards.board import ConceptBoard, IndustryBoard
from omega.boards.server import (
    boards_init,
    fetch_board_members,
    fetch_concept_day_bars,
    fetch_industry_day_bars,
    sync_board_names,
)
from omega.boards.storage import get_latest_date_from_db
from tests import init_test_env
from tests.boards import concept_names, industry_item_bars, industry_names


class BoardsServerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def test_boards_init(self):
        cfg = cfg4py.get_instance()
        _tmp_path = cfg.zarr.store_path
        shutil.rmtree(_tmp_path, ignore_errors=True)

        rc = boards_init()
        self.assertTrue(rc)

        # just for cov
        with mock.patch("omega.boards.board.ConceptBoard.info") as f1:
            with mock.patch("omega.boards.board.IndustryBoard.info") as f2:
                f1.return_value = {"last_sync_date": "2022-12-05", "history": [1]}
                f2.return_value = {"last_sync_date": "2022-12-05", "history": [1]}
                rc = boards_init()
                self.assertTrue(rc)

    @mock.patch("omega.boards.board.ConceptBoard.fetch_board_list")
    @mock.patch("omega.boards.board.ConceptBoard.init")
    @mock.patch("omega.boards.board.IndustryBoard.fetch_board_list")
    @mock.patch("omega.boards.board.IndustryBoard.init")
    async def test_sync_board_names(self, f11, f12, f23, f24):
        # just for cov
        f11.return_value = True
        f12.return_value = True
        f23.return_value = True
        f24.return_value = True

        rc = sync_board_names("industry")
        self.assertTrue(rc)
        rc = sync_board_names("concept")
        self.assertTrue(rc)

        f11.side_effect = [ValueError("aaaaa")]
        rc = sync_board_names("industry")
        self.assertFalse(rc)

    async def test_fetch_industry_day_bars1(self):
        cfg = cfg4py.get_instance()
        _tmp_path = cfg.zarr.store_path
        shutil.rmtree(_tmp_path, ignore_errors=True)

        dt = datetime.date(2022, 12, 6)
        rc = await fetch_industry_day_bars(dt)
        self.assertFalse(rc)

    async def test_fetch_industry_day_bars2(self):
        name = "board_bars_1d"
        client = get_influx_client()
        await client.drop_measurement(name)

        cfg = cfg4py.get_instance()
        _tmp_path = cfg.zarr.store_path
        shutil.rmtree(_tmp_path, ignore_errors=True)

        IndustryBoard.init()

        # loading data
        with mock.patch(
            "omega.boards.board.stock_board_industry_name_ths",
            return_value=industry_names,
        ):
            IndustryBoard.fetch_board_list()

        dt = datetime.date(2022, 12, 6)
        dt2 = datetime.date(2022, 12, 2)
        with mock.patch("omega.boards.server.get_latest_date_from_db") as f1:
            # force error 1
            f1.return_value = dt
            rc = await fetch_industry_day_bars(dt)
            self.assertTrue(rc)

            f1.return_value = dt2
            with mock.patch("omega.boards.board.IndustryBoard.get_industry_bars") as f2:
                # force error 2
                f2.return_value = []
                rc = await fetch_industry_day_bars(dt)
                self.assertTrue(rc)

                # reading data from industry_item_bars
                f2.return_value = industry_item_bars
                rc = await fetch_industry_day_bars(dt)
                self.assertTrue(rc)

    async def test_fetch_concept_day_bars1(self):
        cfg = cfg4py.get_instance()
        _tmp_path = cfg.zarr.store_path
        shutil.rmtree(_tmp_path, ignore_errors=True)

        dt = datetime.date(2022, 12, 5)
        rc = await fetch_concept_day_bars(dt)
        self.assertFalse(rc)

    async def test_fetch_concept_day_bars2(self):
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

        dt = datetime.date(2022, 12, 5)
        dt2 = datetime.date(2022, 12, 2)
        with mock.patch("omega.boards.server.get_latest_date_from_db") as f1:
            # 时间相等
            f1.return_value = dt2
            rc = await fetch_concept_day_bars(dt2)
            self.assertTrue(rc)

            f1.return_value = dt
            with mock.patch("omega.boards.board.ConceptBoard.get_concept_bars") as f2:
                # 取不到数据
                f2.return_value = []
                rc = await fetch_concept_day_bars(datetime.date(2022, 5, 25))
                self.assertTrue(rc)

                # reading data from industry_item_bars
                f2.return_value = industry_item_bars
                # 数据时间太旧
                rc = await fetch_concept_day_bars(dt2)
                self.assertTrue(rc)
            f1.return_value = datetime.date(2022, 11, 30)
            with mock.patch("omega.boards.board.ConceptBoard.get_concept_bars") as f2:
                # reading data from industry_item_bars
                f2.return_value = industry_item_bars
                rc = await fetch_concept_day_bars(dt2)
                self.assertTrue(rc)

        # test get_latest_date_from_db
        dt = await get_latest_date_from_db("300435")
        self.assertEqual(dt, datetime.date(2022, 12, 2))

        # just for cov
        dt = await get_latest_date_from_db("300535")
        self.assertLess(dt, datetime.date(2022, 12, 2))

    async def test_fetch_board_members(self):
        # just for cov

        with mock.patch("omega.boards.board.ConceptBoard.fetch_board_members") as f1:
            f1.return_value = True
            await fetch_board_members("concept")

            f1.side_effect = [ValueError("aaaa")]
            await fetch_board_members("concept")

        with mock.patch("omega.boards.board.IndustryBoard.fetch_board_members") as f2:
            f2.return_value = True
            await fetch_board_members("industry")

            f2.side_effect = [ValueError("aaaa")]
            await fetch_board_members("industry")
