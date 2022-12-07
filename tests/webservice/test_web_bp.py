import datetime
import shutil
import unittest
from unittest import mock

import cfg4py
import omicron
from omicron.models import get_influx_client

from omega.boards.board import ConceptBoard, IndustryBoard
from omega.boards.server import fetch_concept_day_bars
from omega.webservice.web_bp import (
    bp_admin_stock_info,
    bp_webapi_board_bars_info,
    bp_webapi_board_filter_members,
    bp_webapi_board_get_info,
    bp_webapi_board_get_name,
    bp_webapi_board_info_by_sec,
    bp_webapi_board_list,
    bp_webapi_cb_list_by_sec,
    bp_webapi_frame_count,
    bp_webapi_frame_shift,
    bp_webapi_ib_list_by_sec,
)
from tests import init_test_env
from tests.boards import concept_members, concept_names, industry_item_bars


class MockRequest:
    json = {}


class WebSvcBPTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

        await self.init_storage()

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
        IndustryBoard.init()

        # loading data
        with mock.patch(
            "omega.boards.board.stock_board_concept_name_ths",
            return_value=concept_names,
        ):
            ConceptBoard.fetch_board_list()

        with mock.patch(
            "omega.boards.board.stock_board_concept_cons_ths",
            side_effect=concept_members,
        ):
            ConceptBoard.fetch_board_members()

        dt2 = datetime.date(2022, 12, 2)
        with mock.patch("omega.boards.board.ConceptBoard.get_concept_bars") as f2:
            f2.return_value = industry_item_bars
            await fetch_concept_day_bars(dt2)

    async def test_bp_frame_shift(self):
        request = MockRequest()
        await bp_webapi_frame_shift(request)

        request.json = {"dt": "abc"}
        with self.assertRaises(BaseException):
            await bp_webapi_frame_shift(request)

        request.json = {"dt": "2022-12-05"}
        await bp_webapi_frame_shift(request)

        request.json["ft"] = "1d"
        with self.assertRaises(BaseException):
            await bp_webapi_frame_shift(request)

        request.json["count"] = 1
        await bp_webapi_frame_shift(request)

    async def test_bp_frame_count(self):
        request = MockRequest()
        await bp_webapi_frame_count(request)

        request.json = {"start": "2022-12-02"}
        await bp_webapi_frame_count(request)

        request.json["end"] = "2022-12-05"
        await bp_webapi_frame_count(request)

        request.json["ft"] = "1d"
        await bp_webapi_frame_count(request)

    async def test_bp_stock_info(self):
        request = MockRequest()
        await bp_admin_stock_info(request)

        request.json = {"security": "000001.XSHE"}
        await bp_admin_stock_info(request)

    async def test_bp_board_list(self):
        request = MockRequest()
        await bp_webapi_board_list(request)

        request.json = {"board_type": "concept"}
        await bp_webapi_board_list(request)

    async def test_bp_ib_list_by_sec(self):
        request = MockRequest()
        await bp_webapi_ib_list_by_sec(request)

        request.json = {"security": "000001.XSHE"}
        with mock.patch("omega.webservice.web_bp.industry_info_by_sec") as f:
            f.return_value = {}
            await bp_webapi_ib_list_by_sec(request)

    async def test_bp_cb_list_by_sec(self):
        request = MockRequest()
        await bp_webapi_cb_list_by_sec(request)

        request.json = {"security": "002041.XSHE"}
        await bp_webapi_cb_list_by_sec(request)

    async def test_bp_board_get_name(self):
        request = MockRequest()
        await bp_webapi_board_get_name(request)

        request.json = {"board_type": "concept", "pattern": "电"}
        await bp_webapi_board_get_name(request)

    async def test_bp_board_get_info(self):
        request = MockRequest()
        await bp_webapi_board_get_info(request)

        request.json = {"board_type": "concept", "board_id": "308016"}
        await bp_webapi_board_get_info(request)

        request.json["fullmode"] = 1
        await bp_webapi_board_get_info(request)

    async def test_bp_board_info_by_sec(self):
        request = MockRequest()
        await bp_webapi_board_info_by_sec(request)

        request.json = {"board_type": "concept", "security": "002041.XSHE"}
        await bp_webapi_board_info_by_sec(request)

    @mock.patch("omega.webservice.stockinfo.GlobalStockInfo.get_stock_name")
    async def test_bp_board_filter_members(self, _stock):
        _stock.return_value = "ABC"

        request = MockRequest()
        await bp_webapi_board_filter_members(request)

        request.json = {"board_type": "concept", "include_boards": "308016"}
        await bp_webapi_board_filter_members(request)
        request.json["include_boards"] = ["308016"]
        await bp_webapi_board_filter_members(request)

        request.json["exclude_boards"] = "308956"
        await bp_webapi_board_filter_members(request)
        request.json["exclude_boards"] = ["308956"]
        await bp_webapi_board_filter_members(request)

    async def test_bp_board_bars_info(self):
        request = MockRequest()
        await bp_webapi_board_bars_info(request)

        request.json = {
            "board_type": "concept",
            "board_id": "308956",
            "end": "2022-12-05",
            "n_bars": 121,
        }
        await bp_webapi_board_bars_info(request)
