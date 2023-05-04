import datetime
import os
import shutil
import unittest
from unittest import mock

import omicron
from freezegun import freeze_time

from omega.boards.app import AKShareFetcher, board_task_entry
from tests import init_test_env


class BoardsAppTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def test_fetcher_init(self):
        fetcher = AKShareFetcher()

        # 正常初始化和关闭
        fetcher.init()
        fetcher.close()

        # 初始化异常覆盖
        with mock.patch("omega.boards.app.omicron.init") as f:
            with mock.patch("omega.boards.app.os._exit") as f2:
                f2.return_value = True
                f.side_effect = [ValueError("aaaa")]
                await fetcher.init()

    @mock.patch("asyncio.unix_events._UnixSelectorEventLoop.run_until_complete")
    async def test_board_task_entry(self, _ruc):
        _ruc.return_value = True

        # just for cov
        board_task_entry("sync_industry_bars")
        board_task_entry("sync_concept_bars")
        board_task_entry("sync_industry_list")
        board_task_entry("sync_industry_list")

    @mock.patch("omega.boards.app.boards_init")
    @mock.patch("omega.boards.app.sync_board_names")
    @mock.patch("omega.boards.app.fetch_industry_day_bars")
    @mock.patch("omega.boards.app.fetch_concept_day_bars")
    async def test_fetch_day_bars(self, _cb, _ib, _bn, _binit):
        _binit.return_value = True
        _cb.return_value = True
        _ib.return_value = True

        with freeze_time("2022-12-04 10:10:10"):
            fetcher = AKShareFetcher()
            rc = await fetcher.fetch_day_bars("industry")
            self.assertFalse(rc)
            await fetcher.close()

        with freeze_time("2022-12-02 10:10:10"):
            fetcher = AKShareFetcher()
            _bn.return_value = True
            rc = await fetcher.fetch_day_bars("industry")
            self.assertTrue(rc)
            await fetcher.close()

            fetcher = AKShareFetcher()
            _bn.return_value = True
            rc = await fetcher.fetch_day_bars("concept")
            self.assertTrue(rc)
            await fetcher.close()

    @mock.patch("omega.boards.app.boards_init")
    @mock.patch("omega.boards.app.sync_board_names")
    @mock.patch("omega.boards.app.fetch_board_members")
    async def test_fetch_members(self, _bm, _bn, _bi):
        _bi.return_value = True
        _bm.return_value = True

        with freeze_time("2022-12-04 10:10:10"):
            fetcher = AKShareFetcher()
            rc = await fetcher.fetch_members("industry")
            self.assertFalse(rc)
            await fetcher.close()

        with freeze_time("2022-12-02 10:10:10"):
            fetcher = AKShareFetcher()
            _bn.return_value = False
            rc = await fetcher.fetch_members("industry")
            self.assertFalse(rc)
            await fetcher.close()

            fetcher = AKShareFetcher()
            _bn.return_value = True
            rc = await fetcher.fetch_members("industry")
            self.assertTrue(rc)
            await fetcher.close()
