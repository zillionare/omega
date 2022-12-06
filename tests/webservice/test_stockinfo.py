import datetime
import os
import shutil
import unittest
from unittest import mock

import omicron

from omega.webservice.stockinfo import frame_shift, get_stock_info
from tests import init_test_env


class WebServiceTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def test_frame_shift(self):
        dt = datetime.datetime(2022, 12, 2, 10, 11, 12)
        rc = await frame_shift(dt, "1d", -1)
        self.assertEqual(rc["result"], "2022-12-01")

        dt = datetime.datetime(2022, 12, 2, 10, 11, 12)
        rc = await frame_shift(dt, "30m", 0)
        self.assertEqual(rc["result"], "2022-12-02 10:00:00")

    async def test_get_stock_info(self):
        # 000001.XSHE
        rc = await get_stock_info("000001.XSHE")
        self.assertEqual(rc["name"], "PAYH")
