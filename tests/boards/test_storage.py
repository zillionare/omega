import datetime
import os
import shutil
import unittest
from unittest import mock

import cfg4py
import omicron

from omega.boards.storage import get_latest_date_from_db
from tests import init_test_env


class BoardsAppTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def test_get_latest_date_from_db(self):
        rc = await get_latest_date_from_db("301715")
        self.assertIsNotNone(rc)

        rc = await get_latest_date_from_db("3017151")
        self.assertIsNotNone(rc)
