import datetime
import logging
import os
import tarfile
import unittest

import cfg4py
import omicron
import pandas as pd
from omicron.core.types import FrameType
from omicron.dal import cache

from omega.fetcher import archive
from tests import init_test_env

logger = logging.getLogger(__name__)


class TestArchieveFetcher(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        init_test_env()
        self.cfg = cfg4py.get_instance()
        await omicron.init()

    async def test_archived_bars_handler_save(self):
        await cache.security.delete("000001.XSHE* ")
        h = archive.ArchivedBarsHandler("")

        data_file = os.path.join(os.path.dirname(__file__), "data/2019-01-stock.tgz")
        tar = tarfile.open(data_file, mode="r:gz")

        for member in tar.getmembers():
            f = tar.extractfile(member)
            df = pd.read_parquet(f)
            sec = member.name.split("/")[-1]
            self.assertEqual("000001.XSHE", sec)
            await h.save("000001.XSHE", df)

            head, tail = await cache.get_bars_range(sec, FrameType.DAY)
            self.assertEqual(datetime.date(2019, 1, 4), head)
            self.assertEqual(datetime.date(2019, 1, 4), tail)

            bars = await cache.get_bars(
                sec, datetime.date(2019, 1, 4), 1, FrameType.DAY
            )

            self.assertEqual(datetime.date(2019, 1, 4), bars[0]["frame"])
