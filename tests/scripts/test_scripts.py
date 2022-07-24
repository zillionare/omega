import datetime
import os
import pickle
import unittest
from unittest import mock

import arrow
import cfg4py
import numpy as np
import omicron
import pandas as pd
from coretypes import FrameType
from omicron import cache

from omega.scripts import close_frame, load_lua_script, update_unclosed_bar
from tests import init_test_env

cfg = cfg4py.get_instance()


async def prepare_test_data():
    """准备测试数据"""
    min_bars = [
        (202207180931, 7.0, 7.20, 6.90, 7.02, 1e6, 1e6 * 7.0, 1.0),
        (202207180932, 7.1, 7.21, 6.89, 7.03, 1e6, 1e6 * 7.1, 1.0),
        (202207180933, 7.2, 7.22, 6.88, 7.04, 1e6, 1e6 * 7.2, 1.0),
        (202207180934, 7.3, 7.23, 6.87, 7.05, 1e6, 1e6 * 7.3, 1.0),
        (202207180935, 7.4, 7.24, 6.86, 7.06, 1e6, 1e6 * 7.4, 1.0),
        (202207180936, 7.5, 7.25, 6.85, 7.07, 1e6, 1e6 * 7.5, 1.0),
    ]

    codes = ["000001.XSHE", "000002.XSHE", "600003.XSHG"]
    for code in codes:
        for bar in min_bars:
            await cache.security.hset(
                f"bars:1m:{code}",
                bar[0],
                f"{bar[0]},{bar[1]:.2f},{bar[2]:.2f},{bar[3]:.2f},{bar[4]:.2f},{bar[5]:.2f},{bar[6]:.2f},{bar[7]:.2f}",
            )

    # unclosed = {
    #     "000001.XSHE": f"202207180935,7,7.22,6.88,7.04,{1e6},{1e6*7.2},1.0",
    #     "000002.XSHE": f"202207180935,7,7.22,6.88,7.04,{1e6},{1e6*7.2},1.0",
    #     "000003.XSHG": f"202207180935,7,7.22,6.88,7.04,{1e6},{1e6*7.2},1.0",
    # }

    # await cache.security.hmset_dict("bars:5m:unclosed", **unclosed)


class ScriptTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

        await cache.sys.execute("FUNCTION", "FLUSH")
        await load_lua_script()

    async def asyncTearDown(self) -> None:
        return await omicron.close()

    async def test_load_script(self):
        r = await cache.sys.execute("FUNCTION", "LIST")
        funcs = r[0][5][0][:2]
        funcs.extend(r[0][5][1][:2])
        self.assertIn("close_frame", funcs)
        self.assertIn("update_unclosed", funcs)

    async def test_close_frame(self):
        await prepare_test_data()
        await cache.security.delete("bars:5m:unclosed")
        await cache.security.delete("bars:5m:000001.XSHE")

        for i in range(5):
            await cache.security.execute(
                "fcall", "update_unclosed", 0, "5m", 202207180931 + i
            )

        await close_frame(FrameType.MIN5, datetime.datetime(2022, 7, 18, 9, 35))
        unclosed = await cache.security.hgetall("bars:5m:unclosed")
        closed = await cache.security.hgetall("bars:5m:000001.XSHE")
        exp = f"202207180935,7,7.24,6.86,7.06,{5e6:.0f},{36e6:.0f},1"

        self.assertEqual(exp, closed["202207180935"])
        self.assertTrue(len(unclosed) == 0)

        # raise exception
        with mock.patch.object(cache.security, "execute", side_effect=Exception):
            await close_frame(FrameType.MIN5, datetime.datetime(2022, 7, 18, 9, 35))

    async def test_update_unclosed(self):
        await prepare_test_data()
        await cache.security.delete("bars:5m:unclosed")

        await update_unclosed_bar(FrameType.MIN5, datetime.datetime(2022, 7, 18, 9, 31))
        unclosed = await cache.security.hgetall("bars:5m:unclosed")
        self.assertEqual(
            f"202207180931,7,7.2,6.9,7.02,{1e6:.0f},{1e6*7:.0f},1",
            unclosed["000001.XSHE"],
        )

        await cache.security.execute("fcall", "update_unclosed", 0, "5m", 202207180932)

        await cache.security.execute("fcall", "update_unclosed", 0, "5m", 202207180933)

        # now unclosed bar should have
        unclosed = await cache.security.hgetall("bars:5m:unclosed")
        closed = await cache.security.hgetall("bars:5m:000001.XSHE")
        exp = f"202207180933,7,7.22,6.88,7.04,{3e6:.0f},{1e6*21.3:.0f},1"
        self.assertEqual(exp, unclosed["000001.XSHE"])

        # error handling
        with mock.patch.object(cache.security, "execute", side_effect=Exception):
            await update_unclosed_bar(
                FrameType.MIN5, datetime.datetime(2022, 7, 18, 9, 31)
            )
