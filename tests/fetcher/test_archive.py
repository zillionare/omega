import datetime
import logging
import unittest
from unittest import mock
from unittest.mock import call

import aiohttp
import cfg4py
import omicron
import pandas as pd
from omicron.core.types import FrameType
from omicron.dal import cache
from ruamel.yaml.error import YAMLError

from omega.fetcher import archive
from omega.fetcher.archive import ArchivedBarsHandler
from tests import init_test_env, start_archive_server, start_omega

logger = logging.getLogger(__name__)


class TestArchieveFetcher(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        init_test_env()
        self.cfg = cfg4py.get_instance()

        self.archive_server = await start_archive_server()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.shutdown()
        if self.archive_server:
            self.archive_server.kill()

    async def test_get_bars(self):
        sec = "000001.XSHE"
        await cache.security.delete("000001.XSHE:1d")

        responses = set()
        async for status, desc in archive.get_bars(
            self.cfg.omega.urls.archive, [201901], ["stock"]
        ):
            responses.add(f"{status} {desc}")
        self.assertSetEqual(
            set(
                [
                    "200 读取索引成功",
                    "200 成功导入2019年01月的stock数据",
                    "200 DONE",
                ]
            ),
            responses,
        )

        head, tail = await cache.get_bars_range(sec, FrameType.DAY)
        self.assertEqual(datetime.date(2019, 1, 4), head)
        self.assertEqual(datetime.date(2019, 1, 4), tail)

        bars = await cache.get_bars(sec, datetime.date(2019, 1, 4), 1, FrameType.DAY)

        self.assertEqual(datetime.date(2019, 1, 4), bars[0]["frame"])

    async def test_get_index(self):
        status, index = await archive.get_index(self.cfg.omega.urls.archive)
        self.assertEqual(200, status)
        self.assertListEqual([201901, 201902], index.get("stock"))

        func = "omega.fetcher.archive.get_file"
        for side_effect in [aiohttp.ServerTimeoutError(), YAMLError(), Exception()]:
            with mock.patch(func, side_effect=side_effect):
                status, index = await archive.get_index(self.cfg.omega.urls.archive)
                self.assertEqual(500, status)
                self.assertEqual(index, None)

    @mock.patch("builtins.print")
    async def test_main(self, mock_print):
        await archive._main([201901], ["stock"])
        mock_print.assert_has_calls(
            [
                mock.call(200, "读取索引成功"),
                mock.call(200, "成功导入2019年01月的stock数据"),
                mock.call(200, "DONE"),
            ]
        )

    @mock.patch("aiohttp.ClientSession.get")
    async def test_get_file_404(self, mock_get):
        mock_get.return_value.__aenter__.return_value.status = 404

        url, resp = await archive.get_file("http://mock/2019-11-stock.tgz")
        self.assertEqual(resp, "404 服务器上没有2019年11月的stock数据")

    @mock.patch("aiohttp.ClientSession.get")
    async def test_get_file_connection_error(self, mock_get):
        mock_get.side_effect = aiohttp.ServerTimeoutError()

        url, _ = await archive.get_file("http://mock/2019-11-stock.tgz")
        self.assertEqual("http://mock/2019-11-stock.tgz", url)
        self.assertRaises(aiohttp.ServerTimeoutError)

    async def test_archive_bars_handler_process(self):
        handler = ArchivedBarsHandler("http://mock/2019-01-stock.tgz")
        content = None
        with open("tests/data/2019-01-stock.tgz", "rb") as f:
            content = f.read(-1)
        # 0. normal case
        url, result = await handler.process(content)

        # 1. test when processing cause exceptions
        url, result = await handler.process(None)
        self.assertTrue(result.startswith("500 导入数据"))

        # 2. test when remove temp dir cause exceptions
        with mock.patch("shutil.rmtree", side_effect=Exception()):
            url, result = await handler.process(content)
            self.assertTrue(result.startswith("200 成功导入"))
