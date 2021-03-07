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

from omega.fetcher import archive
from tests import init_test_env, start_archive_server, start_omega
from omega.fetcher.archive import ArchivedBarsHandler

logger = logging.getLogger(__name__)


class TestArchieveFetcher(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        init_test_env()
        self.cfg = cfg4py.get_instance()
        self.omega = await start_omega()
        self.archive = await start_archive_server()

        await omicron.init()

    async def asyncTearDown(self) -> None:
        if self.omega:
            self.omega.kill()

        if self.archive:
            self.archive.kill()

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
        response = await archive.get_archive_index(self.cfg.omega.urls.archive)
        self.assertListEqual([201901, 201902], response.get("stock"))

        func = 'omega.fetcher.archive.get_file'
        with mock.patch(func, side_effect=aiohttp.ClientConnectionError()):
            response = await archive.get_archive_index(self.cfg.omega.urls.archive)
            self.assertEqual(500, response.status)

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
        mock_get.return_value.__aenter__.return_value.raiseError.side_effect = (
            aiohttp.ClientConnectionError()
        )

        archive_log = logging.getLogger("omega.fetcher.archive")
        with mock.patch.object(archive_log, "info") as mock_info:
            url, resp = await archive.get_file("http://mock/2019-11-stock.tgz")
            self.assertEqual("http://mock/2019-11-stock.tgz", url)
            self.assertRaises(aiohttp.ClientConnectionError)
            mock_info.assert_has_calls(
                [
                    call("downloading file from %s", "http://mock/2019-11-stock.tgz"),
                    call(
                        "retry downloading file from %s",
                        "http://mock/2019-11-stock.tgz",
                    ),
                    call("downloading file from %s", "http://mock/2019-11-stock.tgz"),
                    call(
                        "retry downloading file from %s",
                        "http://mock/2019-11-stock.tgz",
                    ),
                    call("downloading file from %s", "http://mock/2019-11-stock.tgz"),
                    call(
                        "retry downloading file from %s",
                        "http://mock/2019-11-stock.tgz",
                    ),
                ]
            )
