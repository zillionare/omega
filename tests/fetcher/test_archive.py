import datetime
import logging
import unittest
from unittest import mock

import aiohttp
import cfg4py
import omicron
from omicron.core.types import FrameType
from omicron.dal import cache
from ruamel.yaml.error import YAMLError

from omega.fetcher import archive
from omega.fetcher.archive import ArchivedBarsHandler
from tests import init_test_env, start_archive_server

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
            self.cfg.omega.urls.archive, [202103], ["stock"]
        ):
            responses.add(f"{status} {desc}")
        self.assertSetEqual(
            set(
                [
                    "200 读取索引成功",
                    "200 成功导入2021年03月的stock数据",
                    "200 DONE",
                ]
            ),
            responses,
        )

        bars = await cache.get_bars(sec, datetime.date(2021, 3, 31), 1, FrameType.DAY)

        self.assertEqual(datetime.date(2021, 3, 31), bars[0]["frame"])

    async def test_get_index(self):
        status, index = await archive.get_index(self.cfg.omega.urls.archive)
        self.assertEqual(200, status)
        self.assertListEqual([202103, 202102, 202101], index.get("stock"))

        func = "omega.fetcher.archive.get_file"
        for side_effect in [aiohttp.ServerTimeoutError(), YAMLError(), Exception()]:
            with mock.patch(func, side_effect=side_effect):
                status, index = await archive.get_index(self.cfg.omega.urls.archive)
                self.assertEqual(500, status)
                self.assertEqual(index, None)

    @mock.patch("builtins.print")
    async def test_main(self, mock_print):
        # either head or tail is None
        await cache.security.hdel("000001.XSHE:1d", "head")
        await cache.security.hmset("000001.XSHE:1d", "tail", 2020)

        # arc_tail (202103311500) < head (2021 0401 1500)
        await cache.security.hmset(
            "000001.XSHE:30m", "head", 202104011500, "tail", 202104031500
        )

        # arc_head (20210104) > tail (20191231)
        await cache.security.hmset("600000.XSHG:1d", "head", 20191230, "tail", 20191231)

        # overlapped
        await cache.security.hmset(
            "600001.XSHG:30m", "head", 202101051400, "tail", 202103021130
        )

        await archive.clear_range()
        await archive._main([202103, 202101, 202102], ["stock"])
        # _main will close omicron
        await omicron.init()
        await archive.adjust_range()

        head, tail = await cache.security.hmget("000001.XSHE:1d", "head", "tail")
        print(head, tail)
        head, tail = await cache.security.hmget("000001.XSHE:30m", "head", "tail")
        print(head, tail)
        head, tail = await cache.security.hmget("600001.XSHG:1d", "head", "tail")
        print(head, tail)
        head, tail = await cache.security.hmget("600001.XSHE:30m", "head", "tail")
        print(head, tail)

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
        handler = ArchivedBarsHandler("http://mock/2021-03-stock.tgz")
        content = None
        with open("tests/data/2021-03-stock.tgz", "rb") as f:
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

    async def test_adjust_ranges(self):
        await archive.clear_range()

        frames = [20200101, 20200102, 20200103, 20200104]
        await cache.sys.lpush("archive.ranges.000001.XSHE:1d", *frames)
        # either head or tail is None
        await cache.security.hdel("000001.XSHE:1d", "head")
        await cache.security.hmset("000001.XSHE:1d", "tail", 20200104)

        frames = [202101011030, 202101011100, 202101011330, 202101011400]
        await cache.sys.lpush("archive.ranges.000001.XSHE:30m", *frames)

        frames = [20200101, 20200102, 20200103, 20200104]
        await cache.sys.lpush("archive.ranges.600001.XSHG:1d", *frames)

        frames = [202101011030, 202101011100, 202101011330, 202101011400]
        await cache.sys.lpush("archive.ranges.600001.XSHG:30m", *frames)

        # arc_tail (2021 0101 1400) < head (2021 0102 1100)
        await cache.security.hmset(
            "000001.XSHE:30m", "head", 202101021030, "tail", 202101021100
        )

        # arch_head (2020 0101) > tail (2019 1231)
        await cache.security.hmset("600001.XSHG:1d", "head", 20191230, "tail", 20191231)

        # others: overlapped
        await cache.security.hmset(
            "600001.XSHG:30m", "head", 202101011100, "tail", 202101011500
        )

        await archive.adjust_range(batch=2)

        head, tail = await cache.security.hmget("000001.XSHE:1d", "head", "tail")
        head, tail = int(head), int(tail)
        self.assertEqual(20200101, head)
        self.assertEqual(20200104, tail)

        head, tail = await cache.security.hmget("000001.XSHE:30m", "head", "tail")
        head, tail = int(head), int(tail)
        self.assertEqual(202101011030, head)
        self.assertEqual(202101011400, tail)

        head, tail = await cache.security.hmget("600001.XSHG:1d", "head", "tail")
        head, tail = int(head), int(tail)
        self.assertEqual(20200101, head)
        self.assertEqual(20200104, tail)

        head, tail = await cache.security.hmget("600001.XSHG:30m", "head", "tail")
        head, tail = int(head), int(tail)
        self.assertEqual(202101011030, head)
        self.assertEqual(202101011500, tail)
