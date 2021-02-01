import logging
import pickle
import unittest

import aiohttp
import arrow
import cfg4py
import omicron

from omega import __version__
from tests import init_test_env, start_archive_server, start_omega

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestWebInterfaces(unittest.IsolatedAsyncioTestCase):
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

    async def server_get(
        self, cat: str, item: str, params: dict = None, is_pickled=True
    ):
        url = f"{cfg.omega.urls.quotes_server}/{cat}/{item}"
        async with aiohttp.ClientSession() as client:
            async with client.get(url, json=params) as resp:
                if is_pickled:
                    content = await resp.content.read(-1)
                    return pickle.loads(content)
                else:
                    return await resp.text()

    async def test_sever_version(self):
        ver = await self.server_get("sys", "version", is_pickled=False)
        self.assertEqual(__version__, ver)

    async def test_get_security_list(self):
        secs = await self.server_get("quotes", "security_list")
        self.assertEqual("平安银行", secs[0][1])

    async def test_get_valuation(self):
        vals = await self.server_get(
            "quotes",
            "valuation",
            {
                "secs": "000001.XSHE",
                "fields": ["code", "frame"],
                "date": "2020-11-20",
                "n": 1,
            },
        )
        self.assertEqual(arrow.get("2020-11-20").date(), vals[0]["frame"])

    async def test_get_bars(self):
        bars = await self.server_get(
            "quotes",
            "bars",
            {
                "sec": "000001.XSHE",
                "end": "2020-11-20",
                "n_bars": 1,
                "frame_type": "1d",
                "include_unclosed": True,
            },
        )
        self.assertEqual(arrow.get("2020-11-20").date(), bars[0]["frame"])

    async def test_get_bars_batch(self):
        bars = await self.server_get(
            "quotes",
            "bars_batch",
            {
                "secs": ["000001.XSHE", "600001.XSHG"],
                "end": "2020-11-20",
                "n_bars": 1,
                "frame_type": "1d",
                "include_unclosed": True,
            },
        )
        self.assertEqual(
            arrow.get("2020-11-20").date(), bars["000001.XSHE"]["frame"][0]
        )

    async def test_archive(self):
        url = "http://localhost:3181/ws/quotes/archive"

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url) as ws:
                await ws.send_json({"request": "index"})
                resp = await ws.receive_json()
                self.assertSetEqual(set(["index", "stock"]), set(resp.keys()))
                self.assertSetEqual(set(["201901", "201902"]), set(resp["stock"]))

                await ws.send_json(
                    {
                        "request": "bars",
                        "params": {"months": ["201901", "201902"], "cats": ["stock"]},
                    }
                )

                results = set([await ws.receive_str() for i in range(4)])
                self.assertSetEqual(
                    set(
                        [
                            "200 读取索引成功",
                            "404 服务器上没有2019年02月的stock数据",
                            "200 成功导入2019年01月的stock数据",
                            "200 DONE",
                        ]
                    ),
                    results,
                )

                await ws.close()


if __name__ == "__main__":
    unittest.main()
