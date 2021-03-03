import logging
import pickle
import unittest

import aiohttp
import arrow
import cfg4py
import omicron

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

    async def server_post(
        self, cat: str, item: str, params: dict = None, is_pickled=True
    ):
        url = f"{cfg.omega.urls.quotes_server}/{cat}/{item}"
        async with aiohttp.ClientSession() as client:
            async with client.post(url, json=params) as resp:
                if is_pickled:
                    content = await resp.content.read(-1)
                    return pickle.loads(content)
                else:
                    return await resp.text()

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


if __name__ == "__main__":
    unittest.main()
