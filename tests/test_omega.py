import asyncio
import logging
import os
import pickle
import signal
import subprocess
import sys
import unittest

import aiohttp
import cfg4py
from omicron.core.lang import async_run

from omega.cli import update_config
from omega.core import get_config_dir

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        os.environ[cfg4py.envar] = 'DEV'

        self.config_dir = get_config_dir()

        cfg4py.init(self.config_dir)

    def start_server(self):
        account = os.environ.get("jqsdk_account")
        password = os.environ.get("jqsdk_password")
        server_url = cfg.omega.server.url
        try:
            port = int(server_url.split(":")[-1])
        except ValueError:
            port = 80
        code = f"from omega.app import start; start(config_dir='{self.config_dir}', " \
               f"port={port}, impl='jqadaptor', account='{account}', password=" \
               f"'{password}')"
        self.server_process = subprocess.Popen([sys.executable, '-c', code],
                                               env=os.environ)

    def stop_server(self) -> None:
        os.kill(self.server_process.pid, signal.SIGTERM)

    def test_update_config(self):
        conf = {
            "host": '127.0.0.1',
            'port': 6379
        }

        update_config("redis.dsn", "redis://127.0.0.1:6379")

        config = [{
            'name':       'jqdatasdk',
            'module':     'jqadaptor',
            'parameters': {
                'account':  'account',
                'password': 'password'
            }
        }]
        update_config('quotes_fetchers', config)

    def test_config_sync(self):
        pass

    @async_run
    async def test_get_security_list(self):
        self.start_server()
        try:
            # wait server start
            await asyncio.sleep(10)
            cfg = cfg4py.get_instance()
            async with aiohttp.ClientSession() as client:
                async with client.get(
                        f'{cfg.omega.server.url}/quotes/security_list') as resp:
                    content = await resp.content.read(-1)
                    secs = pickle.loads(content)
                    logger.info("get_security_list returns %s records", len(secs))
                    self.assertEqual('平安银行', secs[0][1])
        finally:
            self.stop_server()


if __name__ == '__main__':
    unittest.main()
