import asyncio
import logging
import os
import pickle
import signal
import subprocess
import sys
import unittest
from unittest import mock

import aiohttp
import cfg4py
from omicron.core.lang import async_run

from omega.cli import update_config, config_sync

logger = logging.getLogger(__name__)


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        os.environ[cfg4py.envar] = 'TEST'
        self.config_dir = os.path.normpath(os.path.join(os.path.dirname(__file__),
                                                        "../omega/config"))

        cfg4py.init(self.config_dir)

    def start_server(self):
        account = os.environ.get("jqsdk_account")
        password = os.environ.get("jqsdk_password")
        code = f"from omega.app import start; start(config_dir='{self.config_dir}', " \
               f"port=3180, impl='jqadaptor', account='{account}', password='{password}')"
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
        inputs = [
            '1000', '900', '800', '700', '600', '500', '400', '300', '200', '15:06'
        ]

        expected_frames = {
            '1d':  1000,
            '1w':  900,
            '1M':  800,
            '1y':  700,
            '1m':  600,
            '5m':  500,
            '15m': 400,
            '30m': 300,
            '60m': 200
        }
        with mock.patch('builtins.input', side_effect=inputs):
            frames, sync_time = config_sync()
            self.assertDictEqual(expected_frames, frames)
            self.assertEqual('15:06', sync_time)

        inputs = [
            '1000', 'C', '', '700', '600', '500', '400', '300', '200', '25:01', '15:00'
        ]

        expected_frames = {
            '1d':  1000,
            '1M':  1000,
            '1y':  700,
            '1m':  600,
            '5m':  500,
            '15m': 400,
            '30m': 300,
            '60m': 200
        }

        with mock.patch('builtins.input', side_effect=inputs):
            frames, sync_time = config_sync()
            self.assertDictEqual(expected_frames, frames)
            self.assertEqual('15:00', sync_time)

    @async_run
    async def test_get_security_list(self):
        # wait server start
        self.start_server()
        try:
            await asyncio.sleep(5)
            async with aiohttp.ClientSession() as client:
                async with client.get(
                        'http://localhost:3180/quotes/security_list') as resp:
                    content = await resp.content.read(-1)
                    secs = pickle.loads(content)
                    logger.info("get_security_list returns %s records", len(secs))
                    self.assertEqual('平安银行', secs[0][1])
        finally:
            self.stop_server()


if __name__ == '__main__':
    unittest.main()
