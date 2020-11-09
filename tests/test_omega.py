import asyncio
import logging
import os
import pickle
import signal
import subprocess
import sys
import time
import unittest

import aiohttp
import cfg4py

from omega.cli import update_config
from omega.core import get_config_dir

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class TestOmega(unittest.IsolatedAsyncioTestCase):
    """app lifecycle related tests

    Args:
        unittest ([type]): [description]
    """

    def setUp(self) -> None:
        os.environ[cfg4py.envar] = "DEV"

        self.config_dir = get_config_dir()

        cfg4py.init(self.config_dir)

    def start_server(self):
        logger.info("starting omega server")
        account = os.environ["jq_account"]
        password = os.environ["jq_password"]
        self.server_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "omega.app",
                "start",
                "jqadaptor",
                f"--account={account}",
                f"--password={password}"
            ],
            env=os.environ
        )

        # ensure omega server is started before we executed other tests
        time.sleep(5)

    def stop_server(self) -> None:
        os.kill(self.server_process.pid, signal.SIGTERM)

    def test_update_config(self):
        update_config("redis.dsn", "redis://127.0.0.1:6379")

        config = [
            {
                "name": "jqdatasdk",
                "module": "jqadaptor",
                "parameters": {"account": "account", "password": "password"},
            }
        ]
        update_config("quotes_fetchers", config)

    def test_config_sync(self):
        pass

    async def test_get_security_list(self):
        self.start_server()
        try:
            # wait server start
            await asyncio.sleep(10)
            cfg = cfg4py.get_instance()
            async with aiohttp.ClientSession() as client:
                async with client.get(
                    f"{cfg.omega.urls.quotes_server}/quotes/security_list"
                ) as resp:
                    content = await resp.content.read(-1)
                    secs = pickle.loads(content)
                    logger.info("get_security_list returns %s records", len(secs))
                    self.assertEqual("平安银行", secs[0][1])
        finally:
            self.stop_server()

    def test_report_logging(self):
        validation = logging.getLogger("validation")
        validation.info("this is a test, should go to validation")

        quickscan = logging.getLogger("quickscan")
        quickscan.info("this is a test should go to quickscan")

        logging.info("this is default, should go to omega.log")


if __name__ == "__main__":
    unittest.main()
