import logging
import os
import signal
import unittest

import psutil

from tests import init_test_env
from tests import is_local_omega_alive
from tests import start_omega


class TestOmega(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.cfg = init_test_env()

    async def test_start_omega(self):
        if await (is_local_omega_alive()):
            for p in psutil.process_iter():
                cmd = " ".join(p.cmdline())
                if "omega.app" in cmd:
                    os.kill(p.pid, signal.SIGTERM)

        try:
            await start_omega()
            self.assertTrue(True, "omega is alive")
        except EnvironmentError:
            self.assertTrue(False, "omega failed to start")

    def test_report_logging(self):
        """Omega使用rsyslog来consolidate多个进程的日志。"""
        validation = logging.getLogger("validation")
        validation.info("this is a test, should go to validation")

        quickscan = logging.getLogger("quickscan")
        quickscan.info("this is a test should go to quickscan")

        logging.info("this is default, should go to omega.log")
