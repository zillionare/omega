import logging
import unittest

from tests import init_test_env


class TestOmega(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.cfg = init_test_env()

    def test_report_logging(self):
        """Omega使用rsyslog来consolidate多个进程的日志。"""
        validation = logging.getLogger("validation")
        validation.info("this is a test, should go to validation")

        quickscan = logging.getLogger("quickscan")
        quickscan.info("this is a test should go to quickscan")

        logging.info("this is default, should go to omega.log")
