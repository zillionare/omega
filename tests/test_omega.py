import asyncio
import logging
from omega.logging.receiver.redis import RedisLogReceiver
import unittest
import rlog
import shutil
import os

from tests import init_test_env

logger = logging.getLogger(__name__)

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

    async def test_redis_logging(self):
        # remove handlers set by config file, if there is.
        root = logging.getLogger()
        root.handlers.clear()

        channel = "omega"
        redis_logger = logging.getLogger("test_redis")
        fmt = '%(asctime)s %(levelname)-1.1s %(process)d %(name)s:%(funcName)s:%(lineno)s | %(message)s'

        handler = rlog.RedisHandler(
            channel=channel, 
            level=logging.DEBUG,
            host="localhost",
            port="6379",
            formatter=logging.Formatter(fmt)
            )
        redis_logger.addHandler(handler)

        _dir = "/tmp/omega/test_omega"
        shutil.rmtree(_dir, ignore_errors=True)
        receiver = RedisLogReceiver(
            dsn="redis://localhost:6379",
            channel_name=channel,
            filename = f"{_dir}/omega.log",
            max_bytes=20,
            backup_count=2)

        await receiver.start()
        for i in range(5):
            redis_logger.info("this is %sth test log", i)
        await asyncio.sleep(0.5)
        await receiver.stop()
        self.assertEqual(3, len(os.listdir(_dir)))
        with open(f"{_dir}/omega.log.2", "r", encoding="utf-8") as f:
            content = f.readlines()[0]
            msg = content.split("|")[1]
            self.assertEqual(" this is 2th test log\n", msg)

