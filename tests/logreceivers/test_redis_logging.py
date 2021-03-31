import asyncio
import logging
import os
import shutil
import unittest

import rlog

from omega.logreceivers.redis import RedisLogReceiver
from tests import init_test_env


class TestRedisLogging(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.cfg = init_test_env()

    async def test_redis_logging(self):
        # remove handlers set by config file, if there is.
        root = logging.getLogger()
        root.handlers.clear()

        channel = "test_redis_logging"
        redis_logger = logging.getLogger("test_redis")
        fmt = "%(asctime)s %(levelname)-1.1s %(process)d %(name)s:%(funcName)s:%(lineno)s | %(message)s"

        handler = rlog.RedisHandler(
            channel=channel,
            level=logging.DEBUG,
            host="localhost",
            port="6379",
            formatter=logging.Formatter(fmt),
        )
        redis_logger.addHandler(handler)

        _dir = "/tmp/omega/test_redis_logging"
        shutil.rmtree(_dir, ignore_errors=True)
        receiver = RedisLogReceiver(
            dsn="redis://localhost:6379",
            channel_name=channel,
            filename=f"{_dir}/omega.log",
            max_bytes=20,
            backup_count=2,
        )

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
