import logging
from tests import init_test_env
from omicron.core.types import FrameType

from omega.jobs.main import init, start_logging
import unittest
import asyncio
import logging
import os
import shutil
import unittest
from pyemit import emit


import cfg4py
import rlog

from omega.config.schema import Config

cfg: Config = cfg4py.get_instance()

class TestJobsMain(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        init_test_env()
        await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)

    def setUp(self) -> None:
        return super().setUp()

    async def test_start_logging(self):
        # remove handlers set by config file, if there is.
        root = logging.getLogger()
        root.handlers.clear()

        fmt = "%(asctime)s %(levelname)-1.1s %(process)d %(name)s:%(funcName)s:%(lineno)s | %(message)s"
        channel = "test_start_logging"
        redis_logger = logging.getLogger("test_redis")
        handler = rlog.RedisHandler(
            channel=channel,
            level=logging.DEBUG,
            host="localhost",
            port="6379",
            formatter=logging.Formatter(fmt),
        )

        redis_logger.addHandler(handler)

        _dir = "/tmp/omega/test_jobs"
        shutil.rmtree(_dir, ignore_errors=True)
        cfg4py.update_config(
            {
                "logreceiver": {
                    "klass": "omega.logging.receiver.redis.RedisLogReceiver",
                    "dsn": "redis://localhost:6379",
                    "channel": channel,
                    "filename": "/tmp/omega/test_jobs/omega.log",
                    "backup_count": 2,
                    "max_bytes": "0.08K",
                }
            }
        )

        await start_logging()
        for i in range(5):
            redis_logger.info("this is %sth test log", i)

        await asyncio.sleep(0.5)
        self.assertEqual(3, len(os.listdir(_dir)))
        with open(f"{_dir}/omega.log.2", "r", encoding="utf-8") as f:
            content = f.readlines()[0]
            msg = content.split("|")[1]
            self.assertEqual(" this is 2th test log\n", msg)

    async def test_init(self):
        await init(None, None)

    def test_start_sync(self):
        from omega.jobs.main import app

        sync_params = {
            "include": "000001.XSHE",
            "frame": FrameType.MIN60,
            "start": "2020-04-30",
            "stop": "2020-05-07",
        }
        request, response = app.test_client.get('/jobs/sync_bars', params=sync_params)
        self.assertEqual(200, response.status)
