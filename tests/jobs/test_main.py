import asyncio
import logging
import os
import shutil
import unittest
from unittest import mock

import aiohttp
import cfg4py
import omicron
import rlog
from omicron import cache
from omicron.core.types import FrameType

from omega.config.schema import Config
from omega.jobs.__main__ import init, start_logging
from tests import find_free_port, init_test_env, start_job_server

cfg: Config = cfg4py.get_instance()


class TestJobsMain(unittest.IsolatedAsyncioTestCase):
    async def test_start_logging(self):
        init_test_env()
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
        init_test_env()
        await omicron.init()
        await cache.sys.delete("jobs.bars_sync.stop")
        try:
            cfg.omega.sync.bars = [
                {
                    "frame": "1d",
                    "start": "2020-01-02",
                    "stop": "2020-01-03",
                    "delay": 3,
                    "cat": [],
                    "include": "000001.XSHE",
                    "exclude": "000001.XSHG",
                }
            ]
            # disable init, just use cfg here
            with mock.patch("cfg4py.init"):
                await init(None, None)
                # won't trigger sync this time
                await init(None, None)
        finally:
            # cfg.omega.sync.bars = origin
            pass

    async def test_start_sync(self):
        init_test_env()
        port = find_free_port()
        url = f"http://localhost:{port}/jobs/sync_bars"
        job_server = None
        try:
            job_server = await start_job_server(port)
            sync_params = {
                "include": "000001.XSHE",
                "frame": FrameType.MIN60.value,
                "start": "2020-04-30",
                "stop": "2020-05-07",
            }

            async with aiohttp.ClientSession() as client:
                async with client.get(url, json=sync_params) as resp:
                    self.assertEqual(200, resp.status)
                    result = await resp.text()
                    print(result)
        finally:
            if job_server:
                job_server.kill()
