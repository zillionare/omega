# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-06 15:52
import asyncio
import datetime
import logging
import unittest

import cfg4py
from coretypes import FrameType, SecurityType

from omega.config.schema import Config
from omega.master.dfs import MinioStorage, Storage
from omega.master.jobs import get_bars_filename
from tests import init_test_env

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class TestDFS(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

    async def test_minio(self, *args):
        Storage.reset()
        cfg.dfs.engine = "minio"
        cfg.dfs.minio.bucket = "testbucket"
        minio = Storage()
        await asyncio.sleep(1)
        # 测试启动之后桶一定存在了
        self.assertTrue(minio.client.bucket_exists(cfg.dfs.minio.bucket))
        # 读写测试
        content = b"123"
        dt = datetime.datetime.now()
        filename = get_bars_filename(SecurityType.STOCK, dt, FrameType.MIN1)
        await minio.write(filename, content)
        await asyncio.sleep(1)
        self.assertEqual(await minio.read(filename), content)

        Storage.reset()
        minio = Storage(bucket=cfg.dfs.minio.bucket)
        await minio.delete(filename)
        await minio.delete_bucket()
        minio = Storage()
        self.assertIsInstance(minio, MinioStorage)

        Storage.reset()
        cfg.dfs.engine = None
        minio = Storage()
        self.assertIsNone(minio)
