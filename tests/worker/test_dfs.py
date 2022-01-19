# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-06 15:52
import datetime
import logging
import unittest

import cfg4py
from zillionare_core_types.core.types import FrameType

from omega.worker.dfs import Storage, MinioStorage
from omega.config.schema import Config
from tests import init_test_env

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class TestDFS(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

    # @mock.patch("omega.worker.dfs.Minio")
    async def test_minio(self, *args):
        cfg.dfs.engine = "minio"
        cfg.dfs.minio.bucket = "testbucket"
        minio = Storage()
        # 测试启动之后桶一定存在了
        self.assertTrue(minio.client.bucket_exists(cfg.dfs.minio.bucket))
        # 读写测试
        content = b"123"
        prefix = "stock"
        dt = datetime.datetime.now()
        await minio.write(content, prefix, dt, FrameType.MIN1)
        self.assertEqual(await minio.read(prefix, dt, FrameType.MIN1), content)

        Storage.reset()
        minio = Storage(bucket=cfg.dfs.minio.bucket)
        await minio.delete(prefix, dt, FrameType.MIN1)
        await minio.delete_bucket()
        minio = Storage()
        self.assertIsInstance(minio, MinioStorage)

        Storage.reset()
        cfg.dfs.engine = None
        minio = Storage()
        self.assertIsNone(minio)
