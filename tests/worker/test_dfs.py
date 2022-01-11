# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-06 15:52
import logging
import unittest
import datetime

import cfg4py

from omega.worker.dfs import MinioStorage, Storage
from omicron.models.calendar import Calendar as cal, FrameType
from omega.config.schema import Config
from tests import init_test_env

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class TestDFS(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

    async def test_minio(self):
        minio = Storage()
        # 测试启动之后桶一定存在了
        self.assertTrue(minio.client.bucket_exists(cfg.dfs.minio.bucket))
        await minio.write(
            "0000.XSH", {"high": 100}, datetime.datetime.now(), FrameType.MIN1
        )
