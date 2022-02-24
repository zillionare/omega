# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2021-12-31 09:55
import io
import logging
from abc import ABC
from datetime import date, datetime
from typing import AnyStr, Dict, Union

import cfg4py
import numpy as np
from coretypes import FrameType, SecurityType
from minio import Minio, error
from omicron.models.timeframe import TimeFrame
from retrying import retry

from omega.config.schema import Config

cfg: Config = cfg4py.get_instance()

logger = logging.getLogger(__name__)


# 用来和DFS存储系统进行交互的封装
class AbstractStorage(ABC):
    """该类是用来和minio这种dfs存储系统进行交互的抽象类，如果需要对接不同的dfs，需要继承该类，并实现对应的方法
    在yaml中的配置如下
    dfs:
      engine: minio
      minio:
        host: ${MINIO_HOST}
        port: ${MINIO_PORT}
        access: ${MINIO_ACCESS}
        secret: ${MINIO_SECRET}
        secure: false
        bucket: zillionare
    """

    client = None

    async def write(
        self,
        filename: str,
        bar: bytes,
    ):  # pragma: no cover
        """
        将bar写入dfs中 按照 /日期/
        Args:
            filename: 要写入的文件名
            bar: K线数据字典
        Returns:

        """

    async def read(self, filename: str) -> np.array:  # pragma: no cover
        """
        Args:
            filename: 文件名
        Returns: np.array:

        """

    async def delete_bucket(self):  # pragma: no cover
        """删除bucket"""

    async def delete(self, filename: str):
        """删除一个文件"""


class TempStorage:
    async def write(self, *args, **kwargs):  # pragma: no cover
        pass


class Storage:
    __instance = None

    def __new__(cls, *args, **kwargs) -> Union[AbstractStorage, None]:
        if cls.__instance is not None:
            return cls.__instance

        elif cfg.dfs.engine == "minio":
            cls.__instance = MinioStorage(*args, **kwargs)
        else:
            return None
        return cls.__instance

    @classmethod
    def reset(cls):
        cls.__instance = None


class MinioStorage(AbstractStorage):
    def __init__(self, bucket=None, readonly=False):
        """初始化minio连接，检查bucket 是否存在"""
        self.client = Minio(
            endpoint=f"{cfg.dfs.minio.host}:{cfg.dfs.minio.port}",
            access_key=cfg.dfs.minio.access,
            secret_key=cfg.dfs.minio.secret,
            secure=cfg.dfs.minio.secure,
        )
        self.__readonly = readonly
        if bucket is None:
            self.bucket = cfg.dfs.minio.bucket
        else:
            self.bucket = bucket
        if not self.__readonly:
            self.create_bucket()

    def create_bucket(self):
        # 调用make_bucket来创建一个存储桶。
        exists = self.client.bucket_exists(self.bucket)
        if not exists:
            self.client.make_bucket(self.bucket)
        else:  # pragma: no cover
            logger.info(f"bucket {self.bucket}已存在,跳过创建")

    async def delete_bucket(self):
        """删除bucket"""
        self.client.remove_bucket(self.bucket)

    @retry(stop_max_attempt_number=5)
    async def write(
        self,
        filename: str,
        bar: bytes,
    ):
        # filename = self.get_filename(prefix, dt, frame_type)
        data = io.BytesIO(bar)
        ret = self.client.put_object(self.bucket, filename, data, length=len(bar))
        logger.info(f"Written {filename} to minio")
        return ret

    async def read(self, filename: str) -> np.array:
        response = self.client.get_object(self.bucket, filename)
        return response.read()

    async def delete(self, filename: str):
        self.client.remove_object(self.bucket, filename)
        return True
