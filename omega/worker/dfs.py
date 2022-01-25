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
from coretypes import FrameType
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

    @staticmethod
    def get_filename(
        prefix: str,
        dt: Union[datetime, date, AnyStr],
        frame_type: Union[FrameType, AnyStr],
    ) -> AnyStr:  # pragma: no cover
        """拼接文件名"""
        filename = []
        if isinstance(prefix, str) and prefix in ("stock", "index"):
            if prefix != "":
                filename.append(prefix)
        else:
            raise TypeError("prefix must be type str and in ('stock 'index')")

        if isinstance(frame_type, FrameType):
            filename.append(frame_type.value)
        elif isinstance(frame_type, str):
            filename.append(frame_type)
        else:
            raise TypeError("prefix must be type FrameType, str")
        if isinstance(dt, str):
            filename.append(TimeFrame.int2date(dt))
        elif isinstance(dt, datetime) or isinstance(dt, date):
            filename.append(str(TimeFrame.date2int(dt)))
        else:
            raise TypeError("dt must be type datetime, date, str")

        return "/".join(filename)

    async def write(
        self,
        bar: bytes,
        prefix,
        dt: Union[datetime, date, AnyStr],
        frame_type: Union[FrameType, AnyStr],
    ):  # pragma: no cover
        """
        将bar写入dfs中 按照 /日期/
        Args:
            prefix: 股票或基金的名称
            bar: K线数据字典
            dt: 日期
            frame_type: K线类型
        Returns:

        """

    async def read(
        self,
        prefix,
        dt: Union[datetime, date, AnyStr],
        frame_type: Union[FrameType, AnyStr],
    ) -> np.array:  # pragma: no cover
        """
        Args:
            prefix:  股票或基金的名称
            dt:  日期
            frame_type: K线类型

        Returns: np.array:

        """

    async def delete_bucket(self):  # pragma: no cover
        """删除bucket"""

    async def delete(
        self,
        prefix,
        dt: Union[datetime, date, AnyStr],
        frame_type: Union[FrameType, AnyStr],
    ):
        """删除一个文件"""


class TempStorage:
    async def write(self, *args, **kwargs):  # pragma: no cover
        pass


class Storage:
    __instance = None

    def __new__(cls, *args, **kwargs):
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
        bar: bytes,
        prefix,
        dt: Union[datetime, date, AnyStr],
        frame_type: Union[FrameType, AnyStr],
    ):
        filename = self.get_filename(prefix, dt, frame_type)
        data = io.BytesIO(bar)
        ret = self.client.put_object(self.bucket, filename, data, length=len(bar))
        logger.info(f"Written {filename} to minio")
        return ret

    async def read(
        self,
        prefix: AnyStr,
        dt: Union[datetime, date, AnyStr],
        frame_type: Union[FrameType, AnyStr],
    ) -> np.array:
        filename = self.get_filename(prefix, dt, frame_type)
        response = self.client.get_object(self.bucket, filename)
        return response.read()

    async def delete(
        self,
        prefix: AnyStr,
        dt: Union[datetime, date, AnyStr],
        frame_type: Union[FrameType, AnyStr],
    ):
        filename = self.get_filename(prefix, dt, frame_type)
        self.client.remove_object(self.bucket, filename)
        return True
