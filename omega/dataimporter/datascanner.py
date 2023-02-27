import datetime
import logging
import os
import pickle
from os import path

import aioredis
import cfg4py
import numpy as np
import omicron
from coretypes import FrameType, bars_dtype
from omicron.models.timeframe import TimeFrame

from omega.dataimporter.load_cache import (
    load_calendar,
    load_security_list,
    read_timestamp,
)

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


async def data_importer():
    """重建数据的步骤
    1. 判断redis中是否有日历和证券列表，如果没有，从本地文件读取所有的历史数据，并导入，并恢复redis中的各项时间指针
    2. 判断redis中是否有时间指针，如果没有，重新创建全部数据
    3. 重建数据时，对分钟线的时间指针强行设定为最新时刻
    4. 没有配置聚宽账号时，跳过所有数据的同步动作
    5. 时间指针有head选项的，统一指向2005.1.4，tail选项指向历史数据的最后时间，比如2023.2.10
    """
    # check redis
    redis = aioredis.from_url(
        cfg.redis.dsn, encoding="utf-8", decode_responses=True, db=1
    )

    # 检查calendar:1d的长度，正常大于4000
    count = await redis.llen("calendar:1d")
    if count and count > 4800:
        logger.info("calendar info found in redis, skip importing")
        return 0

    logger.info("begin to import local data...")
    base_folder = cfg.omega.local_data
    file = path.normpath(path.join(base_folder, "timestamp.txt"))

    # 读取文件中的时间戳，比如2023.2.10
    latest_ts = read_timestamp(file)
    if not latest_ts:
        logger.error("no timestamp found in local folder, exit...")
        return -1

    # 加载redis的核心数据
    try:
        logger.info("loading calendar...")
        await load_calendar(redis, base_folder)

        logger.info("loading security list...")
        await load_security_list(redis, base_folder, latest_ts)
    finally:
        if redis:
            await redis.close()

    # load omicron
    logger.info("loading omicron...")
    await omicron.init()

    logger.info("loading influx records...")
    # begin to import influx records

    logger.info("loading board info...")
    # import board zarr files

    return True
