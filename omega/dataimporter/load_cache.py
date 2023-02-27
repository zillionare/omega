import datetime
import glob
import logging
import os
import pickle

import aioredis
import arrow
import cfg4py
import numpy as np
from coretypes import FrameType, bars_dtype
from omicron.models.timeframe import TimeFrame

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


def read_timestamp(file: str):
    try:
        with open(file, "r") as f:
            line = f.readline().strip()
            _t = arrow.get(line, "YYYY-MM-DD").naive.date()
            logger.info("timestamp in file: %s", _t)
            return _t
    except Exception as e:
        logger.exception(e)
        return None


async def load_calendar(redis, base_dir: str):
    # import static file
    for ft in [
        FrameType.DAY,
        FrameType.WEEK,
        FrameType.MONTH,
        FrameType.QUARTER,
        FrameType.YEAR,
    ]:
        target_file = f"{base_dir}/redis_calendar_{ft.value}.pik"
        with open(target_file, "rb") as f:
            key = f"calendar:{ft.value}"
            await redis.delete(key)

            trade_days = pickle.load(f)
            pl = redis.pipeline()
            pl.rpush(key, *trade_days)
            await pl.execute()

        logger.info("calendar:%s loaded: %s", ft.value, target_file)


async def load_security_list(redis, base_dir: str, ts: datetime.date):
    # files = glob.glob("")
    target_file = f"{base_dir}/redis_seclist.pik"
    with open(target_file, "rb") as f:
        key = "security:latest_date"
        await redis.delete(key)
        await redis.set(key, ts.strftime("%Y-%m-%d"))

        key = "security:all"
        await redis.delete(key)
        sec_list = pickle.load(f)
        pl = redis.pipeline()
        pl.rpush(key, *sec_list)
        await pl.execute()

    logger.info("security:all loaded: %s", target_file)
