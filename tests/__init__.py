import datetime
import logging
import os
import pickle

import aioredis
import cfg4py
import numpy as np
from coretypes import FrameType, bars_dtype
from numpy.testing import assert_array_almost_equal, assert_array_equal
from omicron.models.timeframe import TimeFrame

from omega.config import get_config_dir

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


async def clear_cache(dsn):
    redis = aioredis.from_url(dsn)
    await redis.flushall()
    await redis.close()


async def set_security_data(redis):
    # set example securities
    stocks = [
        ("000001.XSHE", "平安银行", "PAYH", "1991-04-03", "2200-01-01", "stock"),
        ("000001.XSHG", "上证指数", "SZZS", "1991-07-15", "2200-01-01", "index"),
        ("300001.XSHE", "特锐德", "TRD", "2009-10-30", "2200-01-01", "stock"),
    ]

    pl = redis.pipeline()
    key = "security:all"
    await redis.delete(key)
    for s in stocks:
        pl.rpush(key, ",".join(s))
    await pl.execute()


async def set_calendar_data(redis):
    # set calendar
    module_dir = os.path.dirname(__file__)
    _dir = os.path.normpath(os.path.join(module_dir, "data"))

    target_file = f"{_dir}/calendar_20221205.pik"
    with open(target_file, "rb") as f:
        trade_days = pickle.load(f)

        for ft in [FrameType.WEEK, FrameType.MONTH, FrameType.QUARTER, FrameType.YEAR]:
            days = TimeFrame.resample_frames(trade_days, ft)
            frames = [TimeFrame.date2int(x) for x in days]

            key = f"calendar:{ft.value}"
            pl = redis.pipeline()
            pl.delete(key)
            pl.rpush(key, *frames)
            await pl.execute()

        frames = [TimeFrame.date2int(x) for x in trade_days]
        key = f"calendar:{FrameType.DAY.value}"
        pl = redis.pipeline()
        pl.delete(key)
        pl.rpush(key, *frames)
        await pl.execute()


async def init_test_env():
    import logging

    logging.captureWarnings(True)

    os.environ[cfg4py.envar] = "DEV"

    cfg4py.init(get_config_dir(), False)

    handler = logging.StreamHandler()
    fmt = "%(asctime)s %(levelname)-1.1s %(name)s:%(funcName)s:%(lineno)s | %(message)s"
    formatter = logging.Formatter(fmt=fmt)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)

    redis = aioredis.from_url(
        cfg.redis.dsn, encoding="utf-8", decode_responses=True, db=1
    )

    try:
        await set_calendar_data(redis)
        await set_security_data(redis)
    finally:
        if redis:
            await redis.close()

    return cfg


async def is_local_omega_alive():
    return True


def assert_bars_equal(exp, actual):
    assert_array_equal(exp["frame"].item().date(), actual["frame"][0])

    assert_array_almost_equal(exp["volume"], actual["volume"][0], decimal=-1)
    assert_array_almost_equal(exp["amount"], actual["amount"][0], decimal=-1)
    assert_array_almost_equal(exp["open"], actual["open"][0], 2)
    assert_array_almost_equal(exp["high"], actual["high"][0], 2)


def assert_daybars_equal(exp, actual):
    _a = []
    for _item in actual["frame"]:
        _a.append(_item.item().date())
    assert_array_equal(exp["frame"], _a)

    assert_array_almost_equal(exp["volume"], actual["volume"], decimal=-1)
    assert_array_almost_equal(exp["amount"], actual["amount"], decimal=-1)
    assert_array_almost_equal(exp["open"], actual["open"], 2)
    assert_array_almost_equal(exp["high"], actual["high"], 2)


def dir_test_home():
    from pathlib import Path

    home = os.path.dirname(__file__)
    return Path(home)


async def reset_influxdb():
    """clean up influxdb"""

    from omicron.dal.influx.influxclient import InfluxClient

    # create influxdb client
    url, token, bucket, org = (
        cfg.influxdb.url,
        cfg.influxdb.token,
        cfg.influxdb.bucket_name,
        cfg.influxdb.org,
    )
    client = InfluxClient(url, token, bucket, org)

    await client.delete_bucket()
    await client.create_bucket()
    return client


def mock_jq_data(filename: str):
    module_dir = os.path.dirname(__file__)
    _dir = os.path.normpath(os.path.join(module_dir, "jq_data"))

    target_file = f"{_dir}/{filename}"
    with open(target_file, "rb") as f:
        all_secs = pickle.load(f)
        return all_secs
