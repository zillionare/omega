import datetime
import logging
import os
import pickle

import aioredis
import cfg4py
import numpy as np
from coretypes import FrameType, bars_dtype
from omicron.models import get_influx_client
from omicron.models.timeframe import TimeFrame

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


dtype_sec_list = [
    ("_time", "datetime64[s]"),
    ("code", "O"),
    ("info", "O"),
]

dtype_bars_min = [
    ("frame", "datetime64[s]"),
    ("code", "O"),
    ("open", "f4"),
    ("high", "f4"),
    ("low", "f4"),
    ("close", "f4"),
    ("volume", "f8"),
    ("amount", "f8"),
    ("factor", "f4"),
]

dtype_bars_day = [
    ("frame", "datetime64[s]"),
    ("code", "O"),
    ("open", "f4"),
    ("close", "f4"),
    ("high", "f4"),
    ("low", "f4"),
    ("high_limit", "f4"),
    ("low_limit", "f4"),
    ("volume", "f8"),
    ("amount", "f8"),
    ("factor", "f4"),
]


def decode_sec_code(code_c: int):
    if code_c > 2000000:
        _tmp = f"{code_c}.XSHG"
        return _tmp[1:]
    else:
        _tmp = f"{code_c}.XSHE"
        return _tmp[1:]


def decode_board_code(code_c: int):
    return f"{code_c}.THS"


async def save_sec_list(records):
    measurement = "security_list"
    client = get_influx_client()
    # _time, code, info

    converted_list = []
    for x in records:
        code = decode_sec_code(x["code"])
        converted_list.append((x["_time"], code, x["info"]))

    report_list = np.array(converted_list, dtype=dtype_sec_list)
    await client.save(report_list, measurement, time_key="_time", tag_keys=["code"])
    logger.info("%d records saved into %s", len(report_list), measurement)


async def save_sec_xrxd_info(records):
    measurement = "security_xrxd_reports"
    client = get_influx_client()
    # _time, code, info

    converted_list = []
    for x in records:
        code = decode_sec_code(x["code"])
        converted_list.append((x["_time"], code, x["info"]))

    report_list = np.array(converted_list, dtype=dtype_sec_list)
    await client.save(report_list, measurement, time_key="_time", tag_keys=["code"])
    logger.info("%d records saved into %s", len(report_list), measurement)


async def save_bars_1d(records):
    measurement = "stock_bars_1d"
    client = get_influx_client()

    converted_list = []
    for x in records:
        code = decode_sec_code(x["code"])
        converted_list.append(
            (
                x["frame"],
                code,
                x["open"],
                x["close"],
                x["high"],
                x["low"],
                x["high_limit"],
                x["low_limit"],
                x["volume"],
                x["amount"],
                x["factor"],
            )
        )

    report_list = np.array(converted_list, dtype=dtype_bars_day)
    await client.save(report_list, measurement, time_key="frame", tag_keys=["code"])
    logger.info("%d records saved into %s", len(report_list), measurement)


async def save_bars_week_month(records, ft: FrameType):
    measurement = "stock_bars_%s" % ft.value
    client = get_influx_client()

    converted_list = []
    for x in records:
        code = decode_sec_code(x["code"])
        converted_list.append(
            (
                x["frame"],
                code,
                x["open"],
                x["high"],
                x["low"],
                x["close"],
                x["volume"],
                x["amount"],
                x["factor"],
            )
        )

    report_list = np.array(converted_list, dtype=dtype_bars_min)
    await client.save(report_list, measurement, time_key="frame", tag_keys=["code"])
    logger.info("%d records saved into %s", len(report_list), measurement)


async def save_bars_30m(records):
    measurement = "stock_bars_30m"
    client = get_influx_client()

    converted_list = []
    for x in records:
        code = decode_sec_code(x["code"])
        converted_list.append(
            (
                x["frame"],
                code,
                x["open"],
                x["high"],
                x["low"],
                x["close"],
                x["volume"],
                x["amount"],
                x["factor"],
            )
        )

    report_list = np.array(converted_list, dtype=dtype_bars_min)
    await client.save(report_list, measurement, time_key="frame", tag_keys=["code"])
    logger.info("%d records saved into %s", len(report_list), measurement)


async def save_board_bars(records):
    measurement = "board_bars_1d"
    client = get_influx_client()
    await client.drop_measurement(measurement)

    converted_list = []
    for x in records:
        code = decode_board_code(x["code"])
        converted_list.append(
            (
                x["frame"],
                code,
                x["open"],
                x["high"],
                x["low"],
                x["close"],
                x["volume"],
                x["amount"],
                x["factor"],
            )
        )

    report_list = np.array(converted_list, dtype=dtype_bars_min)
    await client.save(report_list, measurement, time_key="frame", tag_keys=["code"])
    logger.info("%d records saved into %s", len(report_list), measurement)
