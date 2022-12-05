import datetime
import logging

import numpy as np
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.serialize import DataframeDeserializer
from omicron.models import get_influx_client
from omicron.models.security import Security
from omicron.models.timeframe import TimeFrame

from omega.boards.board import ConceptBoard, IndustryBoard

logger = logging.getLogger(__name__)


board_bars_dtype = np.dtype(
    [
        ("code", "O"),
        ("frame", "datetime64[D]"),
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
        ("volume", "f8"),
        ("amount", "f8"),
    ]
)


async def get_latest_date_from_db(_code: str):
    # 行业板块回溯1年的数据，概念板块只取当年的数据
    code = f"{_code}.THS"

    client = get_influx_client()
    measurement = "board_bars_1d"

    now = datetime.datetime.now()
    dt_end = TimeFrame.day_shift(now, 0)
    # 250 + 60: 可以得到60个MA250的点, 默认K线图120个节点
    dt_start = TimeFrame.day_shift(now, -310)

    flux = (
        Flux()
        .measurement(measurement)
        .range(dt_start, dt_end)
        .bucket(client._bucket)
        .tags({"code": code})
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return dt_start

    ds = DataframeDeserializer(
        sort_values="_time", usecols=["_time"], time_col="_time", engine="c"
    )
    actual = ds(data)
    secs = actual.to_records(index=False).astype("datetime64[s]")

    _dt = secs[-1].item()
    return _dt.date()


async def save_board_bars(bars):
    client = get_influx_client()
    measurement = "board_bars_1d"

    logger.info("persisting bars to influxdb: %s, %d secs", measurement, len(bars))

    await client.save(bars, measurement, tag_keys=["code"], time_key="frame")

    return True
