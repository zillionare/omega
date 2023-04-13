import datetime
import logging
import pickle

import arrow
import cfg4py
from coretypes import FrameType
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


async def set_cache_ts_for_records(redis, ts: datetime.date):
    ts_str = ts.strftime("%Y-%m-%d")

    # security list
    await redis.set("jobs.secs_sync.archive.head", "2005-01-04")
    await redis.set("jobs.secs_sync.archive.tail", ts_str)

    # bars:1d
    await redis.set("jobs.bars_sync.day.head", "2005-01-04")
    await redis.set("jobs.bars_sync.day.tail", ts_str)

    # bars:1d
    await redis.set("jobs.bars_sync.archive.head", "2005-01-04")
    await redis.set("jobs.bars_sync.archive.tail", ts_str)

    await redis.set("jobs.bars_sync.min_5_15_30_60.tail", ts_str)
    await redis.set("jobs.bars_sync.trade_price.tail", ts_str)

    now = datetime.datetime.now()
    min_ts = TimeFrame.day_shift(now, -1)
    # 减少分钟线下载开销，强制对齐到上一个交易日结束
    ts_min_str = min_ts.strftime("%Y-%m-%d 15:00:00")
    await redis.set("master.bars_sync.minute.tail", ts_min_str)

    week_ts = TimeFrame.week_shift(ts, 0)
    await redis.set("jobs.bars_sync.week.tail", week_ts.strftime("%Y-%m-%d"))

    month_ts = TimeFrame.month_shift(ts, 0)
    await redis.set("jobs.bars_sync.month.tail", month_ts.strftime("%Y-%m-%d"))

    logger.info("all timestamp for influx records are set in cache.")
