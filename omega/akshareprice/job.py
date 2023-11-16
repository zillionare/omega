import contextlib
import datetime
import io
import logging

import akshare as ak
import cfg4py
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from omega.akshareprice.datasync import data_writer, idx_data_writer, reset_cache

logger = logging.getLogger(__name__)


def load_cron_task(scheduler):
    scheduler.add_job(
        reset_cache_at_serverside,
        "cron",
        hour=9,
        minute=24,
        second=55,
        name="reset_cache_at_serverside",
    )

    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=9,
        minute=25,
        second="0,5,10",
        name="fetch_price_task",
    )

    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=9,
        minute="30-59",
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=10,
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=11,
        minute="0-29",
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=11,
        minute=30,
        second="0,5,10",
        name="fetch_price_task",
    )

    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour="13-14",
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=15,
        minute=0,
        second="0,5,10",
        name="fetch_price_task",
    )


async def start_cron_task():
    scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
    load_cron_task(scheduler)
    scheduler.start()


def get_akshare_data_em():
    try:
        all_secs = ak.stock_zh_a_spot_em()
        if all_secs is None or len(all_secs) == 0:
            return None

        # 最高 最低 今开 昨收
        data = all_secs[["代码", "最新价", "今开", "昨收", "最高", "最低"]]
        return data
    except Exception as e:
        logger.error("exception found while getting data from akshare: %s", e)
        return None


def get_akshare_index_sina():
    try:
        all_indexes = ak.stock_zh_index_spot()
        if all_indexes is None or len(all_indexes) == 0:
            return None

        # 代码 名称 最新价 涨跌额 涨跌幅 昨收 今开 最高 最低 成交量 成交额
        data = all_indexes[["代码", "最新价", "昨收", "今开", "最高", "最低"]]
        return data
    except Exception as e:
        logger.error("exception found while getting data from akshare: %s", e)
        return None


async def reset_cache_at_serverside():
    now = datetime.datetime.now()
    if now.weekday() >= 5:  # 周末不运行
        return True

    await reset_cache()


async def process_stock_price():
    data = get_akshare_data_em()
    if data is None:
        logger.info("no stock data returned from akshare")
        return False

    await data_writer(data, "server")
    return True


def ak_get_idx_price():
    with contextlib.redirect_stderr(io.StringIO()):
        return get_akshare_index_sina()


async def process_index_price():
    # 指数价格下载的速度依赖网络环境，首选云平台

    data = ak_get_idx_price()
    if data is None:
        logger.info("no index data returned from akshare")
        return False

    await idx_data_writer(data, "server")
    return True


async def fetch_price_from_akshare():
    # cfg = cfg4py.get_instance()
    running_mode = "server"

    now = datetime.datetime.now()
    if now.weekday() >= 5:  # 周末不运行
        return True

    seconds = now.second
    _seq_num = seconds / 5

    if _seq_num % 2 == 0:  # running at 0, 10, 20, 30, 40, 50
        logger.info("%s side: %s", running_mode, now)
        await process_stock_price()

    if _seq_num % 4 == 0:  # running at 0, 20, 40
        logger.info("%s side for index: %s", running_mode, now)
        await process_index_price()

    return True
