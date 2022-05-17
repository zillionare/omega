import datetime
import itertools
import logging
import pickle
from typing import AnyStr, Union

import arrow
import cfg4py
import numpy as np
from cfg4py.config import Config
from coretypes import FrameType, SecurityType
from omicron.dal import cache
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame

from omega.core import constants
from omega.core.constants import MINIO_TEMPORAL
from omega.core.events import Events
from omega.master.dfs import Storage
from omega.master.tasks.sync_other_bars import (
    get_month_week_day_sync_date,
    get_month_week_sync_task,
)
from omega.master.tasks.synctask import BarsSyncTask, master_syncbars_task
from omega.master.tasks.task_utils import delete_temporal_bars

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


def get_trade_limit_filename(
    prefix: SecurityType, dt: Union[datetime.datetime, datetime.date, AnyStr]
):
    assert isinstance(prefix, SecurityType)
    assert isinstance(dt, (datetime.datetime, datetime.date, str))
    filename = [prefix.value, "trade_limit", str(TimeFrame.date2int(dt))]
    return "/".join(filename)


async def write_trade_price_limits_to_dfs(name: str, dt: datetime.datetime):
    """
    将涨跌停写入dfs
    Args:
        name: task的名字
        dt: 写入dfs的日期，用来作为文件名
    Returns:
    """
    dfs = Storage()
    if dfs is None:  # pragma: no cover
        return
    today = datetime.datetime.now().date()

    for typ, ft in itertools.product(
        [SecurityType.STOCK, SecurityType.INDEX], [FrameType.DAY]
    ):
        queue_name = f"{MINIO_TEMPORAL}.{name}.{typ.value}.{ft.value}"

        data = await cache.temp.lrange(queue_name, 0, -1, encoding=None)
        if not data:  # pragma: no cover
            return
        all_bars = []
        for item in data:
            bars = pickle.loads(item)
            assert isinstance(bars, np.ndarray)
            all_bars.append(bars)
        bars = np.concatenate(all_bars)
        # 涨跌停写入inflaxdb 和 cache
        await Stock.save_trade_price_limits(bars, to_cache=False)
        if dt == today:
            logger.info("说明同步的是今天的涨跌停，写入cache")
            await Stock.save_trade_price_limits(bars, to_cache=True)
        binary = pickle.dumps(bars, protocol=cfg.pickle.ver)
        await dfs.write(get_trade_limit_filename(typ, dt), binary)
        await cache.temp.delete(queue_name)


async def run_sync_trade_price_limits_task(task: BarsSyncTask):
    """用来启动涨跌停的方法，接收一个task实例"""
    ret = await task.run()
    if not ret:
        # 执行失败需要删除数据队列
        await delete_temporal_bars(task.name, task.frame_type)
        return False
    await write_trade_price_limits_to_dfs(task.name, task.end)
    await cache.sys.set(
        constants.BAR_SYNC_TRADE_PRICE_TAIL, task.end.strftime("%Y-%m-%d")
    )


@master_syncbars_task()
async def sync_trade_price_limits():
    """每天9点半之后同步一次今日涨跌停并写入redis"""
    frame_type = FrameType.DAY
    async for sync_date in get_month_week_day_sync_date(
        constants.BAR_SYNC_TRADE_PRICE_TAIL, frame_type
    ):
        # 初始化task
        task = await get_month_week_sync_task(
            Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, sync_date, frame_type
        )
        task._quota_type = 2  # 白天的同步任务

        # 持久化涨跌停到dfs
        await run_sync_trade_price_limits_task(task)
