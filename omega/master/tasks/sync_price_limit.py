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
from omega.master.tasks.sync_other_bars import get_month_week_sync_task
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


async def write_price_limits_to_dfs(
    name: str, dt: datetime.date, cache_only: bool = False
):
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

    # clear cache if necessory: cache only, or dt = date_in_cache
    await Stock.reset_price_limits_cache(cache_only, dt)

    for typ, ft in itertools.product(
        [SecurityType.STOCK, SecurityType.INDEX], [FrameType.DAY]
    ):
        queue_name = f"{MINIO_TEMPORAL}.{name}.{typ.value}.{ft.value}"

        data = await cache.temp.lrange(queue_name, 0, -1)
        if not data:  # pragma: no cover
            return
        all_bars = []
        for item in data:
            bars = pickle.loads(item)
            assert isinstance(bars, np.ndarray)
            all_bars.append(bars)
        bars = np.concatenate(all_bars)

        # 涨跌停写入inflaxdb 和 cache
        if cache_only:
            if typ == SecurityType.STOCK:  # 只缓存股票涨跌停数据
                logger.info(f"{typ.value}.{ft.value}, sync data to cache only!")
                await Stock.save_trade_price_limits(bars, to_cache=True)
        else:
            await Stock.save_trade_price_limits(bars, to_cache=False)

            binary = pickle.dumps(bars, protocol=cfg.pickle.ver)
            await dfs.write(get_trade_limit_filename(typ, dt), binary)
            await cache.temp.delete(queue_name)


async def run_sync_price_limits_task(task: BarsSyncTask, cache_only: bool = False):
    """用来启动涨跌停的方法，接收一个task实例"""
    ret = await task.run()
    if not ret:
        # 执行失败需要删除数据队列
        await delete_temporal_bars(task.name, task.frame_type)
        return False
    await write_price_limits_to_dfs(task.name, task.end, cache_only)

    if cache_only is False:  # 正常任务，不是当天刷新cache的临时任务
        await cache.sys.set(
            constants.BAR_SYNC_TRADE_PRICE_TAIL, task.end.strftime("%Y-%m-%d")
        )

    return True


async def get_trade_price_limits_sync_date(tail_key: str, frame_type: FrameType):
    epoch_start = {
        FrameType.DAY: TimeFrame.int2date(TimeFrame.day_frames[0]),
        FrameType.WEEK: TimeFrame.int2date(TimeFrame.week_frames[0]),
        FrameType.MONTH: TimeFrame.int2date(TimeFrame.month_frames[0]),
    }
    while True:
        tail = await cache.sys.get(tail_key)
        now = arrow.now().naive
        if not tail:
            tail = epoch_start.get(frame_type)
        else:
            tail = datetime.datetime.strptime(tail, "%Y-%m-%d")
            tail = TimeFrame.shift(tail, 1, frame_type)  # 返回datetime.date

        # 计算交易日间隔
        count_frame = TimeFrame.count_frames(
            tail,
            now.replace(hour=0, minute=0, second=0, microsecond=0),
            frame_type,
        )

        if TimeFrame.is_trade_day(now):
            if count_frame >= 2:  # 交易日需要间隔一天
                yield tail
            else:
                break
        else:
            if count_frame >= 1:  # 非交易日只需要取上一个交易日即可
                yield tail
            else:
                break


@master_syncbars_task()
async def sync_trade_price_limits():
    """每天1:45同步一次上一个交易日的涨跌停"""
    frame_type = FrameType.DAY

    async for sync_date in get_trade_price_limits_sync_date(
        constants.BAR_SYNC_TRADE_PRICE_TAIL, frame_type
    ):
        # sync_data, datetime.date
        logger.info("sync_trade_price_limits, for %s", sync_date)

        task = await get_month_week_sync_task(
            Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, sync_date, frame_type
        )
        task._quota_type = 2  # 白天的同步任务

        # 持久化涨跌停到dfs，更新时间戳
        rc = await run_sync_price_limits_task(task, False)
        if not rc:  # 执行出错，下次再尝试
            break


@master_syncbars_task()
async def sync_cache_price_limits():
    """每天9:01/09:16各同步一次今日涨跌停并写入redis"""
    frame_type = FrameType.DAY

    now = datetime.datetime.now()
    dt = now.date()  # dt is datetime.date
    if TimeFrame.is_trade_day(dt):
        # 9:01同步一次
        if now.hour == 9 and now.minute < 10:
            logger.info("9:01, sync price limits first time")

            task = await get_month_week_sync_task(
                Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, dt, frame_type
            )
            task._quota_type = 2  # 白天的同步任务

            await run_sync_price_limits_task(task, True)
            return True

        # 9:16再次同步（除权除息等造成的更新）
        if now.hour == 9 and now.minute > 15:
            logger.info("9:16, sync price limits second time")

            task = await get_month_week_sync_task(
                Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, dt, frame_type
            )
            task._quota_type = 2  # 白天的同步任务

            await run_sync_price_limits_task(task, True)
            return True
