import datetime
import itertools
import logging
import pickle
from typing import AnyStr, List, Union

import arrow
import cfg4py
import numpy as np
from coretypes import FrameType, SecurityType
from omicron.dal import cache
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame

from omega.core import constants
from omega.core.constants import MINIO_TEMPORAL
from omega.core.events import Events
from omega.master.dfs import Storage
from omega.master.tasks.synctask import BarsSyncTask
from omega.master.tasks.task_utils import abnormal_master_report, delete_temporal_bars

logger = logging.getLogger(__name__)


async def get_security_sync_date():
    """计算需要证券信息需要同步的时间列表"""

    while True:
        # 取同步的时间头尾信息，head即更早的时间，tail是最新的时间
        head, tail = (
            await cache.sys.get(constants.SECS_SYNC_ARCHIVE_HEAD),
            await cache.sys.get(constants.SECS_SYNC_ARCHIVE_TAIL),
        )

        # 获取前一个交易日
        now = arrow.now().naive.date()
        pre_trade_day = TimeFrame.day_shift(now, 0)

        if not head or not tail:
            logger.info("首次同步，查找最新的交易日, %s", pre_trade_day.strftime("%Y-%m-%d"))
            sync_dt = datetime.datetime.combine(pre_trade_day, datetime.time(0, 0))
            head = tail = pre_trade_day  # 同步完之后更新redis的head和tail
        else:
            # 说明不是首次同步，检查tail到当前的上一个交易日有没有空洞
            tail_date = datetime.datetime.strptime(tail, "%Y-%m-%d")
            head_date = datetime.datetime.strptime(head, "%Y-%m-%d")
            frames = TimeFrame.count_frames(
                tail_date,
                pre_trade_day,
                FrameType.DAY,
            )
            if frames > 1:
                # 最后同步时间到当前最新的交易日之间有空洞
                tail_date = datetime.datetime.combine(
                    TimeFrame.day_shift(tail_date, 1), datetime.time(0, 0)
                )
                sync_dt = tail = tail_date
                head = None  # 不更新redis中head的日期，保持不变
            else:
                # 已同步到最后一个交易日，向前追赶
                sync_dt = head = datetime.datetime.combine(
                    TimeFrame.day_shift(head_date, -1), datetime.time(0, 0)
                )
                tail = None  # 不更新redis中tail的日期，保持不变

        # 检查时间是否小于 2005年，小于则说明同步完成了
        day_frame = TimeFrame.day_frames[0]
        if TimeFrame.date2int(sync_dt) <= day_frame:
            logger.info("sync_securities_job: 所有数据已同步完毕")
            break

        yield sync_dt, head, tail


def get_securities_dfs_filename(dt: datetime.date):
    filename = ["securities", str(TimeFrame.date2int(dt))]
    return "/".join(filename)


async def write_seclist_to_dfs(name: str, dt: datetime.datetime):
    dfs = Storage()
    if dfs is None:  # pragma: no cover
        return

    queue_name = f"{MINIO_TEMPORAL}.{name}.day"

    data = await cache.temp.lrange(queue_name, 0, -1, encoding=None)
    if not data:  # pragma: no cover
        return
    all_bars = []
    for item in data:
        bars = pickle.loads(item)
        assert isinstance(bars, np.ndarray)
        all_bars.append(bars)
    bars = np.concatenate(all_bars)

    cfg = cfg4py.get_instance()
    await Stock.save_trade_price_limits(bars, to_cache=False)
    binary = pickle.dumps(bars, protocol=cfg.pickle.ver)
    await dfs.write(get_securities_dfs_filename(dt), binary)
    await cache.temp.delete(queue_name)


async def delete_temporal_data(name: str):
    """清理临时存储在redis中的行情数据"""
    p = cache.temp.pipeline()
    key = f"{constants.MINIO_TEMPORAL}.{name}.day"
    p.delete(key)
    await p.execute()


async def run_security_sync_task(task: BarsSyncTask):
    ret = await task.run()
    if not ret:
        # 执行失败需要删除数据队列
        await delete_temporal_data(task.name, task.frame_type)
        return False

    # await write_seclist_to_dfs(task.name, task.end)


async def get_security_sync_task(sync_dt: datetime.datetime):
    name = "securities_sync"
    frame_type = [FrameType.DAY]
    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_SECURITIES,
        name=name,
        end=sync_dt,
        frame_type=frame_type,  # 需要同步的类型
        timeout=60,
        recs_per_sec=7500,  # 目前只有7111条记录
    )
    return task


@abnormal_master_report()
async def sync_securities_job():
    """同步证券列表，每天凌晨执行一次，利用多余的quota每天追赶一部分数据"""

    logger.info("sync_securities_job starts")
    async for sync_dt, head, tail in get_security_sync_date():
        logger.info(
            "sync_securities_job, target date: %s", sync_dt.strftime("%Y-%m-%d")
        )

        task = await get_security_sync_task(sync_dt)
        success = await run_security_sync_task(task)
        if not success:  # pragma: no cover
            break
        else:
            # 成功同步了`sync_dt`这一天的数据，更新 head 和 tail
            if head is not None:
                await cache.sys.set(
                    constants.SECS_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d")
                )
            if tail is not None:
                await cache.sys.set(
                    constants.SECS_SYNC_ARCHIVE_TAIL, tail.strftime("%Y-%m-%d")
                )
            logger.info(f"{task.name}({task.end})同步完成,参数为{task.params}")

    logger.info("sync_securities_job ends")
