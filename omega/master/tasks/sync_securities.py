import datetime
import logging
import pickle
from os import sync

import arrow
import cfg4py
import numpy as np
from coretypes import FrameType
from omicron.dal import cache
from omicron.models.stock import Security
from omicron.models.timeframe import TimeFrame

from omega.core import constants
from omega.core.constants import MINIO_TEMPORAL
from omega.core.events import Events
from omega.master.dfs import Storage
from omega.master.tasks.sec_synctask import SecuritySyncTask, master_secs_task

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
        now = arrow.now().date()
        pre_trade_day = TimeFrame.day_shift(now, 0)

        if not head or not tail:
            # try to get date scope from db
            head, tail = await Security.get_datescope_from_db()
            if head is None or tail is None:
                head = tail = None
            else:
                head = head.strftime("%Y-%m-%d")
                tail = tail.strftime("%Y-%m-%d")

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
            logger.info("sync_securities_list: 所有数据已同步完毕")
            break

        yield sync_dt, head, tail


def get_securities_dfs_filename(dt: datetime.date):
    filename = ["securities", str(TimeFrame.date2int(dt))]
    return "/".join(filename)


async def delete_temporal_data(name: str):
    """清理临时存储在redis中的行情数据"""
    p = cache.temp.pipeline()
    key = f"{constants.MINIO_TEMPORAL}.{name}.day"
    p.delete(key)
    await p.execute()


async def run_security_sync_task(task: SecuritySyncTask):
    ret = await task.run()
    if not ret:
        # 执行失败需要删除数据队列
        # await delete_temporal_data(task.name, task.frame_type)
        return False

    # await write_seclist_to_dfs(task.name, task.end)
    return True


async def get_security_sync_task(sync_dt: datetime.datetime):
    # 1:30左右开始，最长到8:30，下午16:00开始，到24:00结束
    name = "securities_sync"
    task = SecuritySyncTask(
        event=Events.OMEGA_DO_SYNC_SECURITIES,
        name=name,
        end=sync_dt,
        timeout=1200,  # 单次任务最长20分钟超时
        recs_per_task=7500,  # 目前只有7111条记录
    )
    if sync_dt.year < 2010:
        task.recs_per_task = 2000
    elif sync_dt.year < 2014:
        task.recs_per_task = 3500
    elif sync_dt.year < 2017:
        task.recs_per_task = 4500
    elif sync_dt.year < 2020:
        task.recs_per_task = 5500
    elif sync_dt.year < 2022:
        task.recs_per_task = 6500
    else:
        task.recs_per_task = 7500

    return task


"""
2005-12-31 1410
2006-12-31 1488
2007-12-31 1647
2008-12-31 1744
2009-12-31 1945
2010-12-31 2408
2011-12-31 2871
2012-12-31 3208
2013-12-31 3346
2014-12-31 3601
2015-12-31 4118
2016-12-31 4426
2017-12-31 4938
2018-12-31 5042
2019-12-31 5331
2020-12-31 5603
2021-12-31 6262
"""


@master_secs_task()
async def sync_securities_list():
    """同步证券列表，每天凌晨执行一次，利用多余的quota每天追赶一部分数据"""

    logger.info("sync_securities_list starts")

    sync_days_count = 0
    async for sync_dt, head, tail in get_security_sync_date():
        logger.info(
            "sync_securities_list, target date: %s", sync_dt.strftime("%Y-%m-%d")
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
            sync_days_count += 1
            logger.info(f"{task.name}({task.end})同步完成,参数为{task.params}")

            if sync_days_count >= 300:  # 3 * 75s，大约4~5分钟
                break

    logger.info("sync_securities_list ends")
