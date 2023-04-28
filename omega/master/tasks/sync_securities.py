import datetime
import logging

import arrow
from coretypes import FrameType
from omicron.dal import cache
from omicron.models.stock import Security
from omicron.models.timeframe import TimeFrame

from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.sec_synctask import SecuritySyncTask, master_secs_task
from omega.master.tasks.task_utils import get_previous_trade_day

logger = logging.getLogger(__name__)


async def get_security_sync_date():
    """计算需要证券信息需要同步的时间列表"""

    while True:
        # 取同步的时间头尾信息，head即更早的时间，tail是最新的时间
        head_str, tail_str = (
            await cache.sys.get(constants.SECS_SYNC_ARCHIVE_HEAD),
            await cache.sys.get(constants.SECS_SYNC_ARCHIVE_TAIL),
        )

        # 获取前一个交易日
        now = arrow.now().date()
        pre_trade_day = get_previous_trade_day(now)

        if not head_str or not tail_str:
            # try to get date scope from db, date(), not datetime()
            head, tail = await Security.get_datescope_from_db()
            if head is None or tail is None:
                head_str = tail_str = None
            else:
                head_str = head.strftime("%Y-%m-%d")
                await cache.sys.set(constants.SECS_SYNC_ARCHIVE_HEAD, head_str)
                tail_str = tail.strftime("%Y-%m-%d")
                await cache.sys.set(constants.SECS_SYNC_ARCHIVE_TAIL, tail_str)

        if not head_str or not tail_str:
            logger.info("首次同步，查找最新的交易日, %s", pre_trade_day.strftime("%Y-%m-%d"))
            sync_dt = datetime.datetime.combine(pre_trade_day, datetime.time(0, 0))
            head = tail = pre_trade_day  # 同步完之后更新redis的head和tail
        else:
            # 说明不是首次同步，检查tail到当前的上一个交易日有没有空洞
            tail = datetime.datetime.strptime(tail_str, "%Y-%m-%d")
            head = datetime.datetime.strptime(head_str, "%Y-%m-%d")
            frames = TimeFrame.count_frames(
                tail,
                pre_trade_day,
                FrameType.DAY,
            )
            if frames > 1:
                # 最后同步时间到当前最新的交易日之间有空洞
                tail = datetime.datetime.combine(
                    TimeFrame.day_shift(tail, 1), datetime.time(0, 0)
                )
                sync_dt = tail
                head = None  # 不更新redis中head的日期，保持不变
            else:
                # 已同步到最后一个交易日，向前追赶

                # 检查时间是否小于 2005年，小于则说明同步完成了
                day_frame = TimeFrame.day_frames[0]
                if TimeFrame.date2int(head) <= day_frame:
                    logger.info("sync_securities_list: 所有数据已同步完毕")
                    break

                sync_dt = head = datetime.datetime.combine(
                    TimeFrame.day_shift(head, -1), datetime.time(0, 0)
                )
                tail = None  # 不更新redis中tail的日期，保持不变

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
    name = "securities_sync"
    task = SecuritySyncTask(
        event=Events.OMEGA_DO_SYNC_SECURITIES,
        name=name,
        end=sync_dt,
        timeout=3600 * 2,  # 从2005年到2022年，大约需要70分钟
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
2009-12-31 1945
2011-12-31 2871
2012-12-31 3208
2014-12-31 3601
2015-12-31 4118
2016-12-31 4426
2017-12-31 4938
2019-12-31 5331
2020-12-31 5603
2021-12-31 6262
"""


@master_secs_task()
async def sync_securities_list():
    """同步证券列表，每天8点执行一次，存昨日的数据到db，取今日的数据到缓存（包含今日上市）"""

    logger.info("sync_securities_list starts")

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

            logger.info(f"{task.name}({task.end})同步完成,参数为{task.params}")

    logger.info("sync_securities_list ends")
