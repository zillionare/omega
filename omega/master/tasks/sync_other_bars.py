import datetime
import logging
from typing import List

import arrow
from coretypes import FrameType
from omicron.dal import cache
from omicron.models.timeframe import TimeFrame

from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.synctask import BarsSyncTask, master_syncbars_task
from omega.master.tasks.task_utils import write_dfs

logger = logging.getLogger(__name__)


async def run_month_week_sync_task(tail_key: str, task: BarsSyncTask):
    """
    运行周月线task实例的方法
    Args:
        tail_key: 记录周月线在redis最后一天的日期 如redis记录 2010-10-10 则说明 2005-01-01至-2010-10-10的数据已经同步完毕
        task: task的实例

    Returns:

    """
    ret = await task.run()
    if not ret:
        return False

    logger.info(
        "month_week_sync_task success, writing data to dfs: %s, %s", task.name, task.end
    )
    await write_dfs(task.name, task.end, task.frame_type)

    await cache.sys.set(tail_key, task.end.strftime("%Y-%m-%d"))
    logger.info("set %s to cache %s", task.end, tail_key)


async def get_month_week_day_sync_date(tail_key: str, frame_type: FrameType):
    """获取周月的同步日期，通常情况下，周月数据相对较少，一天的quota足够同步完所有的数据，所以直接从2005年开始同步至今
    做成生成器方式，不停的获取时间
    """
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
            tail = TimeFrame.shift(tail, 1, frame_type)
        count_frame = TimeFrame.count_frames(
            tail,
            now.replace(hour=0, minute=0, second=0, microsecond=0),
            frame_type,
        )
        if count_frame >= 1:
            yield tail
        else:
            break


async def get_month_week_sync_task(
    event: str, sync_date: datetime.datetime, frame_type: FrameType
) -> BarsSyncTask:
    """
    Args:
        event: 事件名称，emit通过这个key发布消息到worker
        sync_date: 同步的时间
        frame_type: 同步的K线类型
    Returns:
    """
    name = frame_type.value

    task = BarsSyncTask(
        event=event,
        name=name,
        frame_type=[frame_type],
        end=sync_date,
        timeout=60 * 10,
        recs_per_sec=2,
    )
    return task


async def get_min_5_15_30_60_sync_date(tail_key: str, frame_type: FrameType):
    now = arrow.now().naive
    while True:
        tail = await cache.sys.get(tail_key)
        if not tail:
            tail = TimeFrame.int2date(TimeFrame.day_frames[0])
        else:
            tail = datetime.datetime.strptime(tail, "%Y-%m-%d")
            tail = TimeFrame.shift(tail, 1, frame_type)

        # get frame count
        count_frame = TimeFrame.count_frames(
            tail, now.replace(hour=0, minute=0, second=0, microsecond=0), frame_type
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
async def sync_min_5_15_30_60():
    """同步 5 15 30 60 分钟线"""
    # 检查周线 tail
    frame_type = [
        FrameType.MIN5,
        FrameType.MIN15,
        FrameType.MIN30,
        FrameType.MIN60,
    ]
    async for sync_date in get_min_5_15_30_60_sync_date(
        constants.BAR_SYNC_OTHER_MIN_TAIL, FrameType.DAY  # 传日线进去就行，因为这个是按照天同步的
    ):
        # 初始化task
        task = BarsSyncTask(
            event=Events.OMEGA_DO_SYNC_OTHER_MIN,
            name="min_5_15_30_60",
            frame_type=frame_type,
            end=sync_date,
            timeout=60 * 10,
            recs_per_sec=48 + 16 + 8 + 4,
        )

        await run_month_week_sync_task(constants.BAR_SYNC_OTHER_MIN_TAIL, task)
        if not task.status:
            break


@master_syncbars_task()
async def sync_week_bars():
    """同步周线"""
    # 检查周线 tail
    frame_type = FrameType.WEEK
    async for sync_date in get_month_week_day_sync_date(
        constants.BAR_SYNC_WEEK_TAIL, frame_type
    ):
        # 初始化task
        task = await get_month_week_sync_task(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK, sync_date, frame_type
        )
        await run_month_week_sync_task(constants.BAR_SYNC_WEEK_TAIL, task)
        if not task.status:
            break


@master_syncbars_task()
async def sync_month_bars():
    """同步月线"""
    frame_type = FrameType.MONTH
    async for sync_date in get_month_week_day_sync_date(
        constants.BAR_SYNC_MONTH_TAIL, frame_type
    ):
        # 初始化task
        task = await get_month_week_sync_task(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK, sync_date, frame_type
        )
        await run_month_week_sync_task(constants.BAR_SYNC_MONTH_TAIL, task)
        if not task.status:
            break
