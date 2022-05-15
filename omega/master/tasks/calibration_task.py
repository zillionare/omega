import datetime
import logging

import arrow
from coretypes import FrameType
from omicron.dal import cache
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame

from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.synctask import BarsSyncTask
from omega.master.tasks.task_utils import (
    abnormal_master_report,
    get_yesterday_or_pre_trade_day,
    write_dfs,
)

logger = logging.getLogger(__name__)


async def get_sync_date():
    """计算校准（追赶）同步的日期

    追赶同步是系统刚建立不久时，数据库缺少行情数据，受限于每日从上游服务器取数据的quota，因此只能每日在quota限额内，进行小批量的同步，逐步把数据库缺失的数据补起来的一种同步。

    追赶同步使用与校准同步同样的实现，但优先级较低，只有在校准同步完成后，还有quota余额的情况下才进行。

    Returns:
        返回一个(sync_dt, head, tail)组成的元组，sync_dt是同步的日期，如果为None，则无需进行追赶同步。

    """
    while True:
        head, tail = (
            await cache.sys.get(constants.BAR_SYNC_ARCHIVE_HEAD),
            await cache.sys.get(constants.BAR_SYNC_ARCHIVE_TAIL),
        )

        # todo: check if it can get right sync_dt
        now = arrow.now().naive.date()
        pre_trade_day = get_yesterday_or_pre_trade_day(now)
        if not head or not tail:
            # 任意一个缺失都不行
            logger.info("说明是首次同步，查找上一个已收盘的交易日")
            sync_dt = datetime.datetime.combine(pre_trade_day, datetime.time(0, 0))
            head = tail = pre_trade_day

        else:
            # 说明不是首次同步，检查tail到现在有没有空洞
            tail_date = datetime.datetime.strptime(tail, "%Y-%m-%d")
            head_date = datetime.datetime.strptime(head, "%Y-%m-%d")
            frames = TimeFrame.count_frames(
                tail_date,
                pre_trade_day,
                FrameType.DAY,
            )
            if frames > 1:
                # 最后同步时间到当前最后一个结束的交易日之间有空洞，向后追赶
                tail_date = datetime.datetime.combine(
                    TimeFrame.day_shift(tail_date, 1), datetime.time(0, 0)
                )
                sync_dt = tail = tail_date
                head = None

            else:
                # 已同步到最后一个交易日，向前追赶
                sync_dt = head = datetime.datetime.combine(
                    TimeFrame.day_shift(head_date, -1), datetime.time(0, 0)
                )
                tail = None

        # 检查时间是否小于 2005年，小于则说明同步完成了
        day_frame = TimeFrame.day_frames[0]
        if TimeFrame.date2int(sync_dt) <= day_frame:
            logger.info("所有数据已同步完毕")
            # sync_dt = None
            break

        yield sync_dt, head, tail


async def run_daily_calibration_sync_task(task: BarsSyncTask):
    """

    Args:
        task: 需要运行的task实例
    """

    ret = await task.run()
    # 如果运行中出现错误，则中止本次同步
    if not ret:  # pragma: no cover
        return ret
    logger.info(f"daily_calibration -- params:{task.params} 已执行完毕，准备进行持久化")

    # 将raw数据写入块存储--minio
    # todo: disable resample for now
    await write_dfs(task.name, task.end, task.frame_type, resample=False)

    return True


async def get_daily_calibration_job_task(
    sync_dt: datetime.datetime,
):
    """
    获取凌晨校准同步的task实例
    Args:
        sync_dt: 需要同步的时间

    Returns:

    """
    end = sync_dt.replace(hour=15, minute=0, microsecond=0, second=0)
    # 检查 end 是否在交易日

    name = "calibration_sync"
    frame_type = [FrameType.MIN1, FrameType.DAY]
    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_DAILY_CALIBRATION,
        name=name,
        end=end,
        frame_type=frame_type,  # 需要同步的类型
        timeout=60 * 60 * 6,
        recs_per_sec=int((240 * 2 + 4) // 0.75),
    )
    return task


@abnormal_master_report()
async def daily_calibration_job():
    """scheduled task entry

    runs at every day at 2:00 am
    """
    logger.info("每日数据校准已启动")
    now = arrow.now().date()
    async for sync_dt, head, tail in get_sync_date():
        # 创建task
        # 当天的校准启动前，先清除缓存。
        if sync_dt.date() == TimeFrame.day_shift(now, -1):
            await Stock.reset_cache()

        task = await get_daily_calibration_job_task(sync_dt)
        success = await run_daily_calibration_sync_task(task)
        if not success:  # pragma: no cover
            break
        else:
            # 成功同步了`sync_dt`这一天的数据，更新 head 和 tail
            if head is not None:
                await cache.sys.set(
                    constants.BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d")
                )
            if tail is not None:
                await cache.sys.set(
                    constants.BAR_SYNC_ARCHIVE_TAIL, tail.strftime("%Y-%m-%d")
                )
            logger.info(f"{task.name}({task.end})同步完成,参数为{task.params}")

    logger.info("exit daily_calibration_job")
