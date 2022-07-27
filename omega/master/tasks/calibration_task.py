import datetime
import logging

import arrow
from coretypes import FrameType
from omicron.dal import cache
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame

from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.synctask import BarsSyncTask, master_syncbars_task
from omega.master.tasks.task_utils import get_previous_trade_day, write_dfs

logger = logging.getLogger(__name__)


async def get_sync_date(key_head, key_tail):
    """获取下一个同步数据的时间点

    head即更早的时间，最早为2005-01-04，tail是最新的时间，上一个交易日

    Returns:
        返回一个(sync_dt, head, tail)组成的元组，sync_dt是同步的日期，如果为None，则无需进行追赶同步。

    """
    while True:
        head, tail = (
            await cache.sys.get(key_head),
            await cache.sys.get(key_tail),
        )

        now = arrow.now().naive.date()
        if now == datetime.date(2005, 1, 4):
            break

        pre_trade_day = get_previous_trade_day(now)
        if not head or not tail:  # 任意一个缺失都不行
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
            if frames > 1:  # 最后同步时间到当前最后一个结束的交易日之间有空洞，向后追赶
                tail_date = datetime.datetime.combine(
                    TimeFrame.day_shift(tail_date, 1), datetime.time(0, 0)
                )
                sync_dt = tail = tail_date
                head = None
            else:  # 检查时间是否小于 2005年，小于则说明同步完成了
                day_frame = TimeFrame.day_frames[0]
                if TimeFrame.date2int(head_date) <= day_frame:
                    logger.info("所有数据已同步完毕")
                    break

                # 已同步到最后一个交易日，向前追赶
                sync_dt = head = datetime.datetime.combine(
                    TimeFrame.day_shift(head_date, -1), datetime.time(0, 0)
                )
                tail = None

        yield sync_dt, head, tail


async def run_daily_bars_sync_task(task: BarsSyncTask):
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


async def get_daily_bars_sync_task(sync_dt: datetime.datetime, ft: FrameType):
    """
    获取凌晨校准同步的task实例
    Args:
        sync_dt: 需要同步的时间
        ft: 日线或者分钟线

    Returns:

    """
    end = sync_dt.replace(hour=15, minute=0, microsecond=0, second=0)
    if ft == FrameType.DAY:
        n_bars = 4
        timeout = 10
    else:
        n_bars = 240 + 4
        timeout = 30

    name = "daily_bars_sync_%s" % ft.value
    frame_type = [ft]
    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_DAILY_CALIBRATION,
        name=name,
        end=end,
        frame_type=frame_type,  # 需要同步的类型
        timeout=60 * timeout,
        recs_per_sec=n_bars,
    )
    return task


@master_syncbars_task()
async def sync_daily_bars_1m():
    """scheduled task entry, for bars:1m only!

    runs at every day at 2:00 am
    """
    logger.info("每日分钟线下载启动")
    now = arrow.now().date()
    pre_trade_date = get_previous_trade_day(now)

    key_head = constants.BAR_SYNC_ARCHIVE_HEAD
    key_tail = constants.BAR_SYNC_ARCHIVE_TAIL

    async for sync_dt, head, tail in get_sync_date():
        # 当天的校准启动前，先清除缓存。
        if sync_dt.date() == pre_trade_date:
            await Stock.reset_cache()

        task = await get_daily_bars_sync_task(sync_dt, FrameType.MIN1)
        success = await run_daily_bars_sync_task(task)
        if not success:  # pragma: no cover
            break
        else:
            # 成功同步了`sync_dt`这一天的数据，更新 head 和 tail
            if head is not None:
                await cache.sys.set(key_head, head.strftime("%Y-%m-%d"))
            if tail is not None:
                await cache.sys.set(key_tail, tail.strftime("%Y-%m-%d"))
            logger.info(f"{task.name}({task.end})同步完成,参数为{task.params}")

    logger.info("exit sync_daily_bars_1m")


@master_syncbars_task()
async def sync_daily_bars_day():
    """scheduled task entry, for bars:1d only!"""
    logger.info("日线下载启动")

    key_head = constants.BAR_SYNC_DAY_HEAD
    key_tail = constants.BAR_SYNC_DAY_TAIL

    async for sync_dt, head, tail in get_sync_date(key_head, key_tail):
        task = await get_daily_bars_sync_task(sync_dt, FrameType.DAY)
        success = await run_daily_bars_sync_task(task)
        if not success:  # pragma: no cover
            break
        else:
            # 成功同步了`sync_dt`这一天的数据，更新 head 和 tail
            if head is not None:
                await cache.sys.set(key_head, head.strftime("%Y-%m-%d"))
            if tail is not None:
                await cache.sys.set(key_tail, tail.strftime("%Y-%m-%d"))

            logger.info(f"{task.name}({task.end})同步完成,参数为{task.params}")

    logger.info("exit sync_daily_bars_day")


@master_syncbars_task()
async def sync_day_bar_factors():
    """scheduled task entry, for bars:1d only!"""
    logger.info("日线修补factor启动")

    # 修补日线如果出错，采取邮件和钉钉消息双路通知，一定要补齐，否则只能由周末的检查程序扫描结果
    now = arrow.now().date()
    pre_trade_date = get_previous_trade_day(now)
    if now != pre_trade_date + datetime.timedelta(days=1):  # 是否只相邻一天
        logger.info("sync_day_bar_factors, skip non trade day: %s", now)
        return False  # 不处理

    # 检查是否有分红送股的情况
    rc = await Security.get_xrxd_info(pre_trade_date)
    if not rc:
        logger.info("sync_day_bar_factors, no xr xd info in : %s", pre_trade_date)
        return False  # 无分红送股，不用二次纠正数据（factor）

    # 重新下载日线
    sync_dt = datetime.datetime.combine(pre_trade_date, datetime.time(15, 0))
    task = await get_daily_bars_sync_task(sync_dt, FrameType.DAY)
    success = await run_daily_bars_sync_task(task)
    if success:
        logger.info("sync_day_bar_factors, success : %s", pre_trade_date)
        return True

    # 发送错误信息
    msg = f"failed to re-download bars:1d for {pre_trade_date}, check log for detailed information"
    from omicron.notify.dingtalk import DingTalkMessage

    DingTalkMessage.text(msg)

    from omicron.notify.mail import mail_notify

    subject = "failed to update factors for bars:1d"
    await mail_notify(subject=subject, body=msg)

    logger.info("exit sync_daily_bars_day")
