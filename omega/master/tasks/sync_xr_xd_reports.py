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

logger = logging.getLogger(__name__)


async def run_xrxd_sync_task(task: SecuritySyncTask):
    ret = await task.run()
    if not ret:
        return False

    return True


async def get_xrxd_sync_task(sync_dt: datetime.datetime):
    name = "xrxd_reports_sync"
    task = SecuritySyncTask(
        event=Events.OMEGA_DO_SYNC_XRXD_REPORTS,
        name=name,
        end=sync_dt,
        timeout=60 * 2,  # 1年内的数据，大约数秒
        recs_per_task=5000,  # 2021年12月31日的年报，大约1500条数据
    )
    return task


@master_secs_task()
async def sync_xrxd_reports():
    """同步上市公司分红送股数据，聚宽每天8点更新，本程序取前一天的数据"""

    now = datetime.datetime.now()
    today = now.date()
    if not TimeFrame.is_trade_day(today):
        return True

    logger.info("sync_xrxd_reports starts")

    task = await get_xrxd_sync_task(today)

    success = await run_xrxd_sync_task(task)
    if not success:  # pragma: no cover
        logger.error(f"{task.name}({task.end}), task failed, params: {task.params}")
    else:
        logger.info(f"{task.name}({task.end}), task finished, params: {task.params}")

    logger.info("sync_xrxd_reports ends")
    return success


@master_secs_task()
async def sync_all_xrxd_reports():
    """同步上市公司分红送股数据，聚宽每天8点更新，本程序取前一天的数据"""

    logger.info("sync_all_xrxd_reports starts")

    dt = datetime.date(2005, 6, 15)  # 开始日期
    while dt.year < 2023:
        task = await get_xrxd_sync_task(dt)
        success = await run_xrxd_sync_task(task)
        if not success:  # pragma: no cover
            logger.error(f"{task.name}({task.end}), task failed, params: {task.params}")
        else:
            logger.info(
                f"{task.name}({task.end}), task finished, params: {task.params}"
            )

        next_year = dt.year + 1
        dt = dt.replace(year=next_year)

    logger.info("sync_xrxd_reports ends")
    return True
