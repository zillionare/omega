# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-07 18:28
import datetime
import logging
import traceback
from functools import wraps

import arrow
import cfg4py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron import cache
from omicron.models.stock import Security
from omicron.models.timeframe import TimeFrame
from omicron.notify.mail import mail_notify

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as fetcher

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


def cron_work_report():
    def inner(f):
        @wraps(f)
        async def decorated_function():
            key = f"""cron_{f.__name__}"""
            if await cache.sys.setnx(key, 1):
                await cache.sys.setex(key, 3600 * 2, 1)
                try:
                    ret = await f()
                    return ret
                except Exception as e:  # pragma: no cover
                    logger.exception(e)
                    subject = f"exception in cron job task: {f.__name__}"
                    body = f"detailed information: \n{traceback.format_exc()}"
                    await mail_notify(subject, body, html=True)
                finally:
                    await cache.sys.delete(key)

        return decorated_function

    return inner


@cron_work_report()
async def sync_calendar():
    """从上游服务器获取所有交易日，并计算出周线帧和月线帧

    Returns:
    """
    trade_days = await fetcher.get_all_trade_days()
    if trade_days is None or len(trade_days) == 0:
        logger.warning("failed to fetch trade days.")
        return None

    await TimeFrame.init()


@cron_work_report()
async def sync_security_list_today():
    now = datetime.datetime.now()
    if not TimeFrame.is_trade_day(now):
        logger.info("skip non trade day: %s", now)
        return False

    securities = await fetcher.get_security_list(now.date())
    if securities is None or len(securities) < 100:
        msg = "failed to get security list(%s)" % now.strftime("%Y-%m-%d")
        logger.error(msg)
        raise Exception(msg)

    # 更新今天的缓存数据
    logger.info("save security data into cache: %s", now.strftime("%Y-%m-%d"))
    await Security.update_secs_cache(now.date(), securities)
    return len(securities)


async def load_cron_task(scheduler: AsyncIOScheduler):
    scheduler.add_job(
        sync_calendar,  # 默认1点更新一次全部数据
        "cron",
        hour=1,
        minute=2,
        name="sync_calendar",
    )

    scheduler.add_job(
        sync_security_list_today,  # 8点更新今日的证券列表
        "cron",
        hour=8,
        minute=8,
        name="sync_seclist_today",
    )
