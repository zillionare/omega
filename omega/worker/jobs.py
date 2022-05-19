# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-07 18:28
import datetime
import logging
import traceback
from functools import wraps

import cfg4py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron import cache
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
async def sync_funds():
    """更新基金列表"""
    secs = await fetcher.get_fund_list()
    logger.info("%s secs are fetched and saved.", len(secs))
    return secs


@cron_work_report()
async def sync_fund_net_value():
    """更新基金净值数据"""
    now = datetime.datetime.now().date()
    ndays = 8
    n = 0
    while n < ndays:
        await fetcher.get_fund_net_value(day=now - datetime.timedelta(days=n))
        n += 1
        if n > 2:
            break
    return True


@cron_work_report()
async def sync_fund_share_daily():
    """更新基金份额数据"""
    now = datetime.datetime.now().date()
    ndays = 8
    n = 0
    while n < ndays:
        await fetcher.get_fund_share_daily(day=now - datetime.timedelta(days=n))
        n += 1
    return True


@cron_work_report()
async def sync_fund_portfolio_stock():
    """更新基金十大持仓股数据"""
    now = datetime.datetime.now().date()
    ndays = 8
    n = 0
    while n < ndays:
        await fetcher.get_fund_portfolio_stock(
            pub_date=now - datetime.timedelta(days=n)
        )
        n += 1
    return True


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


async def load_cron_task(scheduler: AsyncIOScheduler):
    h, m = map(int, cfg.omega.sync.security_list.split(":"))
    scheduler.add_job(
        sync_calendar,  # 默认1点更新一次全部数据
        "cron",
        hour=h,
        minute=m,
        name="sync_calendar",
    )
    scheduler.add_job(
        sync_fund_net_value,
        "cron",
        hour=4,
        minute=15,
        name="sync_fund_net_value",
    )
    scheduler.add_job(
        sync_funds,
        "cron",
        hour=4,
        minute=0,
        name="sync_funds",
    )
    scheduler.add_job(
        sync_fund_share_daily,
        "cron",
        hour=4,
        minute=5,
        name="sync_fund_share_daily",
    )
    scheduler.add_job(
        sync_fund_portfolio_stock,
        "cron",
        hour=4,
        minute=10,
        name="sync_fund_portfolio_stock",
    )
