import datetime
import logging
from functools import wraps

import cfg4py
from omicron import cache
from omicron.models.timeframe import TimeFrame
from omicron.notify.mail import mail_notify

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as fetcher

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


async def sync_funds():
    """更新基金列表"""
    secs = await fetcher.get_fund_list()
    logger.info("%s secs are fetched and saved.", len(secs))
    return secs


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


async def sync_fund_share_daily():
    """更新基金份额数据"""
    now = datetime.datetime.now().date()
    ndays = 8
    n = 0
    while n < ndays:
        await fetcher.get_fund_share_daily(day=now - datetime.timedelta(days=n))
        n += 1
    return True


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
