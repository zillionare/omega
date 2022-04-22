# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-11 10:24
import logging

logger = logging.getLogger(__name__)

TASK_PREFIX = "master.task"

# jobs.bars_sync.archive.head
BAR_SYNC_ARCHIVE_HEAD = "jobs.bars_sync.archive.head"
BAR_SYNC_ARCHIVE_TAIL = "jobs.bars_sync.archive.tail"

# prefix name of queue that stores temporal bars for minio
MINIO_TEMPORAL = "temp.minio"

# OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK
BAR_SYNC_MINUTE_TAIL = "master.bars_sync.minute.tail"
BAR_SYNC_YEAR_TAIL = "jobs.bars_sync.year.tail"
BAR_SYNC_QUARTER_TAIL = "jobs.bars_sync.quarter.tail"
BAR_SYNC_MONTH_TAIL = "jobs.bars_sync.month.tail"
BAR_SYNC_WEEK_TAIL = "jobs.bars_sync.week.tail"
BAR_SYNC_TRADE_PRICE_TAIL = "jobs.bars_sync.trade_price.tail"
BAR_SYNC_OTHER_MIN_TAIL = "jobs.bars_sync.min_5_15_30_60.tail"

TRADE_PRICE_LIMITS = "trade_price_limits"
