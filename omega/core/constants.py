# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-11 10:24
import logging

logger = logging.getLogger(__name__)

TASK_PREFIX = "master.task"

BAR_SYNC_STATE_MINUTE = "master.bars_sync.state.minute"

# jobs.bars_sync.archive.head
BAR_SYNC_ARCHIVE_HEAD = "jobs.bars_sync.archive.head"
BAR_SYNC_ARCHIVE_TAIl = "jobs.bars_sync.archive.tail"


# OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK
BAR_SYNC_YEAR_TAIl = "jobs.bars_sync.year.tail"
BAR_SYNC_QUARTER_TAIl = "jobs.bars_sync.quarter.tail"
BAR_SYNC_MONTH_TAIl = "jobs.bars_sync.month.tail"
BAR_SYNC_WEEK_TAIl = "jobs.bars_sync.week.tail"

HIGH_LOW_LIMIT = "high_low_limit"
