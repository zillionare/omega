# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-11 10:24
import logging

logger = logging.getLogger(__name__)

BAR_SYNC_STATE = "master.bars_sync.state"
BAR_SYNC_DONE = "master.bars_sync.done"
BAR_SYNC_SCOPE = "master.bars_sync.scope"

# jobs.bars_sync.archive.head
BAR_SYNC_ARCHIVE_HEAD = "jobs.bars_sync.archive.head"
BAR_SYNC_ARCHIVE_TAIl = "jobs.bars_sync.archive.tail"

HIGH_LOW_LIMIT = "high_low_limit"


def get_queue_name(suffix):
    return (
        f"{BAR_SYNC_STATE}.{suffix}",
        f"{BAR_SYNC_DONE}.{suffix}",
        f"{BAR_SYNC_SCOPE}.{suffix}",
    )
