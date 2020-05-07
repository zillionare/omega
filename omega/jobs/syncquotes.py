#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging

from omicron.core import FrameType

from omega.app import Application as app
from omicron.models.securities import Securities

logger = logging.getLogger(__name__)

async def init_sync_scope(frame_type: FrameType):
    pass

async def sync_all_bars():
    pass


async def sync_day_bars():
    if app.is_leader:
        await init_sync_scope(FrameType.DAY)
