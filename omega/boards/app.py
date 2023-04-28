# -*- coding: utf-8 -*-
import asyncio
import datetime
import logging
import os
import sys
import time

import cfg4py
import omicron
from omicron.models.timeframe import TimeFrame
from omicron.notify.dingtalk import ding

from omega.boards.server import (
    boards_init,
    fetch_board_members,
    fetch_concept_day_bars,
    fetch_industry_day_bars,
    sync_board_names,
)
from omega.config import get_config_dir

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


class AKShareFetcher(object):
    async def init(self):
        cfg4py.init(get_config_dir(), False)

        try:
            await omicron.init()
        except Exception as e:
            print(
                "init failed, make sure you have calendar and securities data in store: %s",
                str(e),
            )
            time.sleep(5)
            os._exit(1)

    async def close(self):
        await omicron.close()

    async def fetch_day_bars(self, _type: str):
        # 显式初始化一次存储对象
        boards_init()

        now = datetime.datetime.now()
        if not TimeFrame.is_trade_day(now):
            return False

        dt = TimeFrame.day_shift(now, 0)

        if _type == "industry":
            # get day bars for industry items
            await fetch_industry_day_bars(dt)
        else:
            # get day bars for concept items
            await fetch_concept_day_bars(dt)

        return True

    async def fetch_members(self, _type: str):
        # 显式初始化一次存储对象
        boards_init()

        now = datetime.datetime.now()
        if not TimeFrame.is_trade_day(now):
            return False

        # sync board name first
        rc = sync_board_names(_type)
        if not rc:
            ding("sync %s board names failed." % _type)
            return False

        # get day bars for board items
        rc = await fetch_board_members(_type)
        if not rc:
            ding(f"fetch members for {_type} failed")
            return False

        return True


async def main_task(action: str):
    fetcher = AKShareFetcher()
    await fetcher.init()

    if action == "sync_industry_bars":
        await fetcher.fetch_day_bars("industry")
    elif action == "sync_concept_bars":
        await fetcher.fetch_day_bars("concept")
    elif action == "sync_industry_list":
        await fetcher.fetch_members("industry")
    elif action == "sync_concept_list":
        await fetcher.fetch_members("concept")
    else:
        logger.info("action: %s not supported", action)

    await fetcher.close()


def board_task_entry(action: str):
    logger.info("board_task_entry, action: %s", action)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main_task(action))

    logger.info("board_task_entry, action: %s, finished", action)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        _str = sys.argv[1]
        board_task_entry(_str)
    else:
        print("no action specified.")
