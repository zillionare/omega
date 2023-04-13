# -*- coding: utf-8 -*-
import asyncio
import logging
import os
import sys

import cfg4py

from omega.akshareprice.datasync import init_redis_connection
from omega.akshareprice.job import start_cron_task, start_cron_task_idx
from omega.config import get_config_dir

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


def init_config():
    config_dir = get_config_dir()
    print("config dir:", config_dir)

    try:
        cfg4py.init(config_dir, False)
    except Exception as e:
        print(e)
        os._exit(1)


def run(action: str):
    logger.info("AKShare stock/index price fetcher (%s) starting...", action)

    init_config()
    init_redis_connection()

    loop = asyncio.get_event_loop()

    if action == "stock":
        loop.run_until_complete(start_cron_task())
        loop.run_forever()
    elif action == "index":
        loop.run_until_complete(start_cron_task_idx())
        loop.run_forever()
    else:
        logger.info("action: %s not supported", action)

    logger.info("Stock/index price fetcher process, action: %s, finished", action)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        _str = sys.argv[1]
        run(_str)
    else:
        print("no action specified.")
