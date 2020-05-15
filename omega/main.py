#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    管理应用程序生命期、全局对象、任务、全局消息响应
        """
import asyncio
import logging
import platform

from omega import app_name
from omega.app import Application

logger = logging.getLogger(__name__)


async def main():
    app = Application()
    await app.start()


if __name__ == "__main__":
    logger.info("starting zillionare %s main process...", app_name)
    if platform.system() in "Linux":
        try:
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ModuleNotFoundError:
            logger.warning(
                'uvloop is required for better performance, continuing with '
                'degraded '
                'service.')

    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
