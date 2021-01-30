import json
import logging

import cfg4py
from sanic import Blueprint, response

from omega.fetcher import archive
from omega.interfaces.websockets.base import WebSocketSession

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class ArchiveSession(WebSocketSession):
    __name__ = "archive websocket session"

    async def on_message(self, msg: str):
        logger.info("received %s", msg)
        msg = json.loads(msg)

        if msg.get("request") == "index":
            result = await archive.get_archive_index(cfg.omega.urls.archive)
            await self.send_message(json.dumps(result))
        elif msg.get("request") == "bars":
            params = msg.get("params")
            months = params.get("months")
            cats = params.get("cats")

            async for result in archive.get_bars(cfg.omega.urls.archive, months, cats):
                logger.info(result)
                await self.send_message(f"{result[0]} {result[1]}")
        else:
            await self.send_message("400 错误的请求")
