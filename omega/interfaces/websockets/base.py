import asyncio
import dataclasses
import json
import logging
from typing import Any, Dict, Optional, Union

import cfg4py
from websockets.exceptions import ConnectionClosedError
from websockets.protocol import WebSocketCommonProtocol

logger = logging.getLogger()


class WebSocketSession:
    __name__ = "websocket"

    def __init__(self):
        self.connection: WebSocketCommonProtocol = None

    async def on_connect(self, request):
        logger.debug("on_connect: %s", request)

    async def on_close(self):
        pass

    async def close(self):
        await self.connection.close()

    async def on_message(self, msg):
        raise NotImplementedError

    async def send_message(self, msg: Union[bytes, str, Dict[str, Any]]):
        await self.connection.send(msg)

    async def __call__(self, request, ws: WebSocketCommonProtocol):
        self.connection = ws

        await self.on_connect(request)
        try:
            while True:
                msg = await self.connection.recv()
                logger.debug("received msg: %s", msg)
                asyncio.create_task(self.on_message(msg))
        except ConnectionClosedError:
            await self.on_close()
        except Exception as e:
            logger.exception(e)

    def register(self, app, route):
        return app.add_websocket_route(self, route)
