import asyncio
import datetime
import json
import os
import re
from typing import Union

import aioredis


class RedisLogReceiver:
    def __init__(
        self,
        dsn: str,
        channel_name: str,
        filename: str,
        backup_count: int = 7,
        max_bytes: Union[str, int] = 10 * 1024 * 1024,
        fmt: str = None,
    ):
        self._dsn = dsn
        self._channel_name = channel_name
        self._backup_count = backup_count
        self._max_bytes = self.parse_max_bytes(max_bytes)
        self._dir = os.path.dirname(filename)
        if not os.path.exists(self._dir):
            os.makedirs(self._dir)

        self._filename = os.path.split(filename)[-1]
        self._fmt = (
            fmt
            or "%(asctime)s %(levelname)-1.1s %(process)d %(name)s:%(funcName)s:%(lineno)s | %(message)s"
        )

        # the file handler to save log messages
        self._fh = open(filename, mode="a", encoding="utf-8", buffering=1)

        # bytes written. to decide when to rotate files
        self._written_bytes = os.path.getsize(filename)

        # the redis connection
        self._redis = None

        # the channel returned by redis.subscribe
        self._channel = None

        # the loop for listen and dump log
        self._reader_task = None

    def rotate(self):
        self._fh.flush()
        self._fh.close()
        self._fh = None

        err_msg = None
        try:
            files = []
            for file in os.listdir(self._dir):
                if file.startswith(self._filename):
                    files.append(file)
            files.sort()
            for file in files[::-1]:
                old_file = os.path.join(self._dir, file)
                matched = re.match(fr"{self._filename}\.(\d+)", file)
                if matched:
                    seq = int(matched.group(1))
                    if seq + 1 > self._backup_count:
                        continue
                else:
                    seq = 0

                new_file = os.path.join(self._dir, f"{self._filename}.{seq+1}")
                if os.path.exists(new_file):
                    os.remove(new_file)
                os.rename(old_file, new_file)
        except Exception as e:
            err_msg = str(e)

        filename = os.path.join(self._dir, self._filename)
        self._fh = open(filename, mode="a", encoding="utf-8", buffering=1)
        self._written_bytes = 0

        if err_msg:
            self._fh.write(err_msg)

    def _write(self, msg: str):
        self._written_bytes += len(msg)
        if (1 + self._written_bytes) % 4096 == 0:
            self._fh.flush()

        if self._written_bytes > self._max_bytes:
            self.rotate()

        if self._fh is None:
            print(msg)
        else:
            self._fh.write(msg)
            self._fh.write("\n")

    async def stop(self):
        self._fh.flush()
        self._fh.close()
        self._fh = None

        await self._redis.unsubscribe(self._channel_name)
        await self._reader_task
        self._redis.close()

    async def start(self):
        self._redis = await aioredis.create_redis(self._dsn)
        res = await self._redis.subscribe(self._channel_name)
        self._channel = res[0]
        self._reader_task = asyncio.create_task(self.reader())

    async def reader(self):
        while await self._channel.wait_message():
            msg = (await self._channel.get()).decode("utf-8")
            self._write(msg)

    @staticmethod
    def parse_max_bytes(max_bytes: Union[str, int]):
        if isinstance(max_bytes, str):
            size, unit = re.match(r"([.\d]+)([MK])", max_bytes.upper()).groups()
            if unit == "M":
                max_bytes = float(size) * 1024 * 1024
            elif unit == "K":
                max_bytes = float(size) * 1024
            else:  # pragma: no cover
                raise ValueError(f"{max_bytes} is not parsable")
        elif isinstance(max_bytes, int):
            pass
        else:  # pragma: no cover
            raise ValueError(f"type of max_bytes({type(max_bytes)}) is not supported.")

        return max_bytes
