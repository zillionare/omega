import asyncio
import io
import logging
import os
import tarfile
from typing import List, Tuple

import aiohttp
import cfg4py
import pandas as pd
from omicron import cache
from omicron.core.types import FrameType
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

logger = logging.getLogger(__name__)

cfg = cfg4py.get_instance()


class FileHandler:
    async def process(self, stream):
        raise NotImplementedError


class ArchivedBarsHandler(FileHandler):
    def __init__(self, url: str):
        self.url = url

    async def process(self, stream):
        try:
            fileobj = io.BytesIO(await stream.read(-1))
            tar = tarfile.open(fileobj=fileobj, mode="r:gz")
            count = 0
            for member in tar.getmembers():
                f = tar.extractfile(member)
                df = pd.read_parquet(f)

                sec = member.name.split("/")[-1]

                await self.save(sec, df)
                count += 1

            year, month, cat = parse_url(self.url)
            return self.url, f"200 成功导入{year}年{month}月的{cat}数据"
        except Exception as e:
            logger.exception(e)
            return self.url, "500 导入数据失败"

    async def save(self, code: str, df: pd.DataFrame):
        pipeline = cache.security.pipeline()

        ranges = {}

        # retrieve rng from cache
        for frame_type in set(df["frame_type"]):
            key = f"{code}:{FrameType.from_int(frame_type).value}"

            head = await cache.security.hget(key, "head")
            tail = await cache.security.hget(key, "tail")
            ranges[key] = {
                "head": int(head) if head else None,
                "tail": int(tail) if tail else None,
            }

        for frame, (o, h, l, c, v, a, fq, frame_type) in df.iterrows():
            key = f"{code}:{FrameType.from_int(frame_type).value}"

            pipeline.hset(
                key, frame, f"{o:.2f} {h:.2f} {l:.2f} {c:.2f} {v} {a:.2f} {fq:.2f}"
            )
            ranges[key]["head"] = (
                min(ranges[key]["head"], frame) if ranges[key]["head"] else frame
            )
            ranges[key]["tail"] = (
                min(ranges[key]["tail"], frame) if ranges[key]["tail"] else frame
            )

        for frame_type in set(df["frame_type"]):
            key = f"{code}:{FrameType.from_int(frame_type).value}"
            pipeline.hset(key, "head", ranges[key]["head"])
            pipeline.hset(key, "tail", ranges[key]["tail"])

        await pipeline.execute()


def parse_url(url: str):
    return url.split("/")[-1].split(".")[0].split("-")


async def get_file(url: str, timeout: int = 600, handler: FileHandler = None):
    retry = 1
    timeout = aiohttp.ClientTimeout(total=timeout)
    while retry <= 3:
        logger.info("downloading file from %s", url)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as client:
                async with client.get(url) as response:
                    if response.status == 200:
                        if handler is None:
                            return url, await response.read()
                        else:
                            return await handler.process(response.content)
                    elif response.status == 404:
                        year, month, cat = parse_url(url)
                        return url, f"404 服务器上没有{year}年{month}月的{cat}数据"

        except aiohttp.ClientConnectionError as e:
            logger.warning("downloading %s failed for %sth time", url, retry)
            logger.exception(e)
            raise e

        retry += 1
        logger.info("retry downloading file from %s", url)

    return url, None


async def get_index(url: str):
    try:
        url, content = await get_file(url)
        if content is not None:
            yaml = YAML(typ="safe")
            index = yaml.load(content)
            parsed = {}
            for key in ["index", "stock"]:
                files = index[key]
                parsed[key] = {}

                for file in files:
                    month = "".join(os.path.basename(file).split("-")[:2])
                    parsed[key].update({month: file})

            return 200, parsed
    except aiohttp.ClientConnectionError as e:
        logger.exception(e)
        return 500, f"无法建立与服务器{url}的连接"
    except YAMLError as e:
        logger.exception(e)
        return 500, "无法解析索引文件"
    except Exception as e:
        logger.exception(e)
        return 500, "未知错误"


async def get_bars(server, months: List[str], cats: List[str]) -> Tuple[int, str]:
    if not server.endswith("/"):
        server += "/"
    status, response = await get_index(server + "index.yml")
    if status != 200:
        yield status, response
        yield 500, "读取索引失败，无法下载历史数据"
        return
    else:
        yield 200, "读取索引成功"

    index = response

    files = []
    for month in months:
        for cat in cats:
            file = index.get(cat, {}).get(month)
            if file is None:
                yield 200, f"服务器没有{month}的{cat}数据"
                continue
            else:
                files.append(server + file)

    if len(files) == 0:
        yield 200, "没有可以下载的数据"
        yield 200, "DONE"
        return

    tasks = [get_file(file, handler=ArchivedBarsHandler(file)) for file in files]
    for task in asyncio.as_completed(tasks):
        url, result = await task
        if result is not None:
            yield result.split(" ")

    yield 200, "DONE"


async def get_archive_index(server):
    if not server.endswith("/"):
        server += "/"

    status, index = await get_index(server + "/index.yml")
    index = {cat: list(index[cat].keys()) for cat in index.keys()}
    if status == 200:
        return index
    else:
        return None
