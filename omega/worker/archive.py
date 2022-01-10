import asyncio
import glob
import io
import logging
import os
import random
import shutil
import tarfile
import tempfile
from typing import List, Tuple

import aiohttp
import cfg4py
import fire
import omicron
import pandas as pd
from omicron import cache
from omicron.core.types import FrameType
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError
from ruamel.yaml.main import parse

from omega.config import get_config_dir

logger = logging.getLogger(__name__)

cfg = cfg4py.get_instance()


class FileHandler:
    async def process(self, stream):
        raise NotImplementedError


class ArchivedBarsHandler(FileHandler):
    def __init__(self, url: str):
        self.url = url

    async def process(self, file_content):
        extract_to = tempfile.mkdtemp(prefix="omega-archive-")
        try:
            _, (year, month, cat) = parse_url(self.url)

            fileobj = io.BytesIO(file_content)
            tar = tarfile.open(fileobj=fileobj, mode="r")
            logger.info("extracting %s into %s", self.url, extract_to)
            tar.extractall(extract_to)

            pattern = os.path.join(extract_to, "**/*.XSH?")
            for file in glob.glob(pattern, recursive=True):
                await self.save(file)

            logger.info("%s数据导入完成", self.url)
        except Exception as e:
            logger.exception(e)
            return self.url, f"500 导入数据{year}/{month}:{cat}失败"

        try:
            shutil.rmtree(extract_to)
        except Exception as e:
            logger.exception(e)
            logger.warning("failed to remove temp dir %s", extract_to)

        return self.url, f"200 成功导入{year}年{month}月的{cat}数据"

    async def save(self, file: str):
        try:
            logger.debug("saving file %s", file)
            df = pd.read_parquet(file)
            code = os.path.split(file)[-1]
            pipeline = cache.security.pipeline()
            range_pl = cache.sys.pipeline()

            for frame, (o, h, l, c, v, a, fq, frame_type) in df.iterrows():
                key = f"{code}:{FrameType.from_int(frame_type).value}"

                pipeline.hset(
                    key, frame, f"{o:.2f} {h:.2f} {l:.2f} {c:.2f} {v} {a:.2f} {fq:.2f}"
                )
                range_pl.lpush(f"archive.ranges.{key}", int(frame))

            await pipeline.execute()
            await range_pl.execute()
        except Exception as e:
            logger.info("导入%s失败", file)
            logger.exception(e)


def parse_url(url: str):
    if url.find("index.yml") != -1:
        return True, (url, None, None)

    return False, url.split("/")[-1].split(".")[0].split("-")


async def get_file(url: str, timeout: int = 1200, handler: FileHandler = None):
    timeout = aiohttp.ClientTimeout(total=timeout)
    logger.info("downloading file from %s", url)
    is_index, (year, month, cat) = parse_url(url)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.get(url) as response:
                if response.status == 200:
                    logger.info("file %s downloaded", url)
                    content = await response.read()
                    if handler is None:
                        return url, content
                    else:
                        return await handler.process(content)
                elif response.status == 404:
                    if is_index:
                        return url, "404 未找到索引文件"
                    else:
                        return url, f"404 服务器上没有{year}年{month}月的{cat}数据"
    except aiohttp.ServerTimeoutError as e:
        logger.warning("downloading %s failed", url)
        logger.exception(e)
        if is_index:
            return url, "500 下载索引文件超时"
        else:
            return url, f"500 {year}/{month}的{cat}数据下载超时"
    except Exception as e:
        logger.warning("downloading %s failed", url)
        logger.exception(e)
        if is_index:
            return url, "500 下载索引文件失败"
        else:
            return url, f"500 {year}/{month}的{cat}数据下载失败"


def parse_index(text):
    yaml = YAML(typ="safe")
    index = yaml.load(text)
    parsed = {}
    for key in ["index", "stock"]:
        files = index.get(key) or []
        parsed[key] = {}

        for file in files:
            month = "".join(os.path.basename(file).split("-")[:2])
            parsed[key].update({int(month): file})

    return parsed


async def _load_index(url: str):
    """load and parse index.yml

    Args:
        url (str): [description]

    Returns:
        [type]: [description]
    """
    try:
        url, content = await get_file(url)
        if content is not None:
            return 200, parse_index(content)
    except aiohttp.ClientConnectionError as e:
        logger.exception(e)
        return 500, f"无法建立与服务器{url}的连接"
    except YAMLError as e:
        logger.exception(e)
        return 500, "无法解析索引文件"
    except Exception as e:
        logger.exception(e)
        return 500, "未知错误"


async def get_bars(server, months: List[int], cats: List[str]) -> Tuple[int, str]:
    if not server.endswith("/"):
        server += "/"
    status, response = await _load_index(server + f"index.yml?{random.random()}")
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
                yield 404, f"服务器没有{month}的{cat}数据"
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
            status, desc = result.split(" ")
            yield int(status), desc

    yield 200, "DONE"


async def get_index(server):
    if not server.endswith("/"):
        server += "/"

    status, index = await _load_index(server + f"/index.yml?{random.random()}")
    if status != 200 or (index is None):
        return 500, None

    return 200, {cat: list(index[cat].keys()) for cat in index.keys()}


async def clear_range():
    """clear cached secs's range before/after import archive bars"""
    key = "archive.ranges.*"
    keys = await cache.sys.keys(key)

    if keys:
        await cache.sys.delete(*keys)


async def adjust_range(batch: int = 500):
    """adjust secs's range after archive bars imported"""
    cur = b"0"
    key = "archive.ranges.*"
    logger.info("start adjust range")
    while cur:
        cur, keys = await cache.sys.scan(cur, match=key, count=batch)
        if not keys:
            continue

        pl = cache.security.pipeline()
        for item in keys:
            try:
                values = [int(v) for v in await cache.sys.lrange(item, 0, -1)]
                values.sort()

                arc_head, arc_tail = values[0], values[-1]

                code_frame_key = item.replace("archive.ranges.", "")
                head, tail = await cache.security.hmget(code_frame_key, "head", "tail")

                head = int(head) if head is not None else None
                tail = int(tail) if tail is not None else None

                # head, tail, arc_head, arc_tail should be all frame-aligned
                if head is None or tail is None:
                    head, tail = arc_head, arc_tail
                elif arc_tail < head or arc_head > tail:
                    head, tail = arc_head, arc_tail
                else:
                    head = min(arc_head, head)
                    tail = max(arc_tail, tail)
                pl.hset(code_frame_key, "head", head)
                pl.hset(code_frame_key, "tail", tail)
            except Exception as e:
                logger.exception(e)
                logger.warning("failed to set range for %s", code_frame_key)

        await pl.execute()


async def _main(months: list, cats: list):
    await omicron.init()

    try:
        async for status, desc in get_bars(cfg.omega.urls.archive, months, cats):
            print(status, desc)
    finally:
        await omicron.shutdown()


def main(months: str, cats: str, archive_server: str = None):
    """允许将本模块以独立进程运行，以支持多进程

    Args:
        months (str): 逗号分隔的月列表。格式如202012
        cats (str): 逗号分隔的类别列表，如"stock,index"
    """
    config_dir = get_config_dir()
    cfg = cfg4py.init(config_dir, False)

    if archive_server:
        cfg.omega.urls.archive = archive_server

    months = str(months)
    months = [int(x) for x in months.split(",") if x]
    cats = [x for x in cats.split(",")]

    asyncio.run(_main(months, cats))


if __name__ == "__main__":
    fire.Fire({"main": main})
