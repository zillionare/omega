#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import asyncio
import datetime
import json
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

import aiohttp
import arrow
import cfg4py
import omicron
import psutil
import xxhash
from aiocache import cached
from aiohttp import ClientError
from dateutil import tz
from omicron import cache
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.models.securities import Securities
from omicron.models.security import Security
from pyemit import emit

from omega.config import get_config_dir
from omega.core.events import Events, ValidationError

validation_errors = []
cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


async def calc_checksums(day: datetime.date, codes: List) -> dict:
    """
    Args:
        day:
        codes:

    Returns:
        返回值为以code为键，该证券对应的{周期：checksum}的集合为值的集合
    """
    end_time = arrow.get(day, tzinfo=cfg.tz).replace(hour=15)

    checksums = {}
    for i, code in enumerate(codes):
        try:
            checksum = {}
            d = await cache.get_bars_raw_data(code, day, 1, FrameType.DAY)
            if d:
                checksum[f"{FrameType.DAY.value}"] = xxhash.xxh32_hexdigest(d)
            d = await cache.get_bars_raw_data(code, end_time, 240, FrameType.MIN1)
            if d:
                checksum[f"{FrameType.MIN1.value}"] = xxhash.xxh32_hexdigest(d)

            d = await cache.get_bars_raw_data(code, end_time, 48, FrameType.MIN5)
            if d:
                checksum[f"{FrameType.MIN5.value}"] = xxhash.xxh32_hexdigest(d)

            d = await cache.get_bars_raw_data(code, end_time, 16, FrameType.MIN15)
            if d:
                checksum[f"{FrameType.MIN15.value}"] = xxhash.xxh32_hexdigest(d)

            d = await cache.get_bars_raw_data(code, end_time, 8, FrameType.MIN30)
            if d:
                checksum[f"{FrameType.MIN30.value}"] = xxhash.xxh32_hexdigest(d)

            d = await cache.get_bars_raw_data(code, end_time, 4, FrameType.MIN60)
            if d:
                checksum[f"{FrameType.MIN60.value}"] = xxhash.xxh32_hexdigest(d)

            checksums[code] = checksum
        except Exception as e:
            logger.exception(e)

        if (i + 1) % 500 == 0:
            logger.info("calc checksum progress: %s/%s", i + 1, len(codes))

    return checksums


@cached(ttl=3600)
async def get_checksum(day: int) -> Optional[List]:
    save_to = (Path(cfg.omega.home) / "data/chksum").expanduser()
    chksum_file = os.path.join(save_to, f"chksum-{day}.json")
    try:
        with open(chksum_file, "r") as f:
            return json.load(f)
    except (FileNotFoundError, Exception):
        pass

    url = cfg.omega.urls.checksum + f"/chksum-{day}.json"
    async with aiohttp.ClientSession() as client:
        for i in range(3):
            try:
                async with client.get(url) as resp:
                    if resp.status != 200:
                        logger.warning("failed to fetch checksum from %s", url)
                        return None

                    checksum = await resp.json(encoding="utf-8")
                    with open(chksum_file, "w+") as f:
                        json.dump(checksum, f, indent=2)

                    return checksum
            except ClientError:
                continue


def do_validation_process_entry():
    try:
        t0 = time.time()
        asyncio.run(do_validation())
        logger.info("validation finished in %s seconds", time.time() - t0)
        return 0
    except Exception as e:
        logger.warning("validation exit due to exception:")
        logger.exception(e)
        return -1


async def do_validation(secs: List[str] = None, start: str = None, end: str = None):
    """对列表secs中指定的证券行情数据按start到end指定的时间范围进行校验

    Args:
        secs (List[str], optional): [description]. Defaults to None.
        start (str, optional): [description]. Defaults to None.
        end (str, optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """
    logger.info("start validation...")
    report = logging.getLogger("validation_report")

    cfg = cfg4py.init(get_config_dir(), False)

    await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
    await omicron.init()
    start = int(start or await cache.sys.get("jobs.bars_validation.range.start"))
    if end is None:
        end = tf.date2int(arrow.now().date())
    else:
        end = int(end or await cache.sys.get("jobs.bars_validation.range.stop"))

    if secs is None:

        async def get_sec():
            return await cache.sys.lpop("jobs.bars_validation.scope")

    else:

        async def get_sec():
            return secs.pop() if len(secs) else None

    errors = 0
    while code := await get_sec():
        try:
            for day in tf.day_frames[(tf.day_frames >= start) & (tf.day_frames <= end)]:
                expected = await get_checksum(day)
                if expected and expected.get(code):
                    actual = await calc_checksums(tf.int2date(day), [code])
                    d1 = actual.get(code)
                    d2 = expected.get(code)

                    missing1 = d2.keys() - d1  # local has no checksum
                    missing2 = d1.keys() - d2  # remote has no checksum
                    mismatch = {k for k in d1.keys() & d2 if d1[k] != d2[k]}

                    for k in missing1:
                        info = (
                            ValidationError.LOCAL_MISS,
                            day,
                            code,
                            k,
                            d1.get(k),
                            d2.get(k),
                        )
                        report.info("%s,%s,%s,%s,%s,%s", *info)
                        await emit.emit(Events.OMEGA_VALIDATION_ERROR, info)
                    for k in missing2:
                        info = (
                            ValidationError.REMOTE_MISS,
                            day,
                            code,
                            k,
                            d1.get(k),
                            d2.get(k),
                        )
                        report.info("%s,%s,%s,%s,%s,%s", *info)
                        await emit.emit(Events.OMEGA_VALIDATION_ERROR, info)
                    for k in mismatch:
                        info = (
                            ValidationError.MISMATCH,
                            day,
                            code,
                            k,
                            d1.get(k),
                            d2.get(k),
                        )
                        report.info("%s,%s,%s,%s,%s,%s", *info)
                        await emit.emit(Events.OMEGA_VALIDATION_ERROR, info)

                else:
                    logger.error("checksum for %s not found.", day)
                    info = (ValidationError.NO_CHECKSUM, day, None, None, None, None)
                    report.info("%s,%s,%s,%s,%s,%s", *info)
                    await emit.emit(Events.OMEGA_VALIDATION_ERROR, info)
        except Exception as e:
            logger.exception(e)
            errors += 1

    await emit.emit(Events.OMEGA_VALIDATION_ERROR, (ValidationError.UNKNOWN, errors))
    logger.warning("do_validation meet %s unknown errors", errors)


async def on_validation_error(report: tuple):
    """
    Args:
        report: object like ::(reason, day, code, frame, local, remote)

    Returns:

    """
    global validation_errors, no_validation_error_days

    # todo: raise no checksum issue
    if report[0] == ValidationError.UNKNOWN:
        no_validation_error_days = set()
    else:
        validation_errors.append(report)
        if report[1] is not None:
            no_validation_error_days -= {report[1]}


async def start_validation():
    """
    将待校验的证券按CPU个数均匀划分，创建与CPU个数相同的子进程来执行校验。校验的起始时间由数据
    库中jobs.bars_validation.range.start和jobs.bars_validation.range.stop来决定，每次校验
    结束后，将jobs.bars_validation.range.start更新为校验截止的最后交易日。如果各个子进程报告
    的截止交易日不一样（比如发生了异常），则使用最小的交易日。
    """
    global validation_errors, no_validation_error_days
    validation_errors = []

    secs = Securities()

    cpu_count = psutil.cpu_count()

    # to check if the range is right
    pl = cache.sys.pipeline()
    pl.get("jobs.bars_validation.range.start")
    pl.get("jobs.bars_validation.range.end")
    start, end = await pl.execute()

    if start is None:
        if cfg.omega.validation.start is None:
            logger.warning("start of validation is not specified, validation aborted.")
            return
        else:
            start = tf.date2int(arrow.get(cfg.omega.validation.start))
    else:
        start = int(start)

    if end is None:
        end = tf.date2int(tf.floor(arrow.now().date(), FrameType.DAY))
    else:
        end = int(end)

    assert start <= end

    no_validation_error_days = set(
        tf.day_frames[(tf.day_frames >= start) & (tf.day_frames <= end)]
    )

    # fixme: do validation per frame_type
    # fixme: test fail. Rewrite this before 0.6 releases
    codes = secs.choose(cfg.omega.sync)
    await cache.sys.delete("jobs.bars_validation.scope")
    await cache.sys.lpush("jobs.bars_validation.scope", *codes)

    logger.info("start validation %s secs from %s to %s.", len(codes), start, end)
    emit.register(Events.OMEGA_VALIDATION_ERROR, on_validation_error)

    t0 = time.time()

    code = (
        "from omega.core.sanity import do_validation_process_entry; "
        "do_validation_process_entry()"
    )

    procs = []
    for i in range(cpu_count):
        proc = subprocess.Popen([sys.executable, "-c", code], env=os.environ)
        procs.append(proc)

    timeout = 3600
    while timeout > 0:
        await asyncio.sleep(2)
        timeout -= 2
        for proc in procs:
            proc.poll()

        if all([proc.returncode is not None for proc in procs]):
            break

    if timeout <= 0:
        for proc in procs:
            try:
                os.kill(proc.pid, signal.SIGTERM)
            except Exception:
                pass

    # set next start point
    validation_days = set(
        tf.day_frames[(tf.day_frames >= start) & (tf.day_frames <= end)]
    )
    diff = validation_days - no_validation_error_days
    if len(diff):
        last_no_error_day = min(diff)
    else:
        last_no_error_day = end

    await cache.sys.set("jobs.bars_validation.range.start", last_no_error_day)
    elapsed = time.time() - t0
    logger.info(
        "Validation cost %s seconds, validation will start at %s next time",
        elapsed,
        last_no_error_day,
    )


async def quick_scan():
    # fixme
    secs = Securities()

    report = logging.getLogger("quickscan")

    counters = {}
    for sync_config in cfg.omega.sync.bars:
        frame = sync_config.get("frame")
        start = sync_config.get("start")

        if frame is None or start is None:
            logger.warning(
                "skipped %s: required fields are [frame, start]", sync_config
            )
            continue

        frame_type = FrameType(frame)

        start = arrow.get(start).date()
        start = tf.floor(start, FrameType.DAY)

        stop = sync_config.get("stop") or arrow.now().date()

        if frame_type in tf.minute_level_frames:
            minutes = tf.ticks[frame_type][0]
            h, m = minutes // 60, minutes % 60
            start = datetime.datetime(
                start.year, start.month, start.day, h, m, tzinfo=tz.gettz(cfg.tz)
            )

            stop = datetime.datetime(
                stop.year, stop.month, stop.day, 15, tzinfo=tz.gettz(cfg.tz)
            )

        counters[frame] = [0, 0]

        codes = secs.choose(sync_config.get("type"))
        include = filter(lambda x: x, sync_config.get("include", "").split(","))
        include = map(lambda x: x.strip(" "), include)
        codes.extend(include)
        exclude = sync_config.get("exclude", "")
        exclude = map(lambda x: x.strip(" "), exclude)
        codes = set(codes) - set(exclude)

        counters[frame][1] = len(codes)
        for code in codes:
            head, tail = await cache.get_bars_range(code, frame_type)
            if head is None or tail is None:
                report.info("ENOSYNC,%s,%s", code, frame)
                counters[frame][0] = counters[frame][0] + 1
                continue

            expected = tf.count_frames(head, tail, frame_type)

            # 'head', 'tail' should be excluded
            actual = (await cache.security.hlen(f"{code}:{frame_type.value}")) - 2
            if actual != expected:
                report.info(
                    "ELEN,%s,%s,%s,%s,%s,%s", code, frame, expected, actual, head, tail
                )
                counters[frame][0] = counters[frame][0] + 1
                continue

            sec = Security(code)
            if start != head:
                if (
                    type(start) == datetime.date
                    and start > sec.ipo_date
                    or (
                        type(start) == datetime.datetime and start.date() > sec.ipo_date
                    )
                ):
                    report.info(
                        "ESTART,%s,%s,%s,%s,%s", code, frame, start, head, sec.ipo_date
                    )
                    counters[frame][0] = counters[frame][0] + 1
                    continue
            if tail != stop:
                report.info("EEND,%s,%s,%s,%s", code, frame, stop, tail)
                counters[frame][0] = counters[frame][0] + 1

    return counters
