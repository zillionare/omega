#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import datetime
import logging
import os
from pathlib import Path
from typing import Optional, Union, List

import aiohttp
import arrow
import cfg4py
import sh
import xxhash
from arrow import Arrow
from omicron.core.errors import FetcherQuotaError
from omicron.core.events import Events
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.dal import cache
from omicron.dal import security_cache as sc
from omicron.models.securities import Securities
from pyemit import emit

from omega import app_name
from omega.config.cfg4py_auto_gen import Config
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq

logger = logging.getLogger(__name__)

cfg: Config = cfg4py.get_instance()


async def start_job_timer(job_name: str):
    key_start = f"jobs.bars_{job_name}.start"
    key_stop = f"jobs.bars_{job_name}.stop"
    key_elapsed = f"jobs.bars_{job_name}.elapsed"

    pl = cache.sys.pipeline()
    pl.delete(key_stop)
    pl.delete(key_elapsed)

    pl.set(key_start, arrow.now(tz=cfg.tz).format('YYYY-MM-DD HH:mm:ss'))
    await pl.execute()


async def stop_job_timer(job_name: str) -> int:
    """
    stop timer, and return elapsed time in seconds
    Args:
        job_name:

    Returns:

    """
    key_start = f"jobs.bars_{job_name}.start"
    key_stop = f"jobs.bars_{job_name}.stop"
    key_elapsed = f"jobs.bars_{job_name}.elapsed"

    start = arrow.get(await cache.sys.get(key_start), tzinfo=cfg.tz)
    stop = arrow.now(tz=cfg.tz)
    elapsed = (stop - start).seconds

    pl = cache.sys.pipeline()
    pl.set(key_stop, stop.format('YYYY-MM-DD HH:mm:ss'))
    pl.set(key_elapsed, elapsed)
    await pl.execute()

    return elapsed


async def start_job(job: str):
    key_scope = f"jobs.bars_{job}.scope"

    secs = Securities()
    await cache.sys.delete(key_scope)

    codes = secs.choose(cfg.omega.sync.type)
    logger.info("add %s securities into %s queue", job, len(codes))
    pl = cache.sys.pipeline()
    pl.delete(key_scope)

    pl.lpush(key_scope, *codes)
    await pl.execute()

    await start_job_timer(job)
    if job == 'validation':
        await emit.emit(Events.OMEGA_DO_VALIDATION, exchange=app_name)
    elif job == 'sync':
        await emit.emit(Events.OMEGA_DO_SYNC, exchange=app_name)


async def do_sync(sync_to: Arrow = None):
    """
    worker's sync job
    Returns:

    """
    logger.info("received %s signal", Events.OMEGA_DO_SYNC)
    key_scope = "jobs.bars_sync.scope"

    if sync_to is None:
        today = arrow.now().date()
        sync_to = arrow.get(today).replace(hour=15)

    # noinspection PyPep8
    while code := await cache.sys.lpop(key_scope):
        try:
            await sync_for_sec(code, sync_to)
        except FetcherQuotaError as e:
            logger.warning("When syncing %s, quota is reached", code)
            logger.exception(e)
            return  # stop the sync
        except Exception as e:
            logger.warning("Failed to sync %s", code)
            logger.exception(e)

    elapsed = await stop_job_timer('sync')
    logger.info('%s finished quotes sync in %s seconds', os.getpid(), elapsed)


async def sync_for_sec(code: str, end: Arrow):
    frames = {}
    for item in cfg.omega.sync.frames:
        frames.update(**item)

    counters = {frame: 0 for frame in frames.keys()}
    logger.info("syncing quotes for %s", code)

    for frame, max_bars in frames.items():
        frame_type = FrameType(frame)
        head, tail = await sc.get_bars_range(code, frame_type)
        if not all([head, tail]):
            await sc.clear_bars_range(code, frame_type)
            bars = await aq.get_bars(code, end, max_bars, frame_type)
            counters[frame_type.value] = len(bars)
            logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                         frame_type, code, end, max_bars, len(bars))
            continue

        start = tf.shift(end, -max_bars + 1, frame_type)
        if start < head:
            n = tf.count_frames(start, head, frame_type) - 1
            if n > 0:
                _end_at = tf.shift(head, -1, frame_type)
                bars = await aq.get_bars(code, _end_at, n, frame_type)
                counters[frame_type.value] += len(bars)
                logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                             frame_type, code, _end_at, n, len(bars))
                if bars['frame'][-1] != _end_at:
                    logger.warning("incontinuous frames found: bars[-1](%s), "
                                   "head(%s)", bars['frame'][-1], head)

        if tf.shift(end, 0, frame_type) > tail:
            n = tf.count_frames(tail, end, frame_type) - 1
            if n > 0:
                bars = await aq.get_bars(code, end, n, frame_type)
                logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                             frame_type, code, end, n, len(bars))
                counters[frame_type.value] += len(bars)
                if bars['frame'][0] != tf.shift(tail, 1, frame_type):
                    logger.warning("incontinuous frames found: tail(%s), bars[0]("
                                   "%s)", tail, bars['frame'][0])

    logger.info("finished sync %s, %s", code,
                ",".join([f"{k}:{v}" for k, v in counters.items()]))


def get_start_frame(frame_type: FrameType) -> Union[datetime.date, datetime.datetime]:
    if frame_type == FrameType.DAY:
        return tf.int2date(tf.day_frames[0])
    elif frame_type == FrameType.WEEK:
        return tf.int2date(tf.week_frames[0])
    elif frame_type == FrameType.MONTH:
        return tf.int2date(tf.month_frames[0])
    elif frame_type == FrameType.MIN1:
        day = datetime.date(2020, 1, 2)
        return datetime.datetime(day.year, day.month, day.day, hour=9, minute=31)
    elif frame_type == FrameType.MIN5:
        day = datetime.date(2019, 1, 2)
        return datetime.datetime(day.year, day.month, day.day, hour=9, minute=35)
    elif frame_type == FrameType.MIN15:
        day = datetime.date(2008, 1, 2)
        return datetime.datetime(day.year, day.month, day.day, hour=9, minute=45)
    elif frame_type == FrameType.MIN30:
        day = datetime.date(2008, 1, 2)
        return datetime.datetime(day.year, day.month, day.day, hour=10)
    elif frame_type == FrameType.MIN60:
        day = datetime.date(2008, 1, 2)
        return datetime.datetime(day.year, day.month, day.day, hour=10, minute=30)
    else:
        raise ValueError(f"{frame_type} not supported")


async def get_checksum(day: int) -> Optional[List]:
    save_to = (Path(cfg.omega.home)/"data/chksum").expanduser()
    chksum_file = os.path.join(save_to, f"chksum-{day}.csv")
    try:
        with open(chksum_file, 'r') as f:
            return f.read().split("\n")[:-1]
    except (FileNotFoundError, Exception) as e:
        pass

    url = cfg.omega.urls.checksum + f"/chksum-{day}.csv"
    async with aiohttp.ClientSession() as client:
        async with client.get(url) as resp:
            if resp.status != 200:
                logger.warning("failed to fetch checksum from %s", url)
                return None

            checksum = await resp.text(encoding='utf-8')
            with open(chksum_file, "w+") as f:
                f.writelines(checksum)

            return checksum.split("\n")[:-1]


async def do_validation():
    """
    SYS validation {
        sec:frame_type:start:end checksum
    }
    日线 按年 2005, 2006, ...
    周线 按年
    60m 按半年 250 * 4
    30m 按季  250 * 8 / 4
    Returns:

    """
    logger.info("start validation...")

    report = logging.getLogger('validation_report')
    codes = Securities().choose(['stock', 'index'])
    for day in tf.day_frames:
        expected = await get_checksum(day)
        if expected is None:
            continue

        expected = set(expected)
        actual = await calc_checksums(day, codes)
        actual = set(actual)

        diff_exp = expected.difference(actual)
        diff_act = actual.difference(expected)

        # report diff
        dict_diff_act = {f"{code}:{ft}": chksum for
                         code, ft, chksum in map(lambda x: x.split(","), diff_act)}
        dict_diff_exp = {f"{code}:{ft}": chksum for
                         code, ft, chksum in map(lambda x: x.split(","), diff_exp)}

        for k, v in dict_diff_act.items():
            report.info("%s,%s,%s,%s", day, k, v, dict_diff_exp.get(k))
            await emit.emit(Events.OMEGA_VALIDATION_ERROR, f"{day},{k},{v},"
                                                           f"{dict_diff_exp.get(k)}",
                            app_name)

    elapsed = await stop_job_timer('validation')
    logger.info('%s stop validation in %s seconds', os.getpid(), elapsed)


async def do_checksum():
    """
    按天生成校验和。起始日期由cache.sys.checksum.cursor指定。

    文件名 checksum-date.csv,内容为：
        code,frame_type,checksum

    """
    logger.info("starting calc checksum...")
    now = tf.date2int(arrow.now().date())
    cursor = int(await cache.sys.get('checksum.cursor') or tf.day_frames[0])

    days = tf.day_frames[(tf.day_frames > cursor) & (tf.day_frames <= now)]

    save_to = os.path.expanduser(cfg.omega.chksum.folder)
    secs = Securities()
    codes = secs.choose(['stock', 'index'])
    for day in days:
        logger.info("calc checksum for day %s", day)
        rst = await calc_checksums(day, codes)
        logger.info("done with calc checksum for day %s", day)
        save_to_file = os.path.join(save_to, f"chksum-{day}.csv")
        with open(save_to_file, "w") as f:
            f.writelines(map(lambda line: line + "\n", rst))

    logger.info("end checksum calc, start uploading...")
    repo = sh.git.bake(_cwd=save_to)
    repo.add(".")
    repo.commit("-m", str(now))
    repo.push()


async def calc_checksums(day: int, codes: List):
    end_dt = tf.int2date(day)
    end_tm = arrow.get(end_dt, tzinfo=cfg.tz).replace(hour=15)

    result = []
    for i, code in enumerate(codes):
        d = await sc.get_bars_raw_data(code, end_dt, 1, FrameType.DAY)
        if d:
            result.append(f"{code},{FrameType.DAY.value},{xxhash.xxh32_hexdigest(d)}")

        d = await sc.get_bars_raw_data(code, end_tm, 240, FrameType.MIN1)
        if d:
            result.append(f"{code},{FrameType.MIN1.value},{xxhash.xxh32_hexdigest(d)}")

        d = await sc.get_bars_raw_data(code, end_tm, 48, FrameType.MIN5)
        if d:
            result.append(f"{code},{FrameType.MIN5.value},{xxhash.xxh32_hexdigest(d)}")

        d = await sc.get_bars_raw_data(code, end_tm, 16, FrameType.MIN15)
        if d:
            result.append(f"{code},{FrameType.MIN15.value},{xxhash.xxh32_hexdigest(d)}")

        d = await sc.get_bars_raw_data(code, end_tm, 8, FrameType.MIN30)
        if d:
            result.append(f"{code},{FrameType.MIN30.value},{xxhash.xxh32_hexdigest(d)}")

        d = await sc.get_bars_raw_data(code, end_tm, 4, FrameType.MIN60)
        if d:
            result.append(f"{code},{FrameType.MIN60.value},{xxhash.xxh32_hexdigest(d)}")

        if (i + 1) % 500 == 0:
            logger.info("calc checksum progress: %s/%s", i + 1, len(codes))

    return result
