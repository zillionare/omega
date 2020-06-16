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
from collections import ChainMap
from pathlib import Path
from typing import Optional, List, Union

import aiohttp
import arrow
import cfg4py
import omicron
import psutil
import xxhash
from aiocache import cached
from aiohttp import ClientError
from arrow import Arrow
from omicron.core.errors import FetcherQuotaError
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.dal import cache
from omicron.dal import security_cache as sc
from omicron.models.securities import Securities
from pyemit import emit

from omega.config.cfg4py_auto_gen import Config
from omega.core import get_config_dir
from omega.core.events import Events, ValidationError
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq

logger = logging.getLogger(__name__)

cfg: Config = cfg4py.get_instance()
validation_errors = []
no_validation_error_days = set()


async def start_job_timer(job_name: str):
    key_start = f"jobs.bars_{job_name}.start"

    pl = cache.sys.pipeline()
    pl.delete(f"jobs.bars_{job_name}.*")

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


async def start_sync(secs: List[str] = None, frames_to_sync: dict = None):
    """
    初始化bars_sync的任务，并发信号给各后台quotes_fetcher进程以启动同步。

    同步的时间范围指定均为日级别。如果是非交易日，自动对齐到上一个已收盘的交易日，使用两端闭合区
    间（截止frame直到已收盘frame)。

    如果未指定同步结束日期，则同步到当前已收盘的交易日。

    Args:
        secs (List[str]): 将同步的证券代码
            如果为None，则使用``omega.sync.type``定义的类型来选择要同步的证券代码。
        frames_to_sync (dict): frames and range(start, end)
            ::
                {
                    '30m': '2018-01-01,2019-01-01'
                }
            表明要同步从2018年1月1日到2019年1月1日的30分钟线数据。

    Returns:

    """
    key_scope = f"jobs.bars_sync.scope"

    if secs is None:
        secs = Securities()
        secs = secs.choose(cfg.omega.sync.type)

    if frames_to_sync is None:
        frames_to_sync = dict(ChainMap(*cfg.omega.sync.frames))

    await cache.sys.delete(key_scope)

    logger.info("add %s securities into sync queue", len(secs))
    pl = cache.sys.pipeline()
    pl.delete(key_scope)
    pl.lpush(key_scope, *secs)
    await pl.execute()

    await start_job_timer('sync')
    await emit.emit(Events.OMEGA_DO_SYNC, frames_to_sync)


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
    pl.get('jobs.bars_validation.range.start')
    pl.get('jobs.bars_validation.range.end')
    start, end = await pl.execute()

    if start is None:
        start = cfg.omega.validation.start
        end = cfg.omega.validation.end
    elif end is None:
        end = tf.date2int(arrow.now().date())

    start, end = int(start), int(end)
    assert start <= end

    no_validation_error_days = set(
            tf.day_frames[(tf.day_frames >= start) & (tf.day_frames <= end)])

    codes = secs.choose(cfg.omega.sync.type)
    await cache.sys.delete("jobs.bars_validation.scope")
    await cache.sys.lpush("jobs.bars_validation.scope", *codes)

    logger.info("start validation %s secs from %s to %s.", len(codes), start, end)
    emit.register(Events.OMEGA_VALIDATION_ERROR, on_validation_error)

    t0 = time.time()

    code = f"from omega.jobs.syncquotes import do_validation_with_multiprocessing; " \
           f"do_validation_with_multiprocessing()"

    procs = []
    for i in range(cpu_count):
        proc = subprocess.Popen([sys.executable, '-c', code], env=os.environ)
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
    validation_days = set(tf.day_frames[(tf.day_frames >= start) & (tf.day_frames <=
                                                                    end)])
    diff = validation_days - no_validation_error_days
    if len(diff):
        last_no_error_day = min(diff)
    else:
        last_no_error_day = end

    await cache.sys.set('jobs.bars_validation.range.start', last_no_error_day)
    elapsed = time.time() - t0
    logger.info("Validation cost %s seconds, validation will start at %s next time",
                elapsed, last_no_error_day)


async def on_validation_error(report: tuple):
    """
    Args:
        report: object like ::
            (reason, day, code, frame, local, remote)
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


async def do_validation(secs: List[str] = None, start: str = None, end: str = None):
    """
    对列表secs中指定的证券行情数据按start到end指定的时间范围进行校验
    Args:
        secs:
        start:
        end:

    Returns:

    """
    logger.info("start validation...")
    report = logging.getLogger('validation_report')

    cfg = cfg4py.init(get_config_dir(), False)

    await emit.start(engine=emit.Engine.REDIS, dsn=cfg.redis.dsn, start_server=True)
    await omicron.init()
    start = int(start or await cache.sys.get('jobs.bars_validation.range.start'))
    if end is None:
        end = tf.date2int(arrow.now().date())
    else:
        end = int(end or await cache.sys.get('jobs.bars_validation.range.stop'))

    if secs is None:
        async def get_sec():
            return await cache.sys.lpop('jobs.bars_validation.scope')
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
                            ValidationError.LOCAL_MISS, day, code, k, d1.get(k),
                            d2.get(k))
                        report.info("%s,%s,%s,%s,%s,%s", *info)
                        await emit.emit(Events.OMEGA_VALIDATION_ERROR, info)
                    for k in missing2:
                        info = (
                            ValidationError.REMOTE_MISS, day, code, k, d1.get(k),
                            d2.get(k)
                        )
                        report.info("%s,%s,%s,%s,%s,%s", *info)
                        await emit.emit(Events.OMEGA_VALIDATION_ERROR, info)
                    for k in mismatch:
                        info = (
                            ValidationError.MISMATCH, day, code, k, d1.get(k),
                            d2.get(k)
                        )
                        report.info("%s,%s,%s,%s,%s,%s", *info)
                        await emit.emit(Events.OMEGA_VALIDATION_ERROR, info)

                else:
                    logger.error("checksum for %s not found.", day)
                    info = (
                        ValidationError.NO_CHECKSUM, day, None, None, None, None
                    )
                    report.info("%s,%s,%s,%s,%s,%s", *info)
                    await emit.emit(Events.OMEGA_VALIDATION_ERROR, info)
        except Exception as e:
            logger.exception(e)
            errors += 1

    await emit.emit(Events.OMEGA_VALIDATION_ERROR, (ValidationError.UNKNOWN, errors))
    logger.warning("do_validation meet %s unknown errors", errors)


def do_validation_with_multiprocessing():
    try:
        t0 = time.time()
        asyncio.run(do_validation())
        logger.info('validation finished in %s seconds', time.time() - t0)
        return 0
    except Exception as e:
        logger.warning('validation exit due to exception:')
        logger.exception(e)
        return -1


async def do_sync(sync_frames: dict = None, secs: List[str] = None):
    """
    worker's sync job
    """
    logger.info("signal: %s with params: %s", Events.OMEGA_DO_SYNC, sync_frames)

    key_scope = "jobs.bars_sync.scope"

    if secs is not None:
        async def get_sec():
            return secs.pop() if len(secs) else None
    else:
        async def get_sec():
            return await cache.sys.lpop(key_scope)

    # noinspection PyPep8
    while code := await get_sec():
        try:
            await sync_for_sec(code, sync_frames)
        except FetcherQuotaError as e:
            logger.warning("Quota exceeded when syncing %s. Sync borted.", code)
            logger.exception(e)
            return  # stop the sync
        except Exception as e:
            logger.warning("Failed to sync %s", code)
            logger.exception(e)

    elapsed = await stop_job_timer('sync')
    logger.info('%s finished quotes sync in %s seconds', os.getpid(), elapsed)


def get_closed_frame(stop: Union[Arrow, datetime.date, datetime.datetime],
                     frame_type: FrameType):
    """
    同步时间范围按天来指定，以符合人类习惯。但具体操作时，需要确定到具体的时间帧上。对截止时间，
    如果是周线、月线、日线，取上一个收盘交易日所在的日、周、月线。如果是分钟线，取上一个收盘交
    易日的最后一根K线。
    Args:
        stop: 用户指定的截止日期
        frame_type:

    Returns:

    """
    stop = arrow.get(stop).date()
    now = arrow.now()
    if stop == now.date() and tf.is_trade_day(now.date()) \
            and now.hour * 60 + now.minute <= 900:
        # 如果截止日期正处在交易时间，则只能同步到上一交易日
        stop = tf.day_shift(stop, -1)

    if frame_type in [FrameType.WEEK, FrameType.MONTH]:
        stop = tf.shift(stop, 0, frame_type)

    if frame_type in tf.minute_level_frames:
        stop = tf.ceil(stop, frame_type)

    return stop


async def sync_for_sec(code: str, sync_frames: dict = None):
    counters = {frame: 0 for frame in sync_frames.keys()}
    logger.info("syncing quotes for %s", code)

    for frame, start_stop in sync_frames.items():
        frame_type = FrameType(frame)
        now = arrow.now()
        if ',' in start_stop:
            start, stop = map(lambda x: x.strip(' '), start_stop.split(','))
        else:
            start, stop = start_stop, now.date()

        start = tf.floor(start, frame_type)
        stop = get_closed_frame(stop, frame_type)

        # 取数据库中该frame_type下该code的k线起始点
        head, tail = await sc.get_bars_range(code, frame_type)
        if not all([head, tail]):
            await sc.clear_bars_range(code, frame_type)
            n_bars = tf.count_frames(start, stop, frame_type)
            bars = await aq.get_bars(code, stop, n_bars, frame_type)
            counters[frame_type.value] = len(bars)
            logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                         frame_type, code, stop, n_bars, len(bars))
            continue

        if start < head:
            n = tf.count_frames(start, head, frame_type) - 1
            if n > 0:
                _end_at = tf.shift(head, -1, frame_type)
                bars = await aq.get_bars(code, _end_at, n, frame_type)
                counters[frame_type.value] += len(bars)
                logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                             frame_type, code, _end_at, n, len(bars))
                if len(bars) and bars['frame'][-1] != _end_at:
                    logger.warning("incontinuous frames found: bars[-1](%s), "
                                   "head(%s)", bars['frame'][-1], head)

        if stop > tail:
            n = tf.count_frames(tail, stop, frame_type) - 1
            if n > 0:
                bars = await aq.get_bars(code, stop, n, frame_type)
                logger.debug("sync %s level bars of %s to %s: expected: %s, actual %s",
                             frame_type, code, stop, n, len(bars))
                counters[frame_type.value] += len(bars)
                if bars['frame'][0] != tf.shift(tail, 1, frame_type):
                    logger.warning("incontinuous frames found: tail(%s), bars[0]("
                                   "%s)", tail, bars['frame'][0])

    logger.info("finished sync %s, %s", code,
                ",".join([f"{k}:{v}" for k, v in counters.items()]))


@cached(ttl=3600)
async def get_checksum(day: int) -> Optional[List]:
    save_to = (Path(cfg.omega.home) / "data/chksum").expanduser()
    chksum_file = os.path.join(save_to, f"chksum-{day}.json")
    try:
        with open(chksum_file, 'r') as f:
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

                    checksum = await resp.json(encoding='utf-8')
                    with open(chksum_file, "w+") as f:
                        json.dump(checksum, f, indent=2)

                    return checksum
            except ClientError:
                continue


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
            d = await sc.get_bars_raw_data(code, day, 1, FrameType.DAY)
            if d:
                checksum[f"{FrameType.DAY.value}"] = xxhash.xxh32_hexdigest(d)
            d = await sc.get_bars_raw_data(code, end_time, 240, FrameType.MIN1)
            if d:
                checksum[f"{FrameType.MIN1.value}"] = xxhash.xxh32_hexdigest(d)

            d = await sc.get_bars_raw_data(code, end_time, 48, FrameType.MIN5)
            if d:
                checksum[f"{FrameType.MIN5.value}"] = xxhash.xxh32_hexdigest(d)

            d = await sc.get_bars_raw_data(code, end_time, 16, FrameType.MIN15)
            if d:
                checksum[f"{FrameType.MIN15.value}"] = xxhash.xxh32_hexdigest(d)

            d = await sc.get_bars_raw_data(code, end_time, 8, FrameType.MIN30)
            if d:
                checksum[f"{FrameType.MIN30.value}"] = xxhash.xxh32_hexdigest(d)

            d = await sc.get_bars_raw_data(code, end_time, 4, FrameType.MIN60)
            if d:
                checksum[f"{FrameType.MIN60.value}"] = xxhash.xxh32_hexdigest(d)

            checksums[code] = checksum
        except Exception as e:
            logger.exception(e)

        if (i + 1) % 500 == 0:
            logger.info("calc checksum progress: %s/%s", i + 1, len(codes))

    return checksums
