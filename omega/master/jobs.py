#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import datetime
import itertools
import logging
import pickle
import sys
import time
import traceback
from functools import wraps
from typing import Any, Dict, List, Tuple, Union

import arrow
import async_timeout
import cfg4py
import numpy as np
from cfg4py.config import Config
from coretypes import FrameType, SecurityType
from omicron.dal import cache
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.notify.mail import mail_notify
from pyemit import emit
from retrying import retry

from omega.core import constants
from omega.core.errors import QuotaExceededError
from omega.core.events import Events
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.dfs import Storage

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()
work_state = {}  # work的状态


def get_first_day_frame():
    """获取自股市开盘以来，第一个交易日"""
    return TimeFrame.day_frames[0]


def abnormal_master_report():
    def inner(f):
        @wraps(f)
        async def decorated_function():
            """装饰所有生产者"""
            try:
                ret = await f()
                return ret
            except Exception as e:  # pragma: no cover
                logger.exception(e)
                # 发送邮件报告错误
                subject = f"执行生产者{f.__name__}时发生异常"
                body = f"详细信息：\n{traceback.format_exc()}"
                traceback.print_exc()
                await mail_notify(subject, body, html=True)

        return decorated_function

    return inner


class BarsSyncTask:
    def __init__(
        self,
        event: str,
        name: str,
        params: dict,
        timeout: int = 60,
        recs_per_sec: int = 240,
        expire_time: int = None,
    ):
        """
        Args:
            event: 发给worker的消息名
            name: 分配给worker的从redis中取数据的队列名
            params: 发送给emit的参数
            timeout: run需要等待的超时时间
            recs_per_sec: 每支证券将要同步的记录数。
            expire_time: 如果设置，则在任务开始执行后的`expire_time`秒内，自动清除redis中保存的任务状态，以便下次重新启动任务。
        """
        # 读取待同步证券品种的队列名惟一，因此也可以作为任务的唯一标识名
        self.name = name
        self.event = event
        self.params = params
        self.timeout = timeout

        self._stock_scope = self.parse_bars_sync_scope(SecurityType.STOCK)
        self._index_scope = self.parse_bars_sync_scope(SecurityType.INDEX)

        self._recs_per_sec = recs_per_sec
        self.expire_time = expire_time

        self.params.update({"timeout": self.timeout, "name": self.name})

    def _state_key_name(self):
        # review: 最初这些名字被保存为成员变量。成员变量一般用来记录对象的状态和属性。如果只是一些可以通过计算得出来的中间属性，可应该考虑使用函数计算的方式（如果没有性能问题）。
        return f"{constants.TASK_PREFIX}.{self.name}.state"

    def _scope_key_name(self, typ: str, is_done: bool):
        if is_done:
            return f"{constants.TASK_PREFIX}.{self.name}.scope.{typ}.done"

        return f"{constants.TASK_PREFIX}.{self.name}.scope.{typ}"

    async def delete_state(self):
        key = self._state_key_name()
        await cache.sys.delete(key)

    @property
    def recs_per_sec(self):
        """当同步一支证券时，将向服务器请求多少条数据。"""
        return self._recs_per_sec

    @recs_per_sec.setter
    def recs_per_sec(self, value):
        self._recs_per_sec = value

    async def is_running(self) -> bool:
        """检查同类任务是否有一个实例正在运行。

        同一类任务（通过名字来区分）只能有一个实例运行。在redis中，每个同步任务都有一个状态，其key由任务名字惟一确定。通过检查这个状态就可以确定同类任务是否正在运行。

        为避免任务出错时未清除状态，而导致后续任务都无法进行，该状态可以设置自动expire。
        """
        state = await self._get_task_state()
        if state and state["is_running"] is not None:
            msg = f"检测到{self.name}下有正在运行的任务，本次任务不执行"
            logger.info(msg)
            return True

        return False

    async def _get_task_state(self, field: str = None) -> Union[Dict, Any]:
        """从redis中获取任务状态

        如果指明了field,则返回某个具体的字段值，否则返回整个字典。
        """
        # review: 由于多处请求和修改state状态，因此集中管理更好。
        key = self._state_key_name()
        # always return a dict
        state = await cache.sys.hgetall(key)
        if len(state) != 0:
            state = {
                # 如果值存在，则表明已在运行，不关心具体取值
                "is_running": state.get("is_running") is not None,
                "done_count": int(state.get("done_count", 0)),
                "error": state.get("error", ""),
                "worker_count": int(state.get("worker_count", 0)),
            }

        if field:
            return state.get(field)
        else:
            return state

    @classmethod
    def parse_bars_sync_scope(cls, _type: str):
        """生成待同步行情数据的证券列表

        该列表由以下方式生成：
        1. 通过_type指定证券类型（stock或index）
        2. 通过exclude排除指定的证券。
        3. 通过include指定包含的证券。

        当exclude和include同时存在时，include优先。
        exclude和include在配置文件的`omega.sync.bars`条目下指定，使用空格分隔，示例如下：
        ```
        omega:
            sync:
                bars:
                    include: 000001.XSHE
                    exclude: 000001.XSHE 000002.XSHE
        ```
        """
        codes = Stock.choose([_type])
        exclude = getattr(cfg.omega.sync.bars, "exclude", "")
        if exclude:
            exclude = map(lambda x: x, exclude.split(" "))
            codes = list(set(codes) - set(exclude))
        include = getattr(cfg.omega.sync.bars, "include", "")
        if include:
            include = list(filter(lambda x: x, cfg.omega.sync.bars.include.split(" ")))
            codes.extend(include)
        return list(set(codes))

    async def send_email(self, error=None):
        subject = f"执行{self.name}时异常！"
        if error:
            body = error
        else:
            body = f"超时时间是：{self.timeout}"
        body += "\n\n================================================\n\n"
        body += "消费者得到的参数是：" + str(self.params)
        body += "\n\n================================================\n\n"
        quota = self.get_quota()
        body += f"剩余可用quota：{quota}"

        # review: 因为报告机制特别重要，所以不能因为读redis失败而导致发送失败。
        try:
            body += f"{await self.get_sync_failed_secs()}"
        except Exception as e:
            body += "获取失败的证券列表时发生异常：" + str(e)

        logger.info(f"发送邮件subject:{subject}, body: {body}")
        await mail_notify(subject, body, html=True)

    async def update_state(self, **kwargs):
        """更新任务状态"""
        key = f"{constants.TASK_PREFIX}.{self.name}.state"

        # aioredis cannot save bool directly
        if isinstance(kwargs.get("is_running"), bool):
            kwargs["is_running"] = str(kwargs["is_running"])

        pl = cache.sys.pipeline()
        pl.hmset_dict(key, kwargs)
        # todo: 是使用expire还是timeout?或者使用auto_expire?
        if self.expire_time is not None:
            pl.expire(key, self.expire_time)

        await pl.execute()

    def check_quota(self) -> Tuple:
        """检查quota是否足够完成本次同步

        Returns:
            返回Tuple(isok, spare quota, required quota)
        """
        count = len(self._index_scope + self._stock_scope)
        assert count > 0
        # 检查quota，通过证券数据 * 每支证券待同步的条数
        quota = self.get_quota()
        need = count * self._recs_per_sec
        return quota < need, quota, need

    async def update_sync_scope(self):
        """将待同步的证券代码列表写入redis队列，以便worker获取"""
        pl = cache.sys.pipeline()
        for typ in ["index", "stock"]:
            key = self._scope_key_name(typ, False)

            # 记录成功完成同步的证券代码
            key_done = self._scope_key_name(typ, True)
            pl.delete(key)
            pl.delete(key_done)

        pl.lpush(key, *self._index_scope)
        pl.lpush(key, *self._stock_scope)

        await pl.execute()

    async def cleanup(self, success: bool = True):
        """在任务结束时（成功或失败）的清理操作"""

        if success:
            await self.delete_state()
        else:
            state = await self._get_task_state()
            await self.send_email(state.get("error"))

    async def run(self):
        """分配任务并发送emit通知worker开始执行，然后阻塞等待"""
        logger.info(f"{self.name}:{self.params} 任务启动")
        if await self.is_running():
            return False

        ok, spare, required = self.check_quota()
        if not ok:
            msg = f"quota不足，剩余quota：{spare}, 需要quota：{required}"
            await mail_notify(msg)
            return False

        await self.update_state(is_running=1)
        await self.update_sync_scope()

        # worker在同步中，会将bars追加到下面的队列中，因此在同步前需要清空队列
        await delete_temporal_bars(
            [SecurityType.STOCK, SecurityType.INDEX],
            [*TimeFrame.minute_level_frames, TimeFrame.day],
        )

        await emit.emit(self.event, self.params)
        ret = await self.check_done()
        return ret

    async def check_done(self) -> bool:
        """等待worker完成任务

        Returns:
            任务是否完成
        """
        # review: 一般我们可以用t0来标识计时的起点
        t0 = time.time()
        try:
            async with async_timeout.timeout(self.timeout):
                while True:
                    state = await self._get_task_state()
                    is_running, error = state.get("is_running"), state.get("error")

                    if error is not None or not is_running:
                        # 异常退出
                        ret = False
                        break
                    # 如果所有证券已完成同步，则退出
                    done_index = await self.get_sync_done_secs("index")
                    if set(done_index) != set(self._index_scope):
                        await asyncio.sleep(0.5)
                        continue

                    done_stock = await self.get_sync_done_secs("stock")
                    if set(done_stock) != set(self._stock_scope):
                        await asyncio.sleep(0.5)
                        continue

                    logger.info(
                        f"{self.scope}, params:{self.params},耗时：{time.time() - t0}"
                    )

                    ret = True
                    break
        except asyncio.exceptions.TimeoutError:
            # review: 这里有一个pragma: nocover. 对异常分支不进行单元测试的问题是，万一这些异常真的出现，它们可能引起二次异常，从而最终导致程序崩溃 -- 这也许并不是我们想要的 -- 单元测试可以排除掉未处理的二次异常。
            logger.info("消费者超时退出")
            ret = False
        finally:
            self.cleanup(ret)

        return ret

    async def get_sync_done_secs(self, typ: str) -> List:
        key = self._scope_key_name(typ, True)
        return await cache.sys.lrange(key, 0, -1)

    async def get_sync_failed_secs(self) -> List[str]:
        """获取本次同步中失败的证券"""
        done_index = await self.get_sync_done_secs("index")
        done_stock = await self.get_sync_done_secs("stock")

        failed = set(self._index_scope) - set(done_index) | set(
            self._stock_scope
        ) - set(done_stock)

        return list(failed)


async def write_dfs(
    dt: datetime.datetime,
    resample: bool = False,
):
    """将校准同步/追赶同步时下载的数据写入块存储 - minio

    Args:
        dt: 日期
        resample: 是否需要重采样  只重采样分钟线 到 5 15 30 60
    Returns:

    """
    prefix = "daily_calibration"

    dfs = Storage()
    if dfs is None:  # pragma: no cover
        return

    for typ, ft in itertools.product(
        ["stock", "index"], [FrameType.MIN1, FrameType.DAY]
    ):
        queue_name = f"{prefix}.{typ}.{ft.value}"

        # todo: structure/fields of the data? does it contains frametye or code?
        data = await cache.temp.lrange(queue_name, 0, -1, encoding=None)
        if not data:
            return

        logger.info(f"queue_name:{queue_name},frame_type:{ft}")

        # todo: it's better to use another variable name for i and data
        all_bars = {}
        for item in data:
            bars = pickle.loads(item)
            assert isinstance(bars, dict)
            all_bars.update(bars)

        # todo: now resmaple is disabled
        binary = pickle.dumps(bars, protocol=cfg.pickle.ver)
        await dfs.write(binary, typ, dt, ft)

        # todo: let worker do the resample
        if resample and ft == FrameType.MIN1:
            for to_frame in (
                FrameType.MIN5,
                FrameType.MIN15,
                FrameType.MIN30,
                FrameType.MIN60,
            ):
                # we need to support batch resample here
                resampled = Stock.resample(bars, FrameType.MIN1, to_frame)
                resampled_binary = pickle.dumps(resampled, protocol=cfg.pickle.ver)
                await dfs.write(resampled_binary, prefix, dt, to_frame)

        await cache.temp.delete(queue_name)


async def __daily_calibration_sync(
    sync_dt: datetime.datetime,
    head: datetime.datetime = None,
    tail: datetime.datetime = None,
):
    """

    Args:
        sync_dt: 本次同步的行情数据所属的交易日
        head: 已经同步的最早时间
        tail: 已经同步的最晚时间

    """
    start = sync_dt.replace(hour=9, minute=31, microsecond=0, second=0)
    end = sync_dt.replace(hour=15, minute=0, microsecond=0, second=0)
    # 检查 end 是否在交易日

    params = {"start": start, "end": end, "to_cache": False}

    task = BarsSyncTask(
        Events.OMEGA_DO_SYNC_DAILY_CALIBRATION,
        "calibration_sync",
        params,
        60 * 60 * 6,
        recs_per_sec=(240 * 2 + 4) // 0.75,
    )

    ret = await task.run()
    # 如果运行中出现错误，则中止本次同步
    if not ret:
        return ret
    logger.info(f"daily_calibration -- params:{params} 已执行完毕，准备进行持久化")

    # 将raw数据写入块存储--minio
    # todo: disable resample for now
    await write_dfs(end, resample=False)

    # 成功同步了`sync_dt`这一天的数据，更新 head 和 tail
    if head is not None:
        await cache.sys.set(constants.BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d"))
    if tail is not None:
        await cache.sys.set(constants.BAR_SYNC_ARCHIVE_TAIL, tail.strftime("%Y-%m-%d"))

    logger.info("%s(%s)同步完成,参数为%s", self.name, sync_dt, self.params)


def get_yesterday_or_pre_trade_day(now):
    """获取昨天或者上一个交易日"""
    # todo: could be replaced by TimeFrame.day_shift(now, 0)?
    if TimeFrame.date2int(now) in TimeFrame.day_frames:
        pre_trade_day = TimeFrame.day_shift(now, -1)
    else:
        pre_trade_day = TimeFrame.day_shift(now, 0)
    return pre_trade_day


async def get_sync_date() -> Tuple:
    """计算校准（追赶）同步的日期

    追赶同步是系统刚建立不久时，数据库缺少行情数据，受限于每日从上游服务器取数据的quota，因此只能每日在quota限额内，进行小批量的同步，逐步把数据库缺失的数据补起来的一种同步。

    追赶同步使用与校准同步同样的实现，但优先级较低，只有在校准同步完成后，还有quota余额的情况下才进行。

    Args:
        now:

    Returns:
        返回一个(sync_dt, head, tail)组成的元组，sync_dt是同步的日期，如果为None，则无需进行追赶同步。

    """
    head, tail = (
        await cache.sys.get(constants.BAR_SYNC_ARCHIVE_HEAD),
        await cache.sys.get(constants.BAR_SYNC_ARCHIVE_TAIL),
    )

    # todo: check if it can get right sync_dt
    now = arrow.now().date()
    pre_trade_day = get_yesterday_or_pre_trade_day(now)
    if not head or not tail:
        # 任意一个缺失都不行
        logger.info("说明是首次同步，查找上一个已收盘的交易日")
        sync_dt = datetime.datetime.combine(pre_trade_day, datetime.time(0, 0))
        head = tail = pre_trade_day

    else:
        # 说明不是首次同步，检查tail到现在有没有空洞
        tail_date = datetime.datetime.strptime(tail, "%Y-%m-%d")
        head_date = datetime.datetime.strptime(head, "%Y-%m-%d")
        frames = TimeFrame.count_frames(
            tail_date,
            pre_trade_day,
            FrameType.DAY,
        )
        if frames > 1:
            # 最后同步时间到当前最后一个结束的交易日之间有空洞，向后追赶
            tail_date = datetime.datetime.combine(
                TimeFrame.day_shift(tail_date, 1), datetime.time(0, 0)
            )
            sync_dt = tail = tail_date
            head = None

        else:
            # 已同步到最后一个交易日，向前追赶
            sync_dt = head = datetime.datetime.combine(
                TimeFrame.day_shift(head_date, -1), datetime.time(0, 0)
            )
            tail = None

    # 检查时间是否小于 2005年，小于则说明同步完成了
    day_frame = get_first_day_frame()
    if TimeFrame.date2int(sync_dt) < day_frame:
        logger.info("所有数据已同步完毕")
        sync_dt = None

    return sync_dt, head, tail


@abnormal_master_report()
async def daily_calibration_job():
    """scheduled task entry

    runs at every day at 2:00 am
    """
    logger.info("每日数据校准已启动")

    while True:
        sync_dt, head, tail = await get_sync_date()
        if sync_dt is None:
            break

        sucess = await __daily_calibration_sync(sync_dt, head=head, tail=tail)
        if not sucess:
            break
        else:
            now = arrow.now().date()
            # 当天的校准同步已经完成，清除缓存。
            if sync_dt == TimeFrame.day_shift(now, 0):
                await Stock.reset_cache()

    logger.info("exit daily_calibration_job")


@abnormal_master_report()
async def after_hour_sync_job():
    """交易日盘后同步任务入口

    收盘之后同步今天的日线和分钟线
    """
    now = arrow.now()
    start = TimeFrame.first_min_frame(now, FrameType.MIN1)
    end = TimeFrame.last_min_frame(now, FrameType.MIN1)

    sync_params = {"start": start, "end": end, "n_bars": 240}
    queue_name = "day"

    task = BarsSyncTask(
        Events.OMEGA_DO_SYNC_DAY,
        queue_name,
        sync_params,
        timeout=60 * 60 * 2,
        recs_per_sec=240 * 2 + 4,
    )

    return await task.run()


@abnormal_master_report()
async def sync_minute_bars():
    """盘中同步每分钟的数据
    1. 从redis拿到上一次同步的分钟数据
    2. 计算开始和结束时间
    """
    end = datetime.datetime.now().replace(second=0, microsecond=0)
    first = end.replace(hour=9, minute=30, second=0, microsecond=0)
    timeout = 60
    # 检查当前时间是否在交易时间内
    if end.hour * 60 + end.minute not in TimeFrame.ticks[FrameType.MIN1]:
        if 11 <= end.hour < 13:
            end = end.replace(hour=11, minute=30)
        else:
            end = end.replace(hour=15, second=0, minute=0)

    if not TimeFrame.is_trade_day(end):
        print("非交易日，不同步")
        return False

    queue_name = "minute"
    tail = await cache.sys.hget(constants.BAR_SYNC_STATE_MINUTE, "tail")
    if tail:
        # todo 如果有，这个时间最早只能是今天的9点31分,因为有可能是昨天执行完的最后一次
        tail = datetime.datetime.strptime(tail, "%Y-%m-%d %H:%M:00")
        if tail < first:
            tail = first
        elif tail.hour == 11 and tail.minute == 30:
            # 说明上午已经同步完了，到下午同步了，把tail改为13点
            tail = tail.replace(hour=13, minute=0)
    else:
        tail = first

    tail += datetime.timedelta(minutes=1)  # 取上次同步截止时间+1 计算出n_bars
    n_bars = TimeFrame.count_frames(tail, end, FrameType.MIN1)  # 获取到一共有多少根k线

    params = {"start": tail, "end": end, "n_bars": n_bars}
    task = BarsSyncTask(
        Events.OMEGA_DO_SYNC_MIN,
        queue_name,
        params,
        timeout=timeout * n_bars,
        recs_per_sec=n_bars,
    )

    flag = await task.run()
    if flag:
        # 说明正常执行完的
        await cache.sys.hset(
            constants.BAR_SYNC_STATE_MINUTE, "tail", end.strftime("%Y-%m-%d %H:%M:00")
        )
    return flag


@abnormal_master_report()
async def sync_trade_price_limits():
    """每天9点半之后同步一次今日涨跌停并写入redis"""
    timeout = 60 * 10
    end = TimeFrame.last_min_frame(arrow.now(), FrameType.MIN1)
    if not TimeFrame.is_trade_day(end):
        logger.info("非交易日，不同步")
        return False
    params = {"end": end}
    task = BarsSyncTask(
        Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, "high_low_limit", params, timeout
    )
    task.recs_per_sec = 1
    await task.run()


async def delete_year_quarter_month_week_queue(stock, index):
    """
    清理校准同步的队列，防止数据重复
    """
    p = cache.temp.pipeline()
    p.delete(stock)
    p.delete(index)
    await p.execute()


async def delete_temporal_bars(typ: List[str], frame_types: List[FrameType]):
    """清理临时存储在redis中的行情数据

    这部分数据使用list来存储，因此，在每次同步之前，必须先清理redis中的list，防止数据重复。

    Args:
        typ: 品种类型，如SecurityType.STOCK
        frame_types: 帧类型，如FrameType.MIN1
    """
    assert isinstance(typ, list)
    assert isinstance(frame_types, list)

    p = cache.temp.pipeline()
    for t, ft in itertools.product(typ, frame_types):
        key = f"{constants.MINIO_TEMPORAL}.{t}.{ft.value}"
        p.delete(key)
    await p.execute()


async def __sync_year_quarter_month_week(tail_key, frame_type):
    year_quarter_month_week_calendar = {
        FrameType.WEEK: TimeFrame.int2date(TimeFrame.week_frames[0]),
        FrameType.MONTH: TimeFrame.int2date(TimeFrame.month_frames[0]),
        FrameType.QUARTER: TimeFrame.int2date(TimeFrame.quarter_frames[0]),
        FrameType.YEAR: TimeFrame.int2date(TimeFrame.year_frames[0]),
    }

    tail = await cache.sys.get(tail_key)
    now = arrow.now()
    if not tail:
        tail = year_quarter_month_week_calendar.get(frame_type)
    else:
        tail = datetime.datetime.strptime(tail, "%Y-%m-%d")
        tail = TimeFrame.shift(tail, 1, frame_type)
    # 判断week_tail到现在有没有空洞
    count_frame = TimeFrame.count_frames(
        tail,
        now.replace(hour=0, minute=0, second=0, microsecond=0),
        frame_type,
    )
    params = {}
    if count_frame >= 1:
        queue_name = frame_type.value
        stock_queue = f"{queue_name}.stock"
        index_queue = f"{queue_name}.index"
        stock_data = f"{queue_name}.stock.data"
        index_data = f"{queue_name}.index.data"
        params = {
            "end": tail,
            "frame_type": frame_type,
            "stock_data": stock_data,
            "index_data": index_data,
        }

        # 说明有空洞,需要同步tail的周线数据
        task = BarsSyncTask(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            queue_name,
            params,
            60 * 10,
            stock_queue=stock_queue,
            index_queue=index_queue,
        )
        # 设置系数
        task.recs_per_sec = 2
        await delete_year_quarter_month_week_queue(stock_data, index_data)
        ret = await task.run()
        if not ret:
            await delete_year_quarter_month_week_queue(stock_data, index_data)
            logger.info(
                f"同步{frame_type.value}时退出，count_frame：{count_frame}, params:{params}"
            )
            return False
        await write_dfs(stock_data, frame_type, "stock", tail)
        await write_dfs(index_data, frame_type, "index", tail)
        await delete_year_quarter_month_week_queue(stock_data, index_data)
        await cache.sys.set(tail_key, tail.strftime("%Y-%m-%d"))
        return True
    logger.info(f"同步{frame_type.value}时退出，count_frame：{count_frame}, params:{params}")
    return False


async def run_sync_year_quarter_month_week(week=True, month=True):
    # 检查周线 tail
    logger.info("sync_year_quarter_month_week 启动")

    if week:
        week = await __sync_year_quarter_month_week(
            constants.BAR_SYNC_WEEK_TAIL, FrameType.WEEK
        )
        logger.info(f"执行{FrameType.WEEK.value}完毕")

    if month:
        month = await __sync_year_quarter_month_week(
            constants.BAR_SYNC_MONTH_TAIL, FrameType.MONTH
        )
        logger.info(f"执行{FrameType.MONTH.value}完毕")

        # return False
    if not week and not month:
        logger.info("同步周、月完毕")
        return False
    await run_sync_year_quarter_month_week(week, month)


@abnormal_master_report()
async def sync_year_quarter_month_week():
    """同步年月日周"""
    # 检查周线 tail
    logger.info("sync_year_quarter_month_week 启动")
    sys.setrecursionlimit(10000)
    await run_sync_year_quarter_month_week()


async def load_cron_task(scheduler):
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=9,
        minute="31-59",
        name=f"{FrameType.MIN1.value}:9:31-59",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=10,
        minute="*",
        name=f"{FrameType.MIN1.value}:10:*",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=11,
        # review: 设定30分结束同步，会导致无法同步到30分钟的数据
        minute="0-31",
        name=f"{FrameType.MIN1.value}:11:0-31",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour="13-14",
        # review: 1-59将导致整数点上的分钟线被延时同步
        minute="0-59",
        name=f"{FrameType.MIN1.value}:13-14:*",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=15,
        # review: 之前的实现会导致只同步到59分钟的数据
        minute="0-1",
        name=f"{FrameType.MIN1.value}:15:00",
    )

    scheduler.add_job(
        after_hour_sync_job,
        "cron",
        hour="15",
        minute=5,
        name="sync_day_bars",
    )
    scheduler.add_job(
        sync_year_quarter_month_week,
        "cron",
        hour=2,
        minute=5,
        name="sync_year_quarter_month_week",
    )
    scheduler.add_job(
        daily_calibration_job,
        "cron",
        hour=2,
        minute=5,
        name="daily_calibration_sync",
    )

    scheduler.add_job(
        sync_trade_price_limits,
        "cron",
        hour=9,
        minute=31,
        name="sync_trade_price_limits",
    )
