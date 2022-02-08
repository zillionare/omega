#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import datetime
import logging
import pickle
import sys
import time
import traceback
from functools import wraps

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
from omega.core.events import Events
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.dfs import Storage

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()
work_state = {}  # work的状态


def get_now():
    return datetime.datetime.now()


def get_timeout(timeout=60):  # pragma: no cover
    return timeout


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


class Task:
    def __init__(
        self,
        channel: str,
        task_queue_name: str,
        params: dict,
        timeout: int = 60,
        stock_queue: str = None,
        index_queue: str = None,
        coefficient: int = 240,
    ):
        """
        Args:
            channel: emit的事件名
            task_queue_name: 分配给worker的从redis中取数据的队列名
            params: 发送给emit的参数
            timeout: run需要等待的超时时间
            stock_queue: 校验同步的股票队列
            index_queue: 校验同步的指数队列
            coefficient: 系数 用来计算quota够不够  如果quota < 任务总数*coefficient 则不同步 并发送邮件报警
        """
        self.channel = channel
        self.task_queue_name = task_queue_name
        self.params = params
        self.state, self.scope, self.fail = self.get_queue_name(task_queue_name)
        self.timeout = timeout
        self.__task_list = []
        self.__stock = []
        self.__index = []
        self.stock_queue = stock_queue
        self.index_queue = index_queue
        self.coefficient = coefficient

    async def set_coefficient(self, coefficient):
        self.coefficient = coefficient

    @classmethod
    def get_queue_name(cls, task_queue_name):
        return (
            f"{constants.TASK_PREFIX}.{task_queue_name}.state",
            f"{constants.TASK_PREFIX}.{task_queue_name}.scope",
            f"{constants.TASK_PREFIX}.{task_queue_name}.fail",
        )

    async def delete_state(self):
        await cache.sys.delete(self.state)
        return True

    @property
    async def is_running(self):
        # 检查这个任务是否已经在运行
        is_running = await cache.sys.hget(self.state, "is_running")
        if is_running is not None:
            msg = f"检测到{self.state}下有正在运行的任务，本次任务不执行"
            logger.info(msg)
            return True

    @classmethod
    async def __generate_task(cls, _type: str):
        """生成股票或基金的任务数据"""
        codes = Stock.choose([_type])
        exclude = getattr(cfg.omega.sync.bars, "exclude", "")
        if exclude:
            exclude = map(lambda x: x, exclude.split(" "))
            codes = list(set(codes) - set(exclude))
        include = getattr(cfg.omega.sync.bars, "include", "")
        if include:  # pragma: no cover
            include = list(filter(lambda x: x, cfg.omega.sync.bars.include.split(" ")))
            codes.extend(include)
        return list(set(codes))

    async def generate_task(self):
        stock = await self.__generate_task(SecurityType.STOCK.value)
        index = await self.__generate_task(SecurityType.INDEX.value)
        self.__stock = stock
        self.__index = index
        self.__task_list = list(set(stock + index))

    async def tasks(self):
        if not self.__task_list:
            await self.generate_task()
        return self.__task_list

    @classmethod
    async def get_quota(cls):
        quota = 0
        for worker in work_state.values():
            quota += worker.get("quota", 0)
        return quota

    async def send_email(self, error=None):
        subject = f"执行{self.state}时异常！"
        if error:
            body = error
        else:
            body = f"超时时间是：{self.timeout}"
        body += "\n\n================================================\n\n"
        body += "消费者得到的参数是：" + str(self.params)
        body += "\n\n================================================\n\n"
        quota = await self.get_quota()
        body += f"剩余可用quota：{quota}"
        body += f"{await self.get_fail()}"
        logger.info(f"发送邮件subject:{subject}, body: {body}")
        await mail_notify(subject, body, html=True)

    async def run(self):
        """分配任务并发送emit通知worker开始执行，然后阻塞等待"""
        logger.info(f"{self.task_queue_name}:{self.params} 任务启动")
        if await self.is_running:
            return False
        tasks = await self.tasks()
        count = len(tasks)
        if not count:
            logger.info(f"未找到任何股票或证券，不执行 params：{self.params}")
            return False
        # 检查quota，通过 task的数量 * 一个系数
        quota = await self.get_quota()
        need = count * self.coefficient
        if quota < need:
            msg = f"剩余quota不够本次同步，剩余：{quota}, 至少需要{need}才可以同步，"
            await self.send_email(msg)
            return False
        p = cache.sys.pipeline()
        p.hset(self.state, "is_running", 1)
        p.expire(self.state, self.timeout * 5)
        await p.execute()
        p = cache.sys.pipeline()
        p.hmset(self.state, "task_count", count)

        if self.stock_queue is None or self.index_queue is None:
            p.delete(self.scope)
            p.lpush(self.scope, *tasks)
        else:
            p.delete(self.stock_queue)
            p.delete(self.index_queue)
            if self.__stock:  # pragma: no cover
                p.lpush(self.stock_queue, *self.__stock)
            if self.__index:  # pragma: no cover
                p.lpush(self.index_queue, *self.__index)
            self.params.update(
                {"stock_queue": self.stock_queue, "index_queue": self.index_queue}
            )
        await p.execute()
        self.params.update(
            {
                "__scope": self.scope,
                "__state": self.state,
                "__fail": self.fail,
                "__timeout": self.timeout,
            }
        )
        await emit.emit(self.channel, self.params)
        ret = await self.check_done(count)
        await self.delete_state()
        return ret

    async def get_fail(self):
        p = cache.sys.pipeline()
        p.lrange(self.fail, 0, -1)
        p.delete(self.fail)
        fails, _ = await p.execute()
        # if fails:
        #     body += "\n执行这些股票数据时发生错误：" + str(fails)
        return fails

    async def check_done(self, count: int) -> bool:
        """
        等待fether完成任务
        Args:
            count: 需要等待完成的任务的条数

        Returns:
            任务是否完成
        """
        s = time.time()
        try:
            async with async_timeout.timeout(self.timeout):
                while True:
                    p = cache.sys.pipeline()
                    p.hget(self.state, "is_running")
                    p.hget(self.state, "done_count")
                    p.hget(self.state, "error")
                    p.hget(self.state, "worker_count")
                    is_running, done_count, error, worker_count = await p.execute()
                    if worker_count is None:
                        # 说明消费者还没收到消息，等待
                        await asyncio.sleep(0.5)
                        continue
                    if not is_running:
                        # 说明消费者异常退出了， 发送邮件
                        await self.send_email(error)
                        return False
                    # done_count = await cache.sys.hget(self.state, "done_count")
                    # 检查消费者状态，如果没有is_running，并且任务完成数量不对，说明消费者移除退出了，错误信息在 error 这个key中
                    # 把error 和 fail 读出来之后 发送邮件，然后master退出，等待下一次执行
                    if done_count is None or int(done_count) != count:
                        await asyncio.sleep(0.5)
                    else:
                        print(
                            f"{self.scope}, params:{self.params},耗时：{time.time() - s}"
                        )
                        logger.info(
                            f"{self.scope}, params:{self.params},耗时：{time.time() - s}"
                        )
                        return True
        except asyncio.exceptions.TimeoutError:  # pragma: no cover
            logger.info("消费者超时退出")
            await self.send_email()
            return False


async def _start_job_timer(job_name: str):
    key_start = f"master.bars_{job_name}.start"

    pl = cache.sys.pipeline()
    pl.delete(f"master.bars_{job_name}.*")

    pl.set(key_start, arrow.now(tz=cfg.tz).format("YYYY-MM-DD HH:mm:ss"))
    await pl.execute()


async def _stop_job_timer(job_name: str) -> int:
    key_start = f"master.bars_{job_name}.start"
    key_stop = f"master.bars_{job_name}.stop"
    key_elapsed = f"master.bars_{job_name}.elapsed"

    start = arrow.get(await cache.sys.get(key_start), tzinfo=cfg.tz)
    stop = arrow.now(tz=cfg.tz)
    elapsed = (stop - start).seconds

    pl = cache.sys.pipeline()
    pl.set(key_stop, stop.format("YYYY-MM-DD HH:mm:ss"))
    pl.set(key_elapsed, elapsed)
    await pl.execute()

    return elapsed


async def delete_daily_calibration_queue(stock_min, index_min, stock_day, index_day):
    """
    清理校准同步的队列，防止数据重复
    Args:
        stock_min: 分钟股票数据
        index_min: 分钟指数数据
        stock_day: 天股票数据
        index_day: 天指数数据

    Returns:

    """
    p = cache.temp.pipeline()
    p.delete(stock_min)
    p.delete(index_min)
    p.delete(stock_day)
    p.delete(index_day)
    await p.execute()


@retry(stop_max_attempt_number=5)
async def persist_bars(frame_type, bars1):
    logger.info(f"正在写入inflaxdb:frame_type:{frame_type}")
    # todo
    await Stock.persist_bars(frame_type, bars1)

    logger.info(f"已经写入inflaxdb:frame_type:{frame_type}")


async def write_dfs(
    queue_name: str,
    frame_type: FrameType,
    prefix: str,
    dt: datetime.datetime,
    resample: bool = False,
):
    """
    Args:
        queue_name: 需要从那个队列中取数据写入dfs
        frame_type: 帧类型，对应 1d, 1m
        prefix: dfs中的前缀 stock, index
        dt: 日期
        resample: 是否需要重采样  只重采样分钟线 到 5 15 30 60
    Returns:

    """
    dfs = Storage()
    if dfs is None:  # pragma: no cover
        return
    data = await cache.temp.lrange(queue_name, 0, -1, encoding=None)
    if not data:
        return
    logger.info(f"queue_name:{queue_name},frame_type:{frame_type.value}")
    bars = [pickle.loads(i) for i in data]
    # data = pickle.loads(data)
    bars = np.concatenate(bars)
    await persist_bars(frame_type, bars)
    binary = pickle.dumps(bars, protocol=cfg.pickle.ver)
    await dfs.write(binary, prefix, dt, frame_type)
    if resample and frame_type == FrameType.MIN1:
        for ftype in (
            FrameType.MIN5,
            FrameType.MIN15,
            FrameType.MIN30,
            FrameType.MIN60,
        ):
            resampled = Stock.resample(bars, FrameType.MIN1, ftype)
            resampled_binary = pickle.dumps(resampled, protocol=cfg.pickle.ver)
            await persist_bars(ftype, resampled)
            await dfs.write(resampled_binary, prefix, dt, ftype)

    await cache.temp.delete(queue_name)


async def __daily_calibration_sync(
    tread_date: datetime.datetime,
    now: datetime.datetime,
    head: datetime.datetime = None,
    tail: datetime.datetime = None,
):
    """

    Args:
        tread_date: 本次需要校验的交易日时间对象
        head: 已经同步的最早时间
        tail: 已经同步的最晚时间

    """
    start = tread_date.replace(hour=9, minute=31, microsecond=0, second=0)
    end = tread_date.replace(hour=15, minute=0, microsecond=0, second=0)
    # 检查 end 是否在交易日
    queue_name = "daily_calibration"
    stock_queue = f"{queue_name}.stock"
    index_queue = f"{queue_name}.index"
    stock_min = f"{queue_name}.stock.min"
    index_min = f"{queue_name}.index.min"
    stock_day = f"{queue_name}.stock.day"
    index_day = f"{queue_name}.index.day"
    params = {
        "start": start,
        "end": end,
        "n_bars": 240,
        "stock_min": stock_min,
        "index_min": index_min,
        "stock_day": stock_day,
        "index_day": index_day,
    }
    task = Task(
        Events.OMEGA_DO_SYNC_DAILY_CALIBRATION,
        queue_name,
        params,
        get_timeout(60 * 60 * 6),
        stock_queue=stock_queue,
        index_queue=index_queue,
    )

    await task.set_coefficient((240 * 2 + 4) // 0.75)

    await delete_daily_calibration_queue(stock_min, index_min, stock_day, index_day)
    ret = await task.run()
    if not ret:
        return ret
    logger.info(f"daily_calibration -- params:{params} 已执行完毕，准备进行持久化")
    # 读出来 写dfs
    await write_dfs(stock_min, FrameType.MIN1, "stock", end, resample=True)
    await write_dfs(stock_day, FrameType.DAY, "stock", end)

    await write_dfs(index_min, FrameType.MIN1, "index", end, resample=True)
    await write_dfs(index_day, FrameType.DAY, "index", end)
    # 说明这一天的搞完了，需要从redis读出所有的bar，然后一起写入dfs
    if head is not None:
        await cache.sys.set(constants.BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d"))
    if tail is not None:
        await cache.sys.set(constants.BAR_SYNC_ARCHIVE_TAIl, tail.strftime("%Y-%m-%d"))
    await delete_daily_calibration_queue(stock_min, index_min, stock_day, index_day)
    # 检查tail是不是上一个交易日的，如果是上一个交易日，则需要清空redis
    pre_trade_day = TimeFrame.date2int(get_yesterday_or_pre_trade_day(now))
    tread_date = TimeFrame.date2int(tread_date)
    temp_now = get_now()
    if pre_trade_day == tread_date and temp_now < temp_now.replace(
        hour=9, minute=31, microsecond=0, second=0
    ):  # 时间小于当天9点31才能清，防止盘中被清掉数据
        await Stock.reset_cache()
        logger.info("上一个交易日数据已同步完毕, 已清空缓存")
    logger.info(f"持久化完成， params:{params}执行完毕")
    print(f"持久化完成， params:{params}执行完毕")
    return await run_daily_calibration_sync(now)


def get_yesterday_or_pre_trade_day(now):
    """获取昨天或者上一个交易日"""
    if TimeFrame.date2int(now) in TimeFrame.day_frames:
        pre_trade_day = TimeFrame.day_shift(now, -1)
    else:
        pre_trade_day = TimeFrame.day_shift(now, 0)
    return pre_trade_day


async def run_daily_calibration_sync(now):
    """凌晨2点数据同步，调用sync_day_bars，添加参数写minio和重采样
    然后需要往前追赶同步，剩余quota > 1天的量就往前赶，并在redis记录已经有daily_calibration_sync在运行了
    """
    head, tail = (
        await cache.sys.get(constants.BAR_SYNC_ARCHIVE_HEAD),
        await cache.sys.get(constants.BAR_SYNC_ARCHIVE_TAIl),
    )

    pre_trade_day = get_yesterday_or_pre_trade_day(now)
    if not head or not tail:
        # 任意一个缺失都不行
        logger.info("说明是首次同步，查找上一个已收盘的交易日")
        tread_date = datetime.datetime.combine(pre_trade_day, datetime.time(0, 0))
        head = tail = pre_trade_day

    else:
        # 说明不是首次同步，检查tail到现在有没有空洞
        tail_date = datetime.datetime.strptime(tail, "%Y-%m-%d")
        head_date = datetime.datetime.strptime(head, "%Y-%m-%d")
        count_frame = TimeFrame.count_frames(
            tail_date,
            pre_trade_day,
            FrameType.DAY,
        )
        if count_frame > 1:
            # 说明有空洞
            tail_date = datetime.datetime.combine(
                TimeFrame.day_shift(tail_date, 1), datetime.time(0, 0)
            )
            tread_date = tail = tail_date
            head = None

        else:
            # 说明没有空洞
            tread_date = head = datetime.datetime.combine(
                TimeFrame.day_shift(head_date, -1), datetime.time(0, 0)
            )
            tail = None
    # 检查时间是否小于于 2005年，大于则说明同步完成了
    day_frame = get_first_day_frame()
    if TimeFrame.date2int(tread_date) < day_frame:
        logger.info("所有数据已同步完毕")
        return True

    return await __daily_calibration_sync(tread_date, now, head=head, tail=tail)


@abnormal_master_report()
async def daily_calibration_sync():
    logger.info("每日数据校准已启动")
    now = get_now()
    sys.setrecursionlimit(10000)
    return await run_daily_calibration_sync(now)


@abnormal_master_report()
async def sync_day_bars():
    """
    收盘之后同步今天的数据, 下午三点的同步
    """
    now = get_now()
    start = now.replace(hour=9, minute=31, second=0, microsecond=0)
    end = now.replace(hour=15, minute=0, second=0, microsecond=0)

    params = {"start": start, "end": end, "n_bars": 240}
    queue_name = "day"

    task = Task(
        Events.OMEGA_DO_SYNC_DAY, queue_name, params, timeout=get_timeout(60 * 60 * 2)
    )
    await task.set_coefficient(240 * 2 + 4)

    return await task.run()


@abnormal_master_report()
async def sync_minute_bars():
    """盘中同步每分钟的数据
    1. 从redis拿到上一次同步的分钟数据
    2. 计算开始和结束时间
    """
    await asyncio.sleep(0.5)
    end = get_now().replace(second=0, microsecond=0)
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
    task = Task(
        Events.OMEGA_DO_SYNC_MIN,
        queue_name,
        params,
        timeout=get_timeout(timeout * n_bars),
    )
    await task.set_coefficient(n_bars)
    flag = await task.run()
    if flag:
        # 说明正常执行完的
        await cache.sys.hset(
            constants.BAR_SYNC_STATE_MINUTE, "tail", end.strftime("%Y-%m-%d %H:%M:00")
        )
    return flag


@abnormal_master_report()
async def sync_high_low_limit():
    """每天9点半之后同步一次今日涨跌停并写入redis"""
    timeout = get_timeout(60 * 10)
    end = get_now().replace(hour=15, minute=0, second=0, microsecond=0)
    if not TimeFrame.is_trade_day(end):
        print("非交易日，不同步")
        return False
    params = {"end": end}
    task = Task(Events.OMEGA_DO_SYNC_HIGH_LOW_LIMIT, "high_low_limit", params, timeout)
    await task.set_coefficient(1)
    await task.run()


async def delete_year_quarter_month_week_queue(stock, index):
    """
    清理校准同步的队列，防止数据重复
    """
    p = cache.temp.pipeline()
    p.delete(stock)
    p.delete(index)
    await p.execute()


async def __sync_year_quarter_month_week(tail_key, frame_type):
    year_quarter_month_week_calendar = {
        FrameType.WEEK: TimeFrame.int2date(TimeFrame.week_frames[0]),
        FrameType.MONTH: TimeFrame.int2date(TimeFrame.month_frames[0]),
        FrameType.QUARTER: TimeFrame.int2date(TimeFrame.quarter_frames[0]),
        FrameType.YEAR: TimeFrame.int2date(TimeFrame.year_frames[0]),
    }

    tail = await cache.sys.get(tail_key)
    now = get_now()
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
        task = Task(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK,
            queue_name,
            params,
            get_timeout(timeout=60 * 10),
            stock_queue=stock_queue,
            index_queue=index_queue,
        )
        # 设置系数
        await task.set_coefficient(2)
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
            constants.BAR_SYNC_WEEK_TAIl, FrameType.WEEK
        )
        logger.info(f"执行{FrameType.WEEK.value}完毕")

    if month:
        month = await __sync_year_quarter_month_week(
            constants.BAR_SYNC_MONTH_TAIl, FrameType.MONTH
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
        minute="0-30",
        name=f"{FrameType.MIN1.value}:11:0-30",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour="13-14",
        minute="1-59",
        name=f"{FrameType.MIN1.value}:13-14:*",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=15,
        name=f"{FrameType.MIN1.value}:15:00",
    )

    scheduler.add_job(
        sync_day_bars,
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
        daily_calibration_sync,
        "cron",
        hour=2,
        minute=5,
        name="daily_calibration_sync",
    )

    scheduler.add_job(
        sync_high_low_limit,
        "cron",
        hour=9,
        minute=31,
        name="sync_high_low_limit",
    )
