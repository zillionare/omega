#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import datetime
import logging
import pickle
import time

import arrow
import async_timeout
import cfg4py
import numpy as np
from cfg4py.config import Config
from omicron import cache
from omicron.core.types import FrameType, SecurityType
from omicron.models.calendar import Calendar as cal
from omicron.models.stock import Stock
from pyemit import emit

from omega.core import constants
from omega.core.events import Events
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from omega.worker.dfs import Storage

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


def get_now():
    return datetime.datetime.now()


class Task:
    def __init__(
        self,
        channel: str,
        task_queue_name: str,
        params: dict,
        timeout: int = 60,
        stock_queue: str = None,
        index_queue: str = None,
    ):
        """
        Args:
            channel: emit的事件名
            task_queue_name: 分配给worker的从redis中取数据的队列名
            params: 发送给emit的参数
            timeout: run需要等待的超时时间
            stock_queue: 校验同步的股票队列
            index_queue: 校验同步的指数队列
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

    @classmethod
    def get_queue_name(cls, task_queue_name):
        return (
            f"{constants.TASK_PREFIX}.{task_queue_name}.state",
            f"{constants.TASK_PREFIX}.{task_queue_name}.scope",
            f"{constants.TASK_PREFIX}.{task_queue_name}.fail",
        )

    async def delete_state(self):
        await cache.sys.delete(self.state)

    @property
    async def is_running(self):
        # 检查这个任务是否已经在运行
        is_running = await cache.sys.hget(self.state, "is_running")
        if is_running is not None:
            msg = f"检测到{self.state}下有正在运行的任务，本次任务不执行"
            logger.info(msg)
            print(msg)
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
        if include:
            include = list(filter(lambda x: x, cfg.omega.sync.bars.include.split(" ")))
            codes.extend(include)
        return codes

    async def generate_task(self):
        stock = await self.__generate_task(SecurityType.STOCK.value)
        index = await self.__generate_task(SecurityType.INDEX.value)
        self.__stock = stock
        self.__index = index
        self.__task_list = list(stock + index)

    async def tasks(self):
        if not self.__task_list:
            await self.generate_task()
        return self.__task_list

    async def run(self):
        """分配任务并发送emit通知worker开始执行，然后阻塞等待"""
        if await self.is_running:
            return False
        p = cache.sys.pipeline()
        p.hset(self.state, "is_running", 1)
        p.expire(self.state, self.timeout * 5)
        await p.execute()
        tasks = await self.tasks()
        count = len(tasks)
        p = cache.sys.pipeline()
        p.hmset(self.state, "task_count", count)
        if self.stock_queue is None or self.index_queue is None:
            p.delete(self.scope)
            p.lpush(self.scope, *tasks)
        else:
            p.delete(self.stock_queue)
            p.delete(self.index_queue)
            p.lpush(self.stock_queue, *self.__stock)
            p.lpush(self.index_queue, *self.__index)
            self.params.update(
                {"stock_queue": self.stock_queue, "index_queue": self.index_queue}
            )
        await p.execute()
        self.params.update({"__scope": self.scope, "__state": self.state})
        await emit.emit(self.channel, self.params)
        await self.check_done(count)
        await self.delete_state()

    async def check_done(self, count: int):
        """
        等待fether完成任务
        Args:
            count: 需要等待完成的任务的条数

        Returns:

        """
        s = time.time()
        while True:
            try:
                async with async_timeout.timeout(self.timeout):
                    while True:
                        done_count = await cache.sys.hget(self.state, "done_count")
                        if done_count is None or int(done_count) != count:
                            await asyncio.sleep(1)
                        else:
                            print(f"{self.scope}耗时：{time.time() - s}")
                            return
            except asyncio.exceptions.TimeoutError:
                # 从失败列表里把所有数据拉出来，
                p = cache.sys.pipeline()
                p.lrange(self.fail, 0, -1)
                p.delete(self.fail)
                fails, _ = await p.execute()
                if fails:
                    print(fails)
                print("超时了, 继续等待，并发送报警邮件")


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


async def sync_calendar():
    """从上游服务器获取所有交易日，并计算出周线帧和月线帧

    Returns:
    """
    trade_days = await aq.get_all_trade_days()
    if trade_days is None or len(trade_days) == 0:
        logger.warning("failed to fetch trade days.")
        return None

    cal.day_frames = [cal.date2int(x) for x in trade_days]
    weeks = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.weekday() < last.weekday() or (cur - last).days >= 7:
            weeks.append(last)
        last = cur

    if weeks[-1] < last:
        weeks.append(last)

    cal.week_frames = [cal.date2int(x) for x in weeks]
    await cache.save_calendar("week_frames", map(cal.date2int, weeks))

    months = []
    last = trade_days[0]
    for cur in trade_days:
        if cur.day < last.day:
            months.append(last)
        last = cur
    months.append(last)

    cal.month_frames = [cal.date2int(x) for x in months]
    await cache.save_calendar("month_frames", map(cal.date2int, months))
    logger.info("trade_days is updated to %s", trade_days[-1])


async def sync_security_list():
    """更新证券列表

    注意证券列表在AbstractQuotesServer取得时就已保存，此处只是触发
    """
    secs = await aq.get_security_list()
    logger.info("%s secs are fetched and saved.", len(secs))


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


async def write_dfs(
    queue_name: str, frame_type: FrameType, prefix: str, dt: datetime.datetime
):
    """
    Args:
        queue_name: 需要从那个队列中取数据写入dfs
        frame_type: 帧类型，对应 1d, 1m
        prefix: dfs中的前缀 stock, index
        dt: 日期

    Returns:

    """
    dfs = Storage()
    if dfs is None:
        return
    p = cache.temp.pipeline()
    p.lrange(queue_name, 0, -1, encoding=None)
    p.delete(queue_name)
    data, _ = await p.execute()

    bars = [pickle.loads(i) for i in data]
    # data = pickle.loads(data)
    bars = np.concatenate(bars)
    binary = pickle.dumps(bars, protocol=cfg.pickle.ver)
    await dfs.write(binary, prefix, dt, frame_type)


async def __daily_calibration_sync(
    tread_date: datetime.datetime,
    head: datetime.datetime = None,
    tail: datetime.datetime = None,
):
    """

    Args:
        tread_date: 本次需要校验的交易日时间对象
        head: 已经同步的最早时间
        tail: 已经同步的最晚时间

    Returns:

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
        60 * 60 * 6,
        stock_queue=stock_queue,
        index_queue=index_queue,
    )
    # 检查head和tail
    # 检查tail和最近一个交易日有没有空洞，如果有空洞，先同步空洞
    total = len(await task.tasks())
    quota = await aq.get_quota()
    # 检查quota够不够，如果不够则return
    if quota * 0.75 < total * 240 * 4:
        print("quota不够，返回")
        return
    await delete_daily_calibration_queue(stock_min, index_min, stock_day, index_day)
    ret = await task.run()
    if ret is not None:
        return

    # 读出来 写dfs
    await write_dfs(stock_min, FrameType.MIN1, "stock", end)
    await write_dfs(stock_day, FrameType.DAY, "stock", end)
    # todo 读取指数数据
    # await write_dfs(index_min, FrameType.MIN1, "index", end)
    # await write_dfs(index_day, FrameType.DAY, "index", end)
    # 说明这一天的搞完了，需要从redis读出所有的bar，然后一起写入dfs
    if head is not None:
        await cache.sys.set(constants.BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d"))
    if tail is not None:
        await cache.sys.set(constants.BAR_SYNC_ARCHIVE_TAIl, tail.strftime("%Y-%m-%d"))
    await delete_daily_calibration_queue(stock_min, index_min, stock_day, index_day)
    await daily_calibration_sync()


async def daily_calibration_sync():
    """凌晨2点数据同步，调用sync_day_bars，添加参数写minio和重采样
    然后需要往前追赶同步，剩余quota > 1天的量就往前赶，并在redis记录已经有daily_calibration_sync在运行了
    """
    head, tail = await cache.sys.get(
        constants.BAR_SYNC_ARCHIVE_HEAD
    ), await cache.sys.get(constants.BAR_SYNC_ARCHIVE_TAIl)
    now = get_now()

    if not head or not tail:
        # 任意一个缺失都不行
        print("说明是首次同步，查找上一个已收盘的交易日")
        pre_trade_day = cal.day_shift(now, -1)
        tread_date = datetime.datetime.combine(pre_trade_day, datetime.time(0, 0))
        head = tail = pre_trade_day

    else:
        # 说明不是首次同步，检查tail到现在有没有空洞
        tail_date = datetime.datetime.strptime(tail, "%Y-%m-%d")
        head_date = datetime.datetime.strptime(head, "%Y-%m-%d")
        if (
            cal.count_frames(
                tail_date,
                now.replace(hour=0, minute=0, second=0, microsecond=0),
                FrameType.DAY,
            )
            - 1
            > 1
        ):
            tread_date = tail = tail_date
            head = None

        else:
            tread_date = head = cal.day_shift(head_date, -1)
            tail = None
    # 检查时间是否小于于 2005年，大于则说明同步完成了
    if cal.date2int(tread_date) < cal.day_frames[0]:
        print("所有数据已同步完毕")
        return

    if not cal.is_trade_day(tread_date):
        print("非交易日，不同步")
        if head is not None:
            await cache.sys.set(
                constants.BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d")
            )
        if tail is not None:
            await cache.sys.set(
                constants.BAR_SYNC_ARCHIVE_TAIl, tail.strftime("%Y-%m-%d")
            )
        await daily_calibration_sync()
    else:
        await __daily_calibration_sync(tread_date, head=head, tail=tail)


async def sync_day_bars():
    """
    收盘之后同步今天的数据, 下午三点的同步
    """
    now = get_now()
    start = now.replace(hour=9, minute=31, second=0, microsecond=0)
    end = now.replace(hour=15, minute=0, second=0, microsecond=0)

    params = {"start": start, "end": end, "n_bars": 240}
    queue_name = "day"

    task = Task(Events.OMEGA_DO_SYNC_DAY, queue_name, params, timeout=60 * 60 * 2)
    await task.run()


async def sync_minute_bars():
    """盘中同步每分钟的数据
    1. 从redis拿到上一次同步的分钟数据
    2. 计算开始和结束时间
    """
    end = get_now().replace(second=0, microsecond=0)
    first = end.replace(hour=9, minute=31, second=0, microsecond=0)
    # 检查当前时间是否在交易时间内
    if end.hour * 60 + end.minute not in cal.ticks[FrameType.MIN1]:
        if 11 <= end.hour < 13:
            end = end.replace(hour=11, minute=30)
        else:
            end = end.replace(hour=15, second=0, minute=0)

    if not cal.is_trade_day(end):
        print("非交易日，不同步")
        return

    queue_name = "minute"
    tail = await cache.sys.hget(constants.BAR_SYNC_STATE_MINUTE, "tail")
    if not tail:
        # 如果么有end,则取今天的9点31分
        tail = first
    else:
        # todo 如果有，这个时间最早只能是今天的9点31分,因为有可能是昨天执行完的最后一次
        tail = datetime.datetime.strptime(tail, "%Y-%m-%d %H:%M:00")
        if tail < first:
            tail = first

    n_bars = cal.count_frames(tail, end, FrameType.MIN1)  # 获取到一共有多少根k线
    if n_bars < 1:
        msg = "k线数量小于1 不同步"
        logger.info(msg)
        print(msg)
        return

    params = {"start": tail, "end": end, "n_bars": n_bars}
    task = Task(Events.OMEGA_DO_SYNC_MIN, queue_name, params)
    flag = await task.run()
    if flag is not None:
        # 说明正常执行完的
        await cache.sys.hset(
            constants.BAR_SYNC_STATE_MINUTE, "tail", end.strftime("%Y-%m-%d %H:%M:00")
        )


async def sync_high_low_limit():
    """每天9点半之后同步一次今日涨跌停并写入redis"""
    timeout = 60 * 10
    end = get_now().replace(hour=15, minute=0, second=0, microsecond=0)
    if not cal.is_trade_day(end):
        print("非交易日，不同步")
    params = {"end": end}
    task = Task(Events.OMEGA_DO_SYNC_HIGH_LOW_LIMIT, "high_low_limit", params, timeout)
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
    tail = await cache.sys.get(tail_key)
    now = get_now()
    if not tail:
        # 如果么有end,则取今天的9点31分
        tail = cal.int2date(cal.week_frames[0])
    else:
        tail = datetime.datetime.strptime(tail, "%Y-%m-%d")
        tail = cal.shift(tail, 1, frame_type)
    # 判断week_tail到现在有没有空洞
    if (
        cal.count_frames(
            tail,
            now.replace(hour=0, minute=0, second=0, microsecond=0),
            frame_type,
        )
        - 1
        > 1
    ):
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
            stock_queue=stock_queue,
            index_queue=index_queue,
        )
        total = len(await task.tasks())
        quota = await aq.get_quota()
        # 检查quota够不够，如果不够则return
        if quota - 999852 < total * 4:
            print("quota不够，返回")
            return False
        await delete_year_quarter_month_week_queue(stock_data, index_data)
        ret = await task.run()
        if ret is not None:
            await delete_year_quarter_month_week_queue(stock_data, index_data)
            return False
        await write_dfs(stock_data, frame_type, "stock", tail)
        # await write_dfs(index_week, FrameType.WEEK, "index", week_tail)
        await delete_year_quarter_month_week_queue(stock_data, index_data)
        await cache.sys.set(tail_key, tail.strftime("%Y-%m-%d"))
    return True


async def sync_year_quarter_month_week():
    """同步年月日周"""
    # 检查周线 tail
    if not await __sync_year_quarter_month_week(
        constants.BAR_SYNC_WEEK_TAIl, FrameType.WEEK
    ):
        return
    if not await __sync_year_quarter_month_week(
        constants.BAR_SYNC_MONTH_TAIl, FrameType.MONTH
    ):
        return
    # await __sync_year_quarter_month_week(constants.BAR_SYNC_QUARTER_TAIl, FrameType.QUARTER)
    if not await __sync_year_quarter_month_week(
        constants.BAR_SYNC_YEAR_TAIl, FrameType.YEAR
    ):
        return
    await sync_year_quarter_month_week()


async def load_cron_task(scheduler):
    h, m = map(int, cfg.omega.sync.security_list.split(":"))
    scheduler.add_job(
        sync_calendar,
        "cron",
        hour=h,
        minute=m,
        args=("calendar",),
        name="sync_calendar",
    )
    scheduler.add_job(
        sync_security_list,
        "cron",
        name="sync_security_list",
        hour=h,
        minute=m,
    )
    # 盘中的实时同步
    scheduler.add_job(
        sync_calendar,
        "cron",
        hour=h,
        minute=m,
        args=("calendar",),
        name="sync_calendar",
    )
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
        minute="*",
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
