#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import datetime
import itertools
import logging
import pickle
import time
import traceback
from functools import wraps
from typing import Any, AnyStr, Dict, List, Optional, Tuple, Union

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

from omega.core import constants
from omega.core.constants import MINIO_TEMPORAL
from omega.core.events import Events
from omega.worker.dfs import Storage

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()
work_state = {}  # work的状态


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
        *,
        end: datetime.datetime,
        frame_type: List[FrameType],
        timeout: int = 60,
        n_bars: int = None,
        recs_per_sec: int = 240,
    ):
        """
        Args:
            event: 发给worker的消息名
            name: 分配给worker的从redis中取数据的队列名
            frame_type: K线类型
            timeout: run需要等待的超时时间
            n_bars: 需要同步多少根K线，如果为None 则默认 分钟线取240根，其他所有类型取1根
            recs_per_sec: 每支证券将要同步的记录数。
            # expire_time: 如果设置，则在任务开始执行后的`expire_time`秒内，自动清除redis中保存的任务状态，以便下次重新启动任务。
        """
        # 读取待同步证券品种的队列名惟一，因此也可以作为任务的唯一标识名
        self.name = name
        self.event = event
        self.params = {}
        self.timeout = timeout
        self.n_bars = n_bars
        self.end = end
        self.frame_type = frame_type
        self._stock_scope = self.parse_bars_sync_scope(SecurityType.STOCK)
        self._index_scope = self.parse_bars_sync_scope(SecurityType.INDEX)

        self._recs_per_sec = recs_per_sec
        self.status = None

    def _state_key_name(self):
        # review: 最初这些名字被保存为成员变量。成员变量一般用来记录对象的状态和属性。如果只是一些可以通过计算得出来的中间属性，可应该考虑使用函数计算的方式（如果没有性能问题）。
        return f"{constants.TASK_PREFIX}.{self.name}.state"

    def _scope_key_name(self, typ: SecurityType, ft: FrameType, is_done: bool):
        if is_done:
            return (
                f"{constants.TASK_PREFIX}.{self.name}.scope.{typ.value}.{ft.value}.done"
            )

        return f"{constants.TASK_PREFIX}.{self.name}.scope.{typ.value}.{ft.value}"

    async def delete_state(self):
        """将任务状态进行删除"""
        key = self._state_key_name()
        await cache.sys.delete(key)

    async def delete_done(self):
        """删除已经完成的任务队列"""
        keys = []
        for typ, ft in itertools.product(
            [SecurityType.STOCK, SecurityType.INDEX], self.frame_type
        ):
            key_done = self._scope_key_name(typ, ft, is_done=True)
            keys.append(key_done)
        await cache.sys.delete(*keys)

    @property
    def recs_per_sec(self):  # pragma: no cover
        """当同步一支证券时，将向服务器请求多少条数据。"""
        return self._recs_per_sec

    @recs_per_sec.setter
    def recs_per_sec(self, value):
        self._recs_per_sec = value

    @classmethod
    def get_quota(cls):
        """获取quota的数量"""
        quota = 0
        for worker in work_state.values():
            quota += worker.get("quota", 0)
        return quota

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
                "error": state.get("error"),
                "worker_count": int(state.get("worker_count", 0)),
            }

        if field:  # pragma: no cover
            return state.get(field)
        else:
            return state

    def parse_bars_sync_scope(self, _type: SecurityType):
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
        end = self.end
        if isinstance(self.end, datetime.datetime):
            end = self.end.date()
        codes = Stock.choose_listed(end, [_type.value])
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
        else:  # pragma: no cover
            body = f"超时时间是：{self.timeout}"
        body += "\n\n================================================\n\n"
        body += "消费者得到的参数是：" + str(self.params)
        body += "\n\n================================================\n\n"
        quota = self.get_quota()
        body += f"剩余可用quota：{quota}"

        # review: 因为报告机制特别重要，所以不能因为读redis失败而导致发送失败。
        try:
            body += f"{await self.get_sync_failed_secs()}"
        except Exception as e:  # noqa   # pragma: no cover
            body += "获取失败的证券列表时发生异常：" + traceback.format_exc()

        logger.info(f"发送邮件subject:{subject}, body: {body}")
        await mail_notify(subject, body, html=True)

    async def update_state(self, **kwargs):
        """更新任务状态"""
        key = self._state_key_name()

        # aioredis cannot save bool directly
        # if isinstance(kwargs.get("is_running"), bool):
        #     kwargs["is_running"] = str(kwargs["is_running"])

        pl = cache.sys.pipeline()
        pl.hmset_dict(key, kwargs)
        # todo: 是使用expire还是timeout?或者使用auto_expire?
        # if self.expire_time is not None:
        pl.expire(key, self.timeout * 2)  # 自动设置超时时间
        await pl.execute()
        self.params.update({"state": key})

    def check_quota(self) -> Tuple:
        """检查quota是否足够完成本次同步

        Returns:
            返回Tuple(isok, spare quota, required quota)
        """
        # 只相加len的值应该快一点
        count = len(self._index_scope) + len(self._stock_scope)
        assert count > 0
        # 检查quota，通过证券数据 * 每支证券待同步的条数
        quota = self.get_quota()
        need = count * self._recs_per_sec
        return need < quota, quota, need

    async def update_sync_scope(self):
        """将待同步的证券代码列表写入redis队列，以便worker获取"""
        # todo 不同的frame_type 和 SecurityType的队列不一样，但是只写了一个队列，这样worker同步第二个frame_type时 就获取不到数据了
        pl = cache.sys.pipeline()
        keys = []
        keys_done = []
        for typ, ft in itertools.product(
            [SecurityType.STOCK, SecurityType.INDEX], self.frame_type
        ):
            key = self._scope_key_name(typ, ft, is_done=False)
            keys.append(key)
            # 记录成功完成同步的证券代码
            key_done = self._scope_key_name(typ, ft, is_done=True)
            pl.delete(key)
            pl.delete(key_done)
            keys_done.append(key_done)
            if typ == SecurityType.STOCK and self._stock_scope:
                pl.lpush(key, *self._stock_scope)
            elif typ == SecurityType.INDEX and self._index_scope:
                pl.lpush(key, *self._index_scope)

        await pl.execute()
        # 将队列名放进参数，worker需要用到
        self.params.update({"scope": keys})

    async def cleanup(self, success: bool = True):
        """在任务结束时（成功或失败）的清理操作"""

        if not success:
            state = await self._get_task_state()
            await self.send_email(state.get("error"))
        await self.delete_state()
        await self.delete_done()

    def get_params(self):
        self.params.update(
            {
                "timeout": self.timeout,
                "name": self.name,
                "frame_type": self.frame_type,
                "end": self.end,
                "n_bars": self.n_bars,
            }
        )
        return self.params

    async def run(self):
        """分配任务并发送emit通知worker开始执行，然后阻塞等待"""
        logger.info(f"{self.name}:{self.get_params()} 任务启动")
        if await self.is_running():
            self.status = False
            return self.status

        ok, spare, required = self.check_quota()
        if not ok:
            msg = f"quota不足，剩余quota：{spare}, 需要quota：{required}"
            await self.send_email(msg)
            self.status = False
            return self.status

        await self.update_state(is_running=1, worker_count=0)
        await self.update_sync_scope()

        # todo worker在同步中，会将bars追加到下面的队列中，因此在同步前需要清空队列，清空队列时仅清空本次需要用到的队列，根据frame_type和name

        await delete_temporal_bars(self.name, self.frame_type)

        await emit.emit(self.event, self.get_params())
        self.status = await self.check_done()
        return self.status

    async def check_done(self) -> bool:
        """等待worker完成任务

        Returns:
            任务是否完成
        """
        # review: 一般我们可以用t0来标识计时的起点
        t0 = time.time()
        ret = False
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
                    for ft in self.frame_type:
                        # 当for循环检查没有任何一次break时，则说明任务全部完成了。有任何一次
                        await asyncio.sleep(0.5)
                        done_index = await self.get_sync_done_secs(
                            SecurityType.INDEX, ft
                        )
                        if set(done_index) != set(self._index_scope):
                            break
                        done_stock = await self.get_sync_done_secs(
                            SecurityType.STOCK, ft
                        )
                        if set(done_stock) != set(
                            self._stock_scope
                        ):  # pragma: no cover
                            break
                    else:
                        # 说明执行完了
                        logger.info(f"params:{self.params},耗时：{time.time() - t0}")
                        print(f"params:{self.params},耗时：{time.time() - t0}")
                        ret = True
                        break
        except asyncio.exceptions.TimeoutError:  # pragma: no cover
            # review: 这里有一个pragma: nocover. 对异常分支不进行单元测试的问题是，万一这些异常真的出现，它们可能引起二次异常，从而最终导致程序崩溃 -- 这也许并不是我们想要的 -- 单元测试可以排除掉未处理的二次异常。
            logger.info("消费者超时退出")
            ret = False
        finally:
            await self.cleanup(ret)

        return ret

    async def get_sync_done_secs(
        self,
        typ: SecurityType,
        ft: FrameType,
    ) -> List:
        key = self._scope_key_name(typ, ft, is_done=True)
        return await cache.sys.lrange(key, 0, -1)

    async def get_sync_failed_secs(self) -> List[str]:
        """获取本次同步中失败的证券"""
        failed = []
        for ft in self.frame_type:
            done_index = await self.get_sync_done_secs(SecurityType.INDEX, ft)
            done_stock = await self.get_sync_done_secs(SecurityType.STOCK, ft)

            failed += list(
                set(self._index_scope) - set(done_index)
                | set(self._stock_scope) - set(done_stock)
            )

        return list(set(failed))


def get_bars_filename(
    prefix: SecurityType,
    dt: Union[datetime.datetime, datetime.date, AnyStr],
    frame_type: Union[FrameType, AnyStr],
) -> AnyStr:  # pragma: no cover
    """拼接bars的文件名
    如 get_bars_filename(SecurityType.Stock, datetime.datetime(2022,2,18), FrameType.MIN)
    Return: stock/1m/20220218
    """
    filename = []
    if isinstance(prefix, SecurityType) and prefix in (
        SecurityType.STOCK,
        SecurityType.INDEX,
    ):
        filename.append(prefix.value)
    else:
        raise TypeError(
            "prefix must be type SecurityType and in (SecurityType.STOCK, SecurityType.INDEX)"
        )

    if isinstance(frame_type, FrameType):
        filename.append(frame_type.value)
    elif isinstance(frame_type, str):
        filename.append(frame_type)
    else:
        raise TypeError("prefix must be type FrameType, str")
    if isinstance(dt, str):
        filename.append(TimeFrame.int2date(dt))
    elif isinstance(dt, datetime.datetime) or isinstance(dt, datetime.date):
        filename.append(str(TimeFrame.date2int(dt)))
    else:
        raise TypeError("dt must be type datetime, date, str, got type:%s" % type(dt))

    return "/".join(filename)


def get_trade_limit_filename(
    prefix: SecurityType, dt: Union[datetime.datetime, datetime.date, AnyStr]
):
    assert isinstance(prefix, SecurityType)
    assert isinstance(dt, (datetime.datetime, datetime.date, str))
    filename = [prefix.value, "trade_limit", str(TimeFrame.date2int(dt))]
    return "/".join(filename)


async def write_dfs(
    name: str,
    dt: datetime.datetime,
    frame_type: List[FrameType],
    resample: bool = False,
):
    """
    将校准同步/追赶同步时下载的数据写入块存储 - minio
    从redis 3号库 temp中读取出worker写入的数据并处理到一起写入dfs，因为如果不用master写的话，会导致文件成多个文件。

    Args:
        name: 任务名
        dt: 日期
        resample: 是否需要重采样  只重采样分钟线 到 5 15 30 60
        frame_type: k线类型
    Returns:

    """

    dfs = Storage()
    if dfs is None:  # pragma: no cover
        return

    for typ, ft in itertools.product(
        [SecurityType.STOCK, SecurityType.INDEX], frame_type
    ):
        queue_name = f"{MINIO_TEMPORAL}.{name}.{typ.value}.{ft.value}"

        # todo: structure/fields of the data? does it contains frametye or code?
        data = await cache.temp.lrange(queue_name, 0, -1, encoding=None)
        if not data:  # pragma: no cover
            return

        logger.info(f"queue_name:{queue_name},frame_type:{ft}")

        # todo: it's better to use another variable name for i and data
        all_bars = {}
        for item in data:
            bars = pickle.loads(item)
            assert isinstance(bars, dict)
            all_bars.update(bars)

        # todo: now resmaple is disabled
        binary = pickle.dumps(all_bars, protocol=cfg.pickle.ver)
        await dfs.write(get_bars_filename(typ, dt, ft), binary)
        # todo: let worker do the resample
        # if resample and ft == FrameType.MIN1 and 1 == 0:
        #     for to_frame in (
        #         FrameType.MIN5,
        #         FrameType.MIN15,
        #         FrameType.MIN30,
        #         FrameType.MIN60,
        #     ):
        #         # we need to support batch resample here
        #         resampled = Stock.resample(all_bars, FrameType.MIN1, to_frame)
        #         resampled_binary = pickle.dumps(resampled, protocol=cfg.pickle.ver)
        #         await dfs.write(get_bars_filename(typ, dt, to_frame), resampled_binary)

        await cache.temp.delete(queue_name)


async def run_daily_calibration_sync_task(task: BarsSyncTask):
    """

    Args:
        task: 需要运行的task实例
    """

    ret = await task.run()
    # 如果运行中出现错误，则中止本次同步
    if not ret:  # pragma: no cover
        return ret
    logger.info(f"daily_calibration -- params:{task.params} 已执行完毕，准备进行持久化")

    # 将raw数据写入块存储--minio
    # todo: disable resample for now
    await write_dfs(task.name, task.end, task.frame_type, resample=False)

    return True


def get_yesterday_or_pre_trade_day(now):
    """获取昨天或者上一个交易日"""
    # todo: could be replaced by TimeFrame.day_shift(now, 0)?
    if TimeFrame.date2int(now) in TimeFrame.day_frames:
        pre_trade_day = TimeFrame.day_shift(now, -1)
    else:
        pre_trade_day = TimeFrame.day_shift(now, 0)
    return pre_trade_day


async def get_sync_date():
    """计算校准（追赶）同步的日期

    追赶同步是系统刚建立不久时，数据库缺少行情数据，受限于每日从上游服务器取数据的quota，因此只能每日在quota限额内，进行小批量的同步，逐步把数据库缺失的数据补起来的一种同步。

    追赶同步使用与校准同步同样的实现，但优先级较低，只有在校准同步完成后，还有quota余额的情况下才进行。

    Returns:
        返回一个(sync_dt, head, tail)组成的元组，sync_dt是同步的日期，如果为None，则无需进行追赶同步。

    """
    while True:
        head, tail = (
            await cache.sys.get(constants.BAR_SYNC_ARCHIVE_HEAD),
            await cache.sys.get(constants.BAR_SYNC_ARCHIVE_TAIL),
        )

        # todo: check if it can get right sync_dt
        now = arrow.now().naive.date()
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
        day_frame = TimeFrame.day_frames[0]
        if TimeFrame.date2int(sync_dt) <= day_frame:
            logger.info("所有数据已同步完毕")
            # sync_dt = None
            break

        yield sync_dt, head, tail


async def get_daily_calibration_job_task(
    sync_dt: datetime.datetime,
):
    """
    获取凌晨校准同步的task实例
    Args:
        sync_dt: 需要同步的时间

    Returns:

    """
    end = sync_dt.replace(hour=15, minute=0, microsecond=0, second=0)
    # 检查 end 是否在交易日

    name = "calibration_sync"
    frame_type = [FrameType.MIN1, FrameType.DAY]
    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_DAILY_CALIBRATION,
        name=name,
        end=end,
        frame_type=frame_type,  # 需要同步的类型
        timeout=60 * 60 * 6,
        recs_per_sec=int((240 * 2 + 4) // 0.75),
    )
    return task


@abnormal_master_report()
async def daily_calibration_job():
    """scheduled task entry

    runs at every day at 2:00 am
    """
    logger.info("每日数据校准已启动")
    now = arrow.now().date()
    async for sync_dt, head, tail in get_sync_date():
        # 创建task
        task = await get_daily_calibration_job_task(sync_dt)
        success = await run_daily_calibration_sync_task(task)
        if not success:  # pragma: no cover
            break
        else:
            # 当天的校准同步已经完成，清除缓存。
            if sync_dt.date() == TimeFrame.day_shift(now, 0):
                await Stock.reset_cache()
            # 成功同步了`sync_dt`这一天的数据，更新 head 和 tail
            if head is not None:
                await cache.sys.set(
                    constants.BAR_SYNC_ARCHIVE_HEAD, head.strftime("%Y-%m-%d")
                )
            if tail is not None:
                await cache.sys.set(
                    constants.BAR_SYNC_ARCHIVE_TAIL, tail.strftime("%Y-%m-%d")
                )
            logger.info(f"{task.name}({task.end})同步完成,参数为{task.params}")

    logger.info("exit daily_calibration_job")


async def get_after_hour_sync_job_task() -> Optional[BarsSyncTask]:
    """获取盘后同步的task实例"""
    now = arrow.now().naive
    if not TimeFrame.is_trade_day(now):  # pragma: no cover
        print("非交易日，不同步")
        return
    end = TimeFrame.last_min_frame(now, FrameType.MIN1)
    if now < end:  # pragma: no cover
        logger.info("当天未收盘，禁止同步")
        return
    name = "day"

    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_DAY,
        name=name,
        frame_type=[FrameType.MIN1, FrameType.DAY],
        end=end,
        timeout=60 * 60 * 2,
        recs_per_sec=240 * 2 + 4,
    )
    return task


@abnormal_master_report()
async def after_hour_sync_job():
    """交易日盘后同步任务入口

    收盘之后同步今天的日线和分钟线
    """
    task = await get_after_hour_sync_job_task()
    if not task:
        return
    await task.run()
    return task


async def get_sync_minute_date():
    """获取这次同步分钟线的时间和n_bars
    如果redis记录的是11：30
    """
    end = arrow.now().naive.replace(second=0, microsecond=0)
    first = end.replace(hour=9, minute=30, second=0, microsecond=0)
    # 检查当前时间是否在交易时间内
    if not TimeFrame.is_trade_day(end):  # pragma: no cover
        print("非交易日，不同步")
        return False
    if end < first:  # pragma: no cover
        print("时间过早，不能拿到k线数据")
        return False

    end = TimeFrame.floor(end, FrameType.MIN1)
    tail = await cache.sys.get(constants.BAR_SYNC_MINUTE_TAIL)
    # tail = "2022-02-22 13:29:00"
    if tail:
        # todo 如果有，这个时间最早只能是今天的9点31分,因为有可能是昨天执行完的最后一次
        tail = datetime.datetime.strptime(tail, "%Y-%m-%d %H:%M:00")
        if tail < first:
            tail = first
    else:
        tail = first

    # 取上次同步截止时间+1 计算出n_bars
    tail = TimeFrame.floor(tail + datetime.timedelta(minutes=1), FrameType.MIN1)
    n_bars = TimeFrame.count_frames(tail, end, FrameType.MIN1)  # 获取到一共有多少根k线
    return end, n_bars


async def get_sync_minute_bars_task() -> Optional[BarsSyncTask]:
    """构造盘中分钟线的task实例"""
    ret = await get_sync_minute_date()
    if not ret:  # pragma: no cover
        return
    else:
        end, n_bars = ret
    name = "minute"
    timeout = 60
    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_MIN,
        name=name,
        frame_type=[FrameType.MIN1],
        end=end,
        timeout=timeout * n_bars,
        n_bars=n_bars,
        recs_per_sec=n_bars,
    )
    return task


async def run_sync_minute_bars_task(task: BarsSyncTask):
    """执行task的方法"""
    flag = await task.run()
    if flag:
        # 说明正常执行完的
        await cache.sys.set(
            constants.BAR_SYNC_MINUTE_TAIL,
            task.end.strftime("%Y-%m-%d %H:%M:00"),
        )

    return task


@abnormal_master_report()
async def sync_minute_bars():
    """盘中同步每分钟的数据
    1. 从redis拿到上一次同步的分钟数据
    2. 计算开始和结束时间
    """
    task = await get_sync_minute_bars_task()
    if not task:
        return
    await run_sync_minute_bars_task(task)


async def write_trade_price_limits_to_dfs(name: str, dt: datetime.datetime):
    """
    将涨跌停写入dfs

    Args:
        name: task的名字
        dt: 写入dfs的日期，用来作为文件名

    Returns:

    """
    dfs = Storage()
    if dfs is None:  # pragma: no cover
        return

    for typ, ft in itertools.product(
        [SecurityType.STOCK, SecurityType.INDEX], [FrameType.DAY]
    ):
        queue_name = f"{MINIO_TEMPORAL}.{name}.{typ.value}.{ft.value}"

        data = await cache.temp.lrange(queue_name, 0, -1, encoding=None)
        if not data:  # pragma: no cover
            return
        all_bars = []
        for item in data:
            bars = pickle.loads(item)
            assert isinstance(bars, np.ndarray)
            all_bars.append(bars)
        bars = np.concatenate(all_bars)
        binary = pickle.dumps(bars, protocol=cfg.pickle.ver)
        await dfs.write(get_trade_limit_filename(typ, dt), binary)
        await cache.temp.delete(queue_name)


async def run_sync_trade_price_limits_task(task: BarsSyncTask):
    """用来启动涨跌停的方法，接收一个task实例"""
    ret = await task.run()
    if not ret:
        # 执行失败需要删除数据队列
        await delete_temporal_bars(task.name, task.frame_type)
        return False
    await write_trade_price_limits_to_dfs(task.name, task.end)
    await cache.sys.set(
        constants.BAR_SYNC_TRADE_PRICE_TAIL, task.end.strftime("%Y-%m-%d")
    )


@abnormal_master_report()
async def sync_trade_price_limits():
    """每天9点半之后同步一次今日涨跌停并写入redis"""
    frame_type = FrameType.DAY
    async for sync_date in get_month_week_day_sync_date(
        constants.BAR_SYNC_TRADE_PRICE_TAIL, frame_type
    ):
        # 初始化task
        task = await get_month_week_sync_task(
            Events.OMEGA_DO_SYNC_TRADE_PRICE_LIMITS, sync_date, frame_type
        )
        # 持久化涨跌停到dfs
        await run_sync_trade_price_limits_task(task)


async def delete_temporal_bars(name: str, frame_types: List[FrameType]):
    """清理临时存储在redis中的行情数据

    这部分数据使用list来存储，因此，在每次同步之前，必须先清理redis中的list，防止数据重复。

    Args:
        frame_types: 帧类型，如FrameType.MIN1
    """
    assert isinstance(frame_types, list)

    p = cache.temp.pipeline()
    for t, ft in itertools.product(
        [SecurityType.STOCK, SecurityType.INDEX], frame_types
    ):
        key = f"{constants.MINIO_TEMPORAL}.{name}.{t.value}.{ft.value}"
        p.delete(key)
    await p.execute()


async def run_month_week_sync_task(tail_key: str, task: BarsSyncTask):
    """
    运行周月线task实例的方法
    Args:
        tail_key: 记录周月线在redis最后一天的日期 如redis记录 2010-10-10 则说明 2005-01-01至-2010-10-10的数据已经同步完毕
        task: task的实例

    Returns:

    """
    ret = await task.run()
    if not ret:
        return False
    await write_dfs(task.name, task.end, task.frame_type)
    await cache.sys.set(tail_key, task.end.strftime("%Y-%m-%d"))


async def get_month_week_day_sync_date(tail_key: str, frame_type: FrameType):
    """获取周月的同步日期，通常情况下，周月数据相对较少，一天的quota足够同步完所有的数据，所以直接从2005年开始同步至今
    做成生成器方式，不停的获取时间
    """
    year_quarter_month_week_calendar = {
        FrameType.DAY: TimeFrame.int2date(TimeFrame.day_frames[0]),
        FrameType.WEEK: TimeFrame.int2date(TimeFrame.week_frames[0]),
        FrameType.MONTH: TimeFrame.int2date(TimeFrame.month_frames[0]),
    }
    while True:
        tail = await cache.sys.get(tail_key)
        now = arrow.now().naive
        if not tail:
            tail = year_quarter_month_week_calendar.get(frame_type)
        else:
            tail = datetime.datetime.strptime(tail, "%Y-%m-%d")
            tail = TimeFrame.shift(tail, 1, frame_type)
        count_frame = TimeFrame.count_frames(
            tail,
            now.replace(hour=0, minute=0, second=0, microsecond=0),
            frame_type,
        )
        if count_frame >= 1:
            yield tail
        else:
            break


async def get_month_week_sync_task(
    event: str, sync_date: datetime.datetime, frame_type: FrameType
) -> BarsSyncTask:
    """

    Args:
        event: 事件名称，emit通过这个key发布消息到worker
        sync_date: 同步的时间
        frame_type: 同步的K线类型

    Returns:

    """
    name = frame_type.value

    task = BarsSyncTask(
        event=event,
        name=name,
        frame_type=[frame_type],
        end=sync_date,
        timeout=60 * 10,
    )
    task.recs_per_sec = 2
    return task


@abnormal_master_report()
async def sync_week_bars():
    """同步周线"""
    # 检查周线 tail
    frame_type = FrameType.WEEK
    async for sync_date in get_month_week_day_sync_date(
        constants.BAR_SYNC_WEEK_TAIL, frame_type
    ):
        # 初始化task
        task = await get_month_week_sync_task(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK, sync_date, frame_type
        )
        await run_month_week_sync_task(constants.BAR_SYNC_WEEK_TAIL, task)
        if not task.status:
            break


@abnormal_master_report()
async def sync_month_bars():
    """同步月线"""
    frame_type = FrameType.MONTH
    async for sync_date in get_month_week_day_sync_date(
        constants.BAR_SYNC_MONTH_TAIL, frame_type
    ):
        # 初始化task
        task = await get_month_week_sync_task(
            Events.OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK, sync_date, frame_type
        )
        await run_month_week_sync_task(constants.BAR_SYNC_MONTH_TAIL, task)
        if not task.status:
            break


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
        name="after_hour_sync_job",
    )
    scheduler.add_job(
        sync_week_bars,
        "cron",
        hour=2,
        minute=5,
        name="sync_week_bars",
    )
    scheduler.add_job(
        sync_month_bars,
        "cron",
        hour=2,
        minute=5,
        name="sync_month_bars",
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
