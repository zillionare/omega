import asyncio
import datetime
import itertools
import logging
import time
import traceback
from functools import wraps
from threading import Lock
from typing import Any, AnyStr, Dict, List, Optional, Tuple, Union

import arrow
import async_timeout
import cfg4py
from cfg4py.config import Config
from coretypes import FrameType, SecurityType
from omicron.dal import cache
from omicron.models.stock import Stock
from omicron.notify.mail import mail_notify
from pyemit import emit

from omega.core import constants
from omega.core.events import Events
from omega.master.tasks.quota_utils import quota_lock, today_quota_stat, work_state
from omega.master.tasks.task_utils import delete_temporal_bars

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class SecuritySyncTask:
    def __init__(
        self,
        event: str,
        name: str,
        *,
        end: datetime.datetime,
        timeout: int = 60,
        recs_per_sec: int = 240,  # 本地任务预计占用的配额
        quota_type: int = 1,  # 默认为75%的配额，剩下的25%给白天时间段的同步和更新
    ):
        """
        Args:
            event: 发给worker的消息名
            name: 分配给worker的从redis中取数据的队列名
            timeout: run需要等待的超时时间
            recs_per_sec: 每支证券将要同步的记录数。
            quota_type: 配额类型，1表示75%的配额，2表示25%的配额
        """
        # 读取待同步证券品种的队列名惟一，因此也可以作为任务的唯一标识名
        self.name = name
        self.event = event
        self.params = {}
        self.timeout = timeout
        self.end = end

        self._recs_per_sec = recs_per_sec
        self._quota_type = quota_type  # 1夜间校准和同步, 2白天
        self.status = None

    def _state_key_name(self):
        # review: 最初这些名字被保存为成员变量。成员变量一般用来记录对象的状态和属性。如果只是一些可以通过计算得出来的中间属性，可应该考虑使用函数计算的方式（如果没有性能问题）。
        return f"{constants.TASK_PREFIX}.{self.name}.state"

    async def delete_state(self):
        """将任务状态进行删除"""
        key = self._state_key_name()
        await cache.sys.delete(key)

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
        key = self._state_key_name()
        # always return a dict
        state = await cache.sys.hgetall(key)
        if len(state) != 0:
            state = {
                "is_running": state.get("is_running") is not None,
                "done_count": int(state.get("done_count", 0)),
                "error": state.get("error"),
                "worker_count": int(state.get("worker_count", 0)),
            }

        if field:  # pragma: no cover
            return state.get(field)
        else:
            return state

    async def send_email(self, error=None):
        subject = f"执行{self.name}时异常！"
        if error:
            body = error
        else:  # pragma: no cover
            body = f"超时时间是：{self.timeout}"
        body += "\n\n================================================\n\n"
        body += "消费者得到的参数是：" + str(self.params)
        body += "\n\n================================================\n\n"
        quota, total = self.get_quota()
        body += f"剩余可用quota：{quota}/{total}"

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

        pl = cache.sys.pipeline()
        pl.hmset_dict(key, kwargs)

        pl.expire(key, self.timeout * 2)  # 自动设置超时时间
        await pl.execute()
        self.params.update({"state": key})

    @classmethod
    def get_quota(cls):
        """获取quota的数量，需要根据worker对应的impl里面的账号计算，目前暂时支持单个impl进程"""
        quota = 0
        total = 0
        for worker in work_state.values():
            quota = worker.get("quota", 0)
            total = worker.get("total", 0)
            break  # 当前只会有一个worker

        return (quota, total)

    def update_quota(self, quota: int, total: int):
        today = arrow.now().naive.date()

        # 获取最新的quota信息
        quota, total = self.get_quota()
        if today_quota_stat["date"] is None or today_quota_stat["date"] != today:
            today_quota_stat["date"] = today  # 初始化当天的配额
            reserved = int(total * 0.25)  # 保留25%给白天同步使用
            if reserved > quota:
                today_quota_stat["quota1"] = 0
                today_quota_stat["quota2"] = quota
            else:
                today_quota_stat["quota1"] = quota - reserved
                today_quota_stat["quota2"] = reserved
        else:
            q1 = today_quota_stat["quota1"]
            q2 = today_quota_stat["quota2"]
            if quota < q1 + q2:  # 有其他任务占用了quota，需要重新计算
                delta = (q1 + q2) - quota
                q1 -= delta  # 扣除到晚上的配额上
                if q1 < 0:  # 不够扣除，则扣除到白天的配额上
                    q2 = quota  # 配额全部给白天的任务
                    q1 = 0
                today_quota_stat["quota1"] = q1
                today_quota_stat["quota2"] = q2

    def check_quota(self) -> Tuple:
        """检查quota是否足够完成本次同步
        Returns:
            返回Tuple(isok, spare quota, required quota)
        """
        # 证券同步的数目是固定的，如果增加期货，需要手动修改Task初始化时的recs_per_sec参数
        if self.event == Events.OMEGA_DO_SYNC_SECURITIES:
            count = 1
        else:
            # 只相加len的值应该快一点
            count = len(self._index_scope) + len(self._stock_scope)
        assert count > 0
        need = count * self._recs_per_sec

        # 检查quota，通过证券数据 * 每支证券待同步的条数
        global today_quota_stat, quota_lock
        try:
            quota_lock.acquire()
            self.update_quota()
            q1 = today_quota_stat["quota1"]
            q2 = today_quota_stat["quota2"]
            if self._quota_type == 1:  # calibration tasks
                if q1 > need:
                    today_quota_stat["quota1"] = q1 - need
                    return True, q1 - need, need
                else:
                    return False, q1, need
            else:
                if q2 > need:
                    today_quota_stat["quota2"] = q2 - need
                    return True, q2 - need, need
                else:
                    return False, q2, need
        finally:
            quota_lock.release()

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

        await delete_temporal_bars(self.name, self.frame_type)

        await emit.emit(self.event, self.get_params())
        self.status = await self.check_done()
        return self.status

    async def check_done(self) -> bool:
        """等待worker完成任务
        Returns:
            任务是否完成
        """
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
                        ret = True
                        break
        except asyncio.exceptions.TimeoutError:  # pragma: no cover
            logger.info("消费者超时退出")
            ret = False
        finally:
            await self.cleanup(ret)

        return ret


def master_task_report():
    def inner(f):
        @wraps(f)
        async def decorated_function():
            try:
                ret = await f()
                return ret
            except Exception as e:  # pragma: no cover
                logger.exception(e)
                subject = f"执行生产者{f.__name__}时发生异常"
                body = f"详细信息：\n{traceback.format_exc()}"
                traceback.print_exc()
                await mail_notify(subject, body, html=True)

        return decorated_function

    return inner
