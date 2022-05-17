import asyncio
import datetime
import logging
import time
import traceback
from functools import wraps
from threading import Lock
from typing import Any, AnyStr, Dict, List, Optional, Tuple, Union

import async_timeout
import cfg4py
from cfg4py.config import Config
from omicron.dal import cache
from omicron.notify.mail import mail_notify
from pyemit import emit

from omega.core import constants
from omega.master.tasks.quota_utils import QuotaMgmt

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


class SecuritySyncTask:
    def __init__(
        self,
        event: str,
        name: str,
        end: datetime.datetime,
        timeout: int = 60,
        recs_per_task: int = 240,  # 本地任务预计占用的配额
        quota_type: int = 1,  # 默认为75%的配额，剩下的25%给白天时间段的同步和更新
    ):
        """
        Args:
            event: 发给worker的消息名
            name: 分配给worker的从redis中取数据的队列名
            timeout: run需要等待的超时时间
            quota_type: 配额类型，1表示75%的配额，2表示25%的配额
        """
        # 读取待同步证券品种的队列名惟一，因此也可以作为任务的唯一标识名
        self.name = name
        self.event = event
        self.params = {}
        self.timeout = timeout
        self.end = end

        self.recs_per_task = recs_per_task
        self.quota_type = quota_type  # 1夜间校准和同步, 2白天
        self.status = None

    def _state_key_name(self):
        return f"{constants.TASK_SECS_PREFIX}.{self.name}.state"

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

    async def _get_task_state(self) -> Union[Dict, Any]:
        """从redis中获取任务状态"""
        key = self._state_key_name()
        state = await cache.sys.hgetall(key)
        if len(state) != 0:
            state = {
                "is_running": state.get("is_running") is not None,
                "done_count": int(state.get("done_count", 0)),
                "error": state.get("error"),
                "worker_count": int(state.get("worker_count", 0)),
            }

        return state

    async def update_state(self, **kwargs):
        """更新任务状态"""
        key = self._state_key_name()

        pl = cache.sys.pipeline()
        pl.hmset_dict(key, kwargs)

        pl.expire(key, self.timeout * 2)  # 自动设置超时时间
        await pl.execute()
        self.params.update({"state": key})

    async def cleanup(self, success: bool = True):
        """在任务结束时（成功或失败）的清理操作"""
        if not success:
            state = await self._get_task_state()
            await self.send_email(state.get("error"))
        await self.delete_state()

    def get_params(self):
        self.params.update(
            {
                "timeout": self.timeout,
                "name": self.name,
                "end": self.end,
            }
        )
        return self.params

    async def run(self):
        """分配任务并发送emit通知worker开始执行，然后阻塞等待"""
        logger.info(f"{self.name}:{self.get_params()} 任务启动")
        if await self.is_running():
            self.status = False
            return self.status

        ok, spare, required = QuotaMgmt.check_quota(self.quota_type, self.recs_per_task)
        if not ok:
            msg = f"quota不足，剩余quota：{spare}, 需要quota：{required}"
            await self.send_email(msg)
            self.status = False
            return self.status

        await self.update_state(is_running=1, worker_count=0)

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
                    if error is not None or not is_running:  # 异常退出
                        ret = False
                        break
                    if state.get("done_count") > 0:  # 执行完毕
                        ret = True
                        logger.info(f"params:{self.params},耗时：{time.time() - t0}")
                        break
        except asyncio.exceptions.TimeoutError:  # pragma: no cover
            logger.info("消费者超时退出")
            ret = False
        finally:
            await self.cleanup(ret)

        return ret

    async def send_email(self, error=None):
        subject = f"执行{self.name}时异常！"
        if error:
            body = error
        else:  # pragma: no cover
            body = f"超时时间是：{self.timeout}"
        body += "\n\n================================================\n\n"
        body += "消费者得到的参数是：" + str(self.params)
        body += "\n\n================================================\n\n"
        quota, total = QuotaMgmt.get_quota()
        body += f"剩余可用quota：{quota}/{total}"

        logger.info(f"发送邮件subject:{subject}, body: {body}")
        await mail_notify(subject, body, html=True)


def master_secs_task():
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
