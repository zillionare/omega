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

    def _task_key_name(self):
        return f"{constants.TASK_SECS_PREFIX}.{self.name}.lock"

    async def delete_state(self):
        """将任务状态进行删除"""
        key = self._state_key_name()
        await cache.sys.delete(key)

        key = self._task_key_name()
        await cache.sys.delete(key)

    async def is_running(self) -> bool:
        key = self._task_key_name()
        rc = await cache.sys.set(
            key,
            self.name,
            expire=self.timeout + 45,  # 比state的超时时间多15秒
            exist="SET_IF_NOT_EXIST",
        )
        if rc == 1:
            return False
        else:
            msg = f"检测到{self.name}下有正在运行的任务，本次任务不执行"
            logger.info(msg)
            return True

    async def _get_task_state(self) -> Union[Dict, Any]:
        """从redis中获取任务状态"""
        key = self._state_key_name()
        state = await cache.sys.hgetall(key)
        if len(state) != 0:
            state = {
                "status": int(state.get("status", 0)),
                "done_count": int(state.get("done_count", 0)),
                "error": state.get("error"),
                "worker_count": int(state.get("worker_count", 0)),
            }

        return state

    async def init_state(self, **kwargs):
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
            msg = f"quota insufficient, remaining: {spare}, quota required: {required}"
            await self.send_email(msg)
            self.status = False
            return self.status

        await self.init_state(status=0, worker_count=0)

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
                    task_status, error = state.get("status"), state.get("error")
                    if error is not None or task_status == -1:  # 异常退出
                        ret = False
                        break
                    if state.get("done_count") > 0:  # 执行完毕
                        ret = True
                        logger.info(f"params:{self.params},耗时：{time.time() - t0}")
                        break
                    if task_status == 1:
                        break
        except asyncio.exceptions.TimeoutError:  # pragma: no cover
            logger.info("消费者超时退出")
            ret = False
        finally:
            await self.cleanup(ret)

        return ret

    async def send_email(self, error=None, additional_info=None):
        subject = f"execution exception: {self.name}"
        if error:
            body = error
        else:  # pragma: no cover
            body = f"timeout parameter: {self.timeout}"
        body += "\n\n================================================\n\n"
        body += "all parameters in worker task: " + str(self.params)
        body += "\n\n================================================\n\n"
        if additional_info:
            body += additional_info

        logger.info(f"send mail: subject: {subject}, body: {body}")
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
                subject = f"exception when execute master task: {f.__name__}"
                body = f"detailed information: \n{traceback.format_exc()}"
                traceback.print_exc()
                await mail_notify(subject, body, html=True)

        return decorated_function

    return inner
