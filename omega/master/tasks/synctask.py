import asyncio
import datetime
import itertools
import logging
import time
import traceback
from functools import wraps
from typing import Any, Dict, List, Union

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
from omega.master.tasks.quota_utils import QuotaMgmt
from omega.master.tasks.task_utils import delete_temporal_bars

logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


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
        recs_per_sec: int = 240,  # 本地任务预计占用的配额
        quota_type: int = 1,  # 默认为75%的配额，剩下的25%给白天时间段的同步和更新
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
            quota_type: 配额类型，1表示75%的配额，2表示25%的配额
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
        self._quota_type = quota_type  # 1夜间校准和同步, 2白天
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
        subject = f"execution exception for master task {self.name}"
        if error:
            body = error
        else:  # pragma: no cover
            body = f"timeout parameter: {self.timeout}"
        body += "\n\n================================================\n\n"
        body += "all params in master task: " + str(self.params)
        body += "\n\n================================================\n\n"

        # review: 因为报告机制特别重要，所以不能因为读redis失败而导致发送失败。
        try:
            body += f"{await self.get_sync_failed_secs()}"
        except Exception as e:  # noqa   # pragma: no cover
            body += "failed to get unfinished security list: " + traceback.format_exc()

        logger.info(f"send mail, subject: {subject}, body: {body}")
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

        # 检查quota，通过证券数据 * 每支证券待同步的条数，只相加len的值应该快一点
        sum1 = len(self._index_scope) + len(self._stock_scope)
        count = sum1 * self._recs_per_sec
        ok, spare, required = QuotaMgmt.check_quota(self._quota_type, count)
        if not ok:
            msg = f"quota insufficient, remaining: {spare}, quota required: {required}"
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


def master_syncbars_task():
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
                subject = f"exception for master task {f.__name__}"
                body = f"detailed information: \n{traceback.format_exc()}"
                traceback.print_exc()
                await mail_notify(subject, body, html=True)

        return decorated_function

    return inner
