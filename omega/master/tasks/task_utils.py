import datetime
import itertools
import logging
import pickle
import traceback
from functools import wraps
from typing import AnyStr, List, Union

import cfg4py
from cfg4py.config import Config
from coretypes import FrameType, SecurityType
from omicron.dal import cache
from omicron.notify.mail import mail_notify
from omicron.models.timeframe import TimeFrame

from omega.core import constants
from omega.worker.dfs import Storage
from omega.core.constants import MINIO_TEMPORAL


logger = logging.getLogger(__name__)
cfg: Config = cfg4py.get_instance()


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
