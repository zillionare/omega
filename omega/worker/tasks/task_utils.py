import datetime
import logging
import pickle
from typing import Dict, Tuple, Union

import cfg4py
import numpy as np
from coretypes import FrameType, SecurityType
from omicron import cache
from omicron.extensions.decimals import math_round
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame

from omega.core.constants import MINIO_TEMPORAL, TASK_PREFIX
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as fetcher
from omega.worker.tasks.fetchers import fetch_bars

cfg = cfg4py.get_instance()

logger = logging.getLogger(__name__)


async def cache_init():
    """初始化缓存 交易日历和证券代码

    当系统第一次启动（cold boot)时，cache中并不存在证券列表和日历。而在`omicron.init`时，又依赖这些信息。因此，我们需要在Omega中加入冷启动处理的逻辑如下。
    """
    if not await cache.security.exists(f"calendar:{FrameType.DAY.value}"):
        await fetcher.get_all_trade_days()
    if not await cache.security.exists("security:all"):
        now = datetime.date.today()
        last_trade_date = TimeFrame.day_shift(now, 0)
        securities = await fetcher.get_security_list(last_trade_date)
        if securities is None:
            raise Exception("获取证券列表失败")
        Security.update_secs_cache(securities)


async def sync_params_analysis(typ: SecurityType, ft: FrameType, params: Dict) -> Tuple:
    """
    解析同步的参数
    Args:
        typ: 证券类型 如 stock  index
        ft: k线类型
        params: master发过来的参数

    Returns: Tuple(任务名称，k线根数，任务队列名，完成的队列名，限制单单次查询的数量)

    """
    name = params.get("name")
    n = params.get("n_bars")
    if not n:
        if ft == FrameType.MIN1:
            n = 240
        elif ft == FrameType.MIN5:
            n = 240 // 5
        elif ft == FrameType.MIN15:
            n = 240 // 15
        elif ft == FrameType.MIN30:
            n = 240 // 30
        elif ft == FrameType.MIN60:
            n = 240 // 60
        else:
            n = 1

    queue = f"{TASK_PREFIX}.{name}.scope.{typ.value}.{ft.value}"
    done_queue = f"{TASK_PREFIX}.{name}.scope.{typ.value}.{ft.value}.done"
    limit = await fetcher.result_size_limit("bars")
    return name, n, queue, done_queue, limit


async def sync_to_cache(typ: SecurityType, ft: FrameType, params: Dict):
    """
    同步收盘后的数据
    Args:
        typ: 证券类型 如 stock  index
        ft: k线类型
        params: master发过来的参数

    Returns:

    """
    logger.debug("_sync_to_cache(%s) 被调用", ft)
    name, n, queue, done_queue, limit = await sync_params_analysis(typ, ft, params)
    logger.info("%s", done_queue)
    async for secs in get_secs_for_sync(limit, n, queue):
        # todo 如果是日线，需要调用获取日线的方法，其他的不用变
        bars = await fetch_bars(secs, params.get("end"), n, ft)
        await Stock.batch_cache_bars(ft, bars)
        # 记录已完成的证券
        await cache.sys.lpush(done_queue, *secs)


async def sync_for_persist(typ: SecurityType, ft: FrameType, params: Dict):
    """校准同步实现，用以分钟线、日线、周线、月线、年线等
    1. 同步完成之后，对两次的bars数据计算做校验和验算，避免数据出错
    2. 校验通过之后将数据写入influxdb
    3. 缓存至redis的3号temp库中，以备master使用写入dfs中
    4. 将完成的证券代码写入完成队列，让master可以知道那些完成了

    Args:
        typ: 证券类型 如 stock  index
        ft: k线类型
        params: master发过来的参数

    """
    name, n, queue, done_queue, limit = await sync_params_analysis(typ, ft, params)
    logger.info("params: %s, %s, %s, %s, %s", name, n, queue, done_queue, limit)
    async for secs in get_secs_for_sync(limit, n, queue):
        # todo: is there better way to do the check? 校验和数据类型问题
        bars1 = await fetch_bars(secs, params.get("end"), n, ft)  # get_bars
        # if ft in (FrameType.MIN1, FrameType.DAY):
        #     bars2 = await fetch_price(secs, params.get("end"), n, ft)  # get_price
        #     if not checksum(bars1, bars2):
        #         raise exception.ChecksumFail()
        await Stock.persist_bars(ft, bars1)
        # except Exception as e:
        #     print(e)
        await cache_bars_for_aggregation(name, typ, ft, bars1)
        # 记录已完成的证券
        logger.info("done sync: %s, %s", ft, secs)
        await cache.sys.lpush(done_queue, *secs)


async def get_secs_for_sync(limit: int, n_bars: int, name: str):
    """计算并获取可在一批次中进行同步的股票代码

    master将本次参与同步的证券代码存入redis中的一个list，该list的名字为`name`。各个worker从该lister中分批获取证券代码，用以同步。

    Args:
        limit: 调用接口限制条数
        n_bars: 每支股票需要取多少条
        name: 队列名称

    Returns:

    """
    while True:
        step = limit // n_bars  # 根据 单次api允许获取的条数 和一共多少根k线 计算每次最多可以获取多少个股票的
        p = cache.sys.pipeline()
        p.lrange(name, 0, step - 1)
        p.ltrim(name, step, -1)
        secs, _ = await p.execute()
        if not len(secs):
            break
        yield secs


def checksum(bars1, bars2) -> bool:
    for code in bars1:
        bar1 = bars1[code]
        bar2 = bars2[code]
        if len(bar1) != len(bar2):
            logger.error("长度不相等，错误")
            break
        for item1, item2 in zip(bar1, bar2):
            # 判断字段是否相等
            for field in ["frame", "open", "high", "low", "close", "volume", "amount"]:
                if field == "frame":
                    if item1[field].strftime("%Y-%m-%d") != item2[field].strftime(
                        "%Y-%m-%d"
                    ):
                        logger.error(f"不相等 item1:{item1}, item2:{item2}, field:{field}")
                        return False
                else:
                    i1 = math_round(item1[field], 2)
                    i2 = math_round(item2[field], 2)
                    if i1 != i2:
                        logger.error(
                            f"不相等 item1:{item1}, {i1}, item2:{item2}, {i2}, field:{field}, code:{code}"
                        )
                        return False

    return True


async def cache_bars_for_aggregation(
    name: str,
    typ: SecurityType,
    ft: FrameType,
    bars: Union[Dict[str, np.ndarray], np.ndarray],
):
    """将bars临时存入redis，以备聚合之用

    行情数据除了需要保存到时序数据库外，还要以块存储的方式，保存到minio中。

    minio中存储的行情数据是按index/stock和frame_type进行分组的。在有多个worker存在的情况下，每个worker都只能拿到这些分组的部分品种数据。因此，我们需要将其写入redis中，由master来执行往minio的的写入工作。

    Args:
        typ: SecurityType of Stock or Index
        ft: FrameType of DAY or MIN1
        bars: dict of bars

    Returns:
        None
    """
    queue = f"{MINIO_TEMPORAL}.{name}.{typ.value}.{ft.value}"
    data = pickle.dumps(bars, protocol=cfg.pickle.ver)
    await cache.temp.lpush(queue, data)
