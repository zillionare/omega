# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-07 18:28
import asyncio
import logging
from omicron import cache

logger = logging.getLogger(__name__)


async def fetch_min_bars(start, end, secs):
    """从远端获取分钟线数据"""

    return []


async def sync_bars(params: dict):
    """"""
    print(params)
    while True:
        print("进入循环")
        p = cache.sys.pipeline()
        p.lrange("master.bars_sync.scope.minute", 0, 10 - 1)
        p.ltrim("master.bars_sync.scope.minute", 10, -1)
        # data = p.execute()
        secs, _ = await p.execute()
        if not len(secs):
            print("队列无数据，已完成")
            break
        print(secs)
        # TODO 拿到股票列表和时间，组装参数调用get_price
        await asyncio.sleep(1)
        await fetch_min_bars(1, 1, secs)
        await cache.sys.lpush("master.bars_sync.done.minute", *secs)
        print("已设置完成队列")