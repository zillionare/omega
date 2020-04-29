import os
import unittest
from concurrent.futures.thread import ThreadPoolExecutor

import arrow
import cfg4py
from omicron.core import Events, FrameType
from omicron.dal import cache
from pyemit import emit

import logging

from omicron.core.lang import async_run

from omega.config.cfg4py_auto_gen import Config
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


class MyTestCase(unittest.TestCase):
    def get_config_path(self):
        src_dir = os.path.dirname(__file__)
        return os.path.join(src_dir, '../omega/config')

    async def start_quotes_fetchers(self):
        cfg: Config = cfg4py.get_instance()
        for fetcher in cfg.quotes_fetchers:
            await AbstractQuotesFetcher.create_instance(fetcher)

    @async_run
    async def setUp(self) -> None:
        os.environ[cfg4py.envar] = 'TEST'

        logger.info("starting solo quotes server...")
        cfg4py.init(self.get_config_path(), False)
        cfg: Config = cfg4py.get_instance()
        if len(cfg.quotes_fetchers) == 0:
            raise ValueError("please config quotes fetcher before test.")
        self.executors = ThreadPoolExecutor(max_workers=cfg.concurrency.threads)
        await cache.init()

        # 启动 emits 事件监听
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, exchange='omega', start_server=True)
        await self.start_quotes_fetchers()

    @async_run
    async def test_get_security_list(self):
        # todo: emit should allow null message
        async def on_security_list_updated(msg):
            logger.info('security list updated')

        emit.register(Events.SECURITY_LIST_UPDATED, on_security_list_updated)
        await AbstractQuotesFetcher.get_security_list()
        print('done')

    @async_run
    async def test_get_bars(self):
        sec = '000001.XSHE'
        end = arrow.get('2020-04-04')
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 10, FrameType.DAY)
        self.assertEqual(bars[0]['frame'], arrow.get('2020-03-23').date())
        self.assertEqual(bars[-1]['frame'], arrow.get('2020-04-03').date())
        print('done')


if __name__ == '__main__':
    unittest.main()
