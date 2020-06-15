import logging
import os
import unittest

import arrow
import cfg4py
from omicron.core.events import Events
from omicron.core.lang import async_run
from omicron.core.types import FrameType
from omicron.dal import cache
from pyemit import emit

from omega.config.cfg4py_auto_gen import Config
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


class MyTestCase(unittest.TestCase):
    def get_config_path(self):
        src_dir = os.path.dirname(__file__)
        return os.path.join(src_dir, '../omega/config')

    async def start_quotes_fetchers(self):
        cfg: Config = cfg4py.get_instance()
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info['impl']
        params = fetcher_info['workers'][0]
        if 'port' in params: del params['port']
        if 'sessions' in params: del params['sessions']
        await AbstractQuotesFetcher.create_instance(impl, **params)

    @async_run
    async def setUp(self) -> None:
        os.environ[cfg4py.envar] = 'TEST'

        logger.info("starting solo quotes server...")
        cfg4py.init(self.get_config_path(), False)
        cfg: Config = cfg4py.get_instance()
        if len(cfg.quotes_fetchers) == 0:
            raise ValueError("please config quotes fetcher before test.")
        await cache.init()

        # 启动 emits 事件监听
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, exchange='omega',
                         start_server=True)
        await self.start_quotes_fetchers()

    @async_run
    async def test_get_security_list(self):
        async def on_security_list_updated(msg):
            logger.info('security list updated')

        emit.register(Events.SECURITY_LIST_UPDATED, on_security_list_updated)
        secs = await AbstractQuotesFetcher.get_security_list()
        self.assertEqual('000001.XSHE', secs[0][0])

    @async_run
    async def test_get_bars(self):
        sec = '000001.XSHE'
        end = arrow.get('2020-04-04')
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 10, FrameType.DAY)
        self.assertEqual(bars[0]['frame'], arrow.get('2020-03-23').date())
        self.assertEqual(bars[-1]['frame'], arrow.get('2020-04-03').date())


if __name__ == '__main__':
    unittest.main()
