import logging
import os
import unittest

import arrow
import cfg4py
import numpy
import numpy as np
import omicron
from omicron.core.events import Events
from omicron.core.lang import async_run
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from omicron.dal import cache, security_cache
from pyemit import emit

from omega.config.cfg4py_auto_gen import Config
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)

cfg = cfg4py.get_instance()
class MyTestCase(unittest.TestCase):
    def get_config_path(self):
        src_dir = os.path.dirname(__file__)
        return os.path.join(src_dir, '../omega/config')

    async def start_quotes_fetchers(self):
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info['impl']
        params = fetcher_info['workers'][0]
        if 'port' in params: del params['port']
        if 'sessions' in params: del params['sessions']
        await AbstractQuotesFetcher.create_instance(impl, **params)

    async def clear_cache(self, sec: str, frame_type: FrameType):
        await cache.security.delete(f"{sec}:{frame_type.value}")

    @async_run
    async def setUp(self) -> None:
        os.environ[cfg4py.envar] = 'TEST'

        logger.info("starting solo quotes server...")
        cfg4py.init(self.get_config_path(), False)
        cfg: Config = cfg4py.get_instance()
        if len(cfg.quotes_fetchers) == 0:
            raise ValueError("please config quotes fetcher before test.")

        # 启动 emits 事件监听
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn, exchange='omega',
                         start_server=True)
        await self.start_quotes_fetchers()
        await omicron.init(AbstractQuotesFetcher)

    @async_run
    async def test_get_security_list(self):
        async def on_security_list_updated(msg):
            logger.info('security list updated')

        emit.register(Events.SECURITY_LIST_UPDATED, on_security_list_updated)
        secs = await AbstractQuotesFetcher.get_security_list()
        self.assertEqual('000001.XSHE', secs[0][0])

    @async_run
    async def test_get_bars_010(self):
        """日线级别, 无停牌"""
        sec = '000001.XSHE'
        frame_type = FrameType.DAY
        # 2020-4-3 Friday
        end = arrow.get('2020-04-03').date()

        # without cache
        await self.clear_cache(sec, frame_type)
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 10, frame_type)
        print(bars)

        self.assertEqual(bars[0]['frame'], arrow.get('2020-03-23').date())
        self.assertEqual(bars[-1]['frame'], arrow.get('2020-04-03').date())
        self.assertAlmostEqual(12.0, bars[0]['open'], places=2)
        self.assertAlmostEqual(12.82, bars[-1]['open'], places=2)

        # 检查cache
        cache_len = await cache.security.hlen(f"{sec}:{frame_type.value}")
        self.assertEqual(12, cache_len)

        # 日线级别，停牌期间，数据应该置为np.nan
        sec = '000029.XSHE'
        end = arrow.get('2020-8-18').date()
        frame_type = FrameType.DAY
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 10, frame_type)
        self.assertEqual(10, len(bars))
        self.assertEqual(end, bars[-1]['frame'])
        self.assertEqual(arrow.get('2020-08-05').date(), bars[0]['frame'])
        self.assertTrue(np.all(np.isnan(bars['close'])))

    @async_run
    async def test_get_bars_011(self):
        """分钟级别，中间有停牌，end指定时间未对齐的情况"""
        # 600721, ST百花， 2020-4-29停牌一天
        sec = '600721.XSHG'
        frame_type = FrameType.MIN60
        end = arrow.get('2020-04-30 10:32', tzinfo='Asia/Shanghai').datetime

        await self.clear_cache(sec, frame_type)
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 7, frame_type)
        print(bars)

        self.assertEqual(7, len(bars))
        self.assertEqual(arrow.get('2020-04-28 15:00', tzinfo='Asia/Shanghai'),
                         bars['frame'][0])
        self.assertEqual(arrow.get('2020-04-30 10:30', tzinfo='Asia/Shanghai'),
                         bars['frame'][-2])
        self.assertEqual(arrow.get('2020-4-30 10:32', tzinfo='Asia/Shanghai'),
                         bars['frame'][-1])

        self.assertAlmostEqual(5.37, bars['open'][0], places=2)
        self.assertAlmostEqual(5.26, bars['open'][-2], places=2)
        self.assertAlmostEqual(5.33, bars['open'][-1], places=2)

        # 检查cache,10：32未存入cache
        cache_len = await cache.security.hlen(f"{sec}:{frame_type.value}")
        self.assertEqual(8, cache_len)
        bars_2 = await security_cache.get_bars(sec, tf.floor(end, frame_type), 6,
                                               frame_type)
        numpy.array_equal(bars[:-1], bars_2)

    @async_run
    async def test_get_bars_012(self):
        """分钟级别，中间有一天停牌，end指定时间正在交易"""
        # 600721, ST百花， 2020-4-29停牌一天
        sec = '600721.XSHG'
        frame_type = FrameType.MIN60
        end = arrow.get('2020-04-30 10:30', tzinfo=cfg.tz).datetime

        bars = await AbstractQuotesFetcher.get_bars(sec, end, 6, frame_type)
        print(bars)
        self.assertEqual(6, len(bars))
        self.assertEqual(arrow.get('2020-04-28 15:00', tzinfo='Asia/Shanghai'),
                         bars['frame'][0])
        self.assertEqual(arrow.get('2020-04-30 10:30', tzinfo='Asia/Shanghai'),
                         bars['frame'][-1])
        self.assertAlmostEqual(5.37, bars['open'][0], places=2)
        self.assertAlmostEqual(5.26, bars['open'][-1], places=2)
        self.assertTrue(numpy.isnan(bars['open'][1]))

        # 结束时间帧未结束
        end = arrow.get("2020-04-30 10:32:00.13", tzinfo=cfg.tz).datetime
        frame_type = FrameType.MIN30
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 6, frame_type)
        print(bars)
        self.assertAlmostEqual(5.33, bars[-1]['close'], places=2)
        self.assertEqual(end.replace(second=0, microsecond=0), bars[-1]['frame'])

    @async_run
    async def test_get_bars_013(self):
        """分钟级别，end指定时间正处在停牌中"""
        # 600721, ST百花， 2020-4-29停牌一天
        sec = '600721.XSHG'
        frame_type = FrameType.MIN60
        end = arrow.get('2020-04-29 10:30', tzinfo='Asia/Chongqing').datetime

        await self.clear_cache(sec, frame_type)

        bars = await AbstractQuotesFetcher.get_bars(sec, end, 6, frame_type)
        print(bars)
        self.assertEqual(6, len(bars))
        self.assertEqual(arrow.get('2020-04-27 15:00', tzinfo='Asia/Shanghai'),
                         bars['frame'][0])
        self.assertEqual(arrow.get('2020-04-29 10:30', tzinfo='Asia/Shanghai'),
                         bars['frame'][-1])
        self.assertAlmostEqual(5.47, bars['open'][0], places=2)
        self.assertAlmostEqual(5.37, bars['open'][-2], places=2)
        self.assertTrue(numpy.isnan(bars['open'][-1]))

        # 检查cache,10：30 已存入cache
        cache_len = await cache.security.hlen(f"{sec}:{frame_type.value}")
        self.assertEqual(8, cache_len)
        bars_2 = await security_cache.get_bars(sec, tf.floor(end, frame_type), 6,
                                               frame_type)
        numpy.array_equal(bars, bars_2)

    @async_run
    async def test_get_bars_014(self):
        """测试周线级别未结束的frame能否对齐"""
        sec = '600721.XSHG'
        frame_type = FrameType.WEEK

        """
        [(datetime.date(2020, 4, 17), 6.02, 6.69, 5.84, 6.58, 22407281., 1.40739506e+08, 1.455)
         (datetime.date(2020, 4, 24), 6.51, 6.57, 5.68, 5.72, 25718911., 1.54392455e+08, 1.455)
         (datetime.date(2020, 4, 29),  nan,  nan,  nan,  nan,       nan,            nan,   nan)]

        [(datetime.date(2020, 4, 17), 6.02, 6.69, 5.84, 6.58, 22407281., 1.40739506e+08, 1.455)
         (datetime.date(2020, 4, 24), 6.51, 6.57, 5.68, 5.72, 25718911., 1.54392455e+08, 1.455)
         (datetime.date(2020, 4, 30),  nan,  nan,  nan,  nan,       nan,            nan,   nan)]

        """
        end = arrow.get('2020-4-29 15:00').datetime  # 周三，当周周四结束
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 3, FrameType.WEEK)
        print(bars)
        self.assertEqual(arrow.get('2020-4-17').date(), bars[0]['frame'])
        self.assertEqual(arrow.get('2020-4-24').date(), bars[1]['frame'])
        self.assertEqual(arrow.get('2020-4-29').date(), bars[-1]['frame'])

        self.assertAlmostEqual(6.02, bars[0]['open'], places=2)
        self.assertAlmostEqual(6.51, bars[1]['open'], places=2)
        self.assertTrue(numpy.isnan(bars[-1]['open']))

        end = arrow.get('2020-04-30 15:00').datetime
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 3, FrameType.WEEK)
        print(bars)

        self.assertEqual(arrow.get('2020-4-17').date(), bars[0]['frame'])
        self.assertEqual(arrow.get('2020-4-24').date(), bars[1]['frame'])
        self.assertEqual(arrow.get('2020-4-30').date(), bars[-1]['frame'])

        self.assertAlmostEqual(6.02, bars[0]['open'], places=2)
        self.assertAlmostEqual(6.51, bars[1]['open'], places=2)
        self.assertAlmostEqual(5.7, bars[-1]['open'], places=2)

    @async_run
    async def test_get_bars_015(self):
        sec = '300677.XSHE'
        frame_type = FrameType.DAY
        end = arrow.now().datetime

        # without cache
        # await self.clear_cache(sec, frame_type)
        bars = await AbstractQuotesFetcher.get_bars(sec, end, 10, frame_type)
        print(bars)


if __name__ == '__main__':
    unittest.main()
