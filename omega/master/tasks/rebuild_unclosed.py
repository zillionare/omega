import datetime
import logging

import numpy as np
from coretypes import FrameType
from omicron import cache, tf
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.notify.dingtalk import DingTalkMessage

logger = logging.getLogger(__name__)


async def _rebuild_min_level_unclosed_bars():
    """根据缓存中的分钟线，重建当日已收盘或者未收盘的分钟级别及日线级别数据"""
    now = datetime.datetime.now()
    end = tf.floor(now, FrameType.MIN1)
    keys = await cache.security.keys("bars:1m:*")

    errors = 0
    for key in keys:
        try:
            sec = key.split(":")[2]
            bars = await Stock._get_cached_bars_n(sec, 240, FrameType.MIN1, end)
        except Exception as e:
            logger.exception(e)
            logger.warning("failed to get cached bars for %s", sec)
            errors += 1
            continue

        try:
            for frame_type in tf.minute_level_frames[1:]:
                resampled = Stock.resample(bars, FrameType.MIN1, frame_type)
                if tf.is_bar_closed(resampled[-1]["frame"].item(), frame_type):
                    await Stock.cache_bars(sec, frame_type, resampled)
                else:
                    await Stock.cache_bars(sec, frame_type, resampled[:-1])
                    await Stock.cache_unclosed_bars(sec, frame_type, resampled[-1:])

            # 重建日线数据
            resampled = Stock.resample(bars, FrameType.MIN1, FrameType.DAY)
            await Stock.cache_unclosed_bars(sec, FrameType.DAY, resampled)
        except Exception as e:
            logger.exception(e)
            logger.warning(
                "failed to build unclosed bar for %s, frame type is %s", sec, frame_type
            )
            errors += 1

    if errors > 0:
        DingTalkMessage.text(f"重建分钟级缓存数据时，出现{errors}个错误。")


async def _rebuild_day_level_unclosed_bars():
    """重建当期未收盘的周线、月线

    !!!Info:
        最终我们需要实时更新年线和季线。目前数据库还没有同步这两种k线。
    """
    codes = await Security.select().types(["stock", "index"]).eval()
    end = datetime.datetime.now().date()
    # just to cover one month's day bars at most
    n = 30

    errors = 0
    for code in codes:
        try:
            bars = await Stock._get_persisted_bars_n(code, FrameType.DAY, n, end=end)
        except Exception as e:
            logger.exception(e)
            logger.warning(
                "failed to get persisted bars for %s from %s[%d]", code, end, n
            )
            errors += 1
            continue

        try:
            unclosed_day = await Stock._get_cached_bars_n(
                code, n, FrameType.DAY, end=end
            )
            bars = np.concatenate([bars, unclosed_day])

            week = Stock.resample(bars, FrameType.DAY, FrameType.WEEK)
            await Stock.cache_unclosed_bars(code, FrameType.WEEK, week[-1:])

            month = Stock.resample(bars, FrameType.DAY, FrameType.MONTH)
            await Stock.cache_unclosed_bars(code, FrameType.MONTH, month[-1:])
        except Exception as e:
            logger.exception(e)
            logger.warning(
                "failed to build unclosed bar for %s, got bars %s", code, len(bars)
            )
            errors += 1

    if errors > 0:
        DingTalkMessage.text(f"重建日线级别缓存数据时，出现{errors}个错误。")


async def rebuild_unclosed_bars():
    """在omega启动时重建未收盘数据

    后续未收盘数据的更新，将在每个分钟线同步完成后，调用lua脚本进行。
    """
    await _rebuild_min_level_unclosed_bars()
    await _rebuild_day_level_unclosed_bars()
