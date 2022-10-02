import logging
import os

from coretypes import Frame, FrameType
from omicron import cache, tf
from omicron.notify.dingtalk import DingTalkMessage

logger = logging.getLogger(__name__)


async def load_lua_script():
    """加载lua脚本到redis中"""
    dir_ = os.path.dirname(os.path.abspath(__file__))
    for file in os.listdir(dir_):
        if not file.endswith(".lua"):
            continue

        path = os.path.join(dir_, file)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

            r = await cache.sys.execute_command("FUNCTION", "LOAD", "REPLACE", content)
            print(r)


async def update_unclosed_bar(frame_type: FrameType, source_min: Frame):
    """wraps the cognominal lua script function

    Args:
        frame_type: which frame type to be updated/merged
        source_min: the minute bar to be merged from
    """
    source = tf.time2int(source_min)
    try:
        await cache.security.execute_command(
            "fcall", "update_unclosed", 0, frame_type.value, source
        )
    except Exception as e:
        msg = f"实时合并{frame_type}未收盘行情数据错误：{source_min}"
        logger.exception(e)
        logging.warning(msg)
        DingTalkMessage.text(msg)


async def close_frame(frame_type: FrameType, frame: Frame):
    """wraps the cognominal lua script function

    Args:
        frame_type: which frame type to be closed
        frame: the closed frame
    """
    dst = (
        tf.date2int(frame) if frame_type in tf.day_level_frames else tf.time2int(frame)
    )

    try:
        await cache.security.execute_command(
            "fcall", "close_frame", 0, frame_type.value, dst
        )
    except Exception as e:
        msg = f"缓存收盘{frame_type}数据失败: {frame}"
        logger.exception(e)
        logger.warning(msg)
        DingTalkMessage.text(msg)
