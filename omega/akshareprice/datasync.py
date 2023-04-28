import datetime
import logging

import aioredis
import cfg4py

logger = logging.getLogger(__name__)


db = None
key_price = "security:latest_price"
key_stock = "security:latest_price_info"
key_idx = "security:latest_index_info"


def init_redis_connection():
    global db
    cfg = cfg4py.get_instance()

    # using db=3
    # dsn = f"{cfg.redis.dsn}/3"
    db = aioredis.from_url(cfg.redis.dsn, db=3, encoding="utf-8", decode_responses=True)


async def reset_cache():
    await db.delete(key_price)
    await db.delete(key_stock)
    await db.delete(key_idx)
    logger.info("cache reset: %s, %s", key_price, key_stock)


async def data_writer(secs_data, client_src: str):
    now = datetime.datetime.now()

    data = secs_data.to_numpy()
    await db.hset(key_price, mapping={k: v for k, v in zip(data[:, 0], data[:, 1])})
    logger.info("latest price saved into cache: %s, %s, %d", client_src, now, len(data))

    # 新的key保存所有数据
    await db.hset(
        key_stock,
        mapping={
            k: f"{v1},{v2},{v3},{v4},{v5}"
            for k, v1, v2, v3, v4, v5 in zip(
                data[:, 0], data[:, 1], data[:, 2], data[:, 3], data[:, 4], data[:, 5]
            )
        },
    )
    logger.info("stock price saved into cache: %s, %s", client_src, now)


async def idx_data_writer(idx_data, client_src: str):
    now = datetime.datetime.now()

    data = idx_data.to_numpy()
    await db.hset(
        key_idx,
        mapping={
            k: f"{v1},{v2},{v3},{v4},{v5}"
            for k, v1, v2, v3, v4, v5 in zip(
                data[:, 0], data[:, 1], data[:, 2], data[:, 3], data[:, 4], data[:, 5]
            )
        },
    )
    logger.info("index data saved into cache: %s, %s", client_src, now)
