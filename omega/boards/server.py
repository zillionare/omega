import datetime
import logging

import numpy as np
from omicron.models.timeframe import TimeFrame

from omega.boards.board import ConceptBoard, IndustryBoard
from omega.boards.storage import (
    board_bars_dtype,
    get_latest_date_from_db,
    save_board_bars,
)

logger = logging.getLogger(__name__)


def boards_init() -> bool:
    ib = IndustryBoard()
    cb = ConceptBoard()
    ib.init()
    cb.init()

    try:
        info = ib.info()
        logger.info(f"行业板块已更新至: {info['last_sync_date']},共{len(info['history'])}天数据。")
    except KeyError:
        logger.info("行业板块数据还从未同步过。")

    try:
        info = cb.info()
        logger.info(f"概念板块已更新至: {info['last_sync_date']},共{len(info['history'])}天数据。")
    except KeyError:
        logger.info("概念板块数据还从未同步过。")

    return True


def sync_board_names(board_type: str):
    try:
        logger.info("start sync %s board names...", board_type)

        if board_type == "industry":
            IndustryBoard.fetch_board_list()
        else:
            ConceptBoard.fetch_board_list()
    except Exception as e:
        logger.exception(e)
        return False

    logger.info("%s board name sync finished...", board_type)
    return True


async def fetch_industry_day_bars(dt: datetime.date):
    dt_end = TimeFrame.day_shift(dt, 1)
    logger.info("start fetch industry board day bars, %s (%s)...", dt, dt_end)

    try:
        ib = IndustryBoard()
        boards = ib.boards
        total_boards = len(ib.boards)
        for i, code in enumerate(boards["code"]):
            _name = ib.get_name(code)
            # 获取db中的最后一条记录时间
            latest_dt = await get_latest_date_from_db(code)
            if latest_dt == dt:
                continue

            logger.info(
                "fetch day bars for industry/%s (%d/%d), (%s - %s]",
                _name,
                i + 1,
                total_boards,
                latest_dt,
                dt,
            )
            df = ib.get_industry_bars(_name, latest_dt, dt_end)
            if len(df) == 0:
                logger.info(
                    "no industry bars fetched from website for %s/%s, skip...",
                    code,
                    _name,
                )
                continue

            df = df.rename(
                columns={
                    "日期": "frame",
                    "开盘价": "open",
                    "最高价": "high",
                    "最低价": "low",
                    "收盘价": "close",
                    "成交量": "volume",
                    "成交额": "amount",
                }
            )
            df.insert(0, "code", f"{code}.THS")
            bars = (
                df[
                    [
                        "code",
                        "frame",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "amount",
                    ]
                ]
                .to_records(index=False)
                .astype(board_bars_dtype)
            )

            bars = bars[~np.isnan(bars["open"])]
            await save_board_bars(bars)
            logger.info(
                "save day bars to influxdb for industry/%s (%s), bars: %d",
                _name,
                code,
                len(bars),
            )

    except Exception as e:
        logger.exception(e)
        return False

    return True


async def fetch_concept_day_bars(dt: datetime.date):
    logger.info("start fetch concept board day bars, %s...", dt)

    try:
        cb = ConceptBoard()
        boards = cb.boards
        total_boards = len(cb.boards)
        for i, code in enumerate(boards["code"]):
            _name = cb.get_name(code)
            _added_dt = cb.get_created_time(code)
            if _added_dt and _added_dt == dt:
                logger.info("skip new added board: %s/%s", _name, code)
                continue

            # 获取db中的最后一条记录时间
            latest_dt = await get_latest_date_from_db(code)
            if latest_dt == dt:
                continue

            logger.info(
                "fetch day bars for concept/%s (%d/%d), %s - %s",
                _name,
                i + 1,
                total_boards,
                latest_dt,
                dt,
            )
            df = cb.get_concept_bars(_name, dt)
            if len(df) == 0:
                logger.info(
                    "no concept bars fetched from website for %s/%s, skip...",
                    code,
                    _name,
                )
                continue
            new_df = df[df["日期"] > latest_dt]
            if len(new_df) == 0:
                continue

            new_df = new_df.rename(
                columns={
                    "日期": "frame",
                    "开盘价": "open",
                    "最高价": "high",
                    "最低价": "low",
                    "收盘价": "close",
                    "成交量": "volume",
                    "成交额": "amount",
                }
            )
            new_df.insert(0, "code", f"{code}.THS")
            bars = (
                new_df[
                    [
                        "code",
                        "frame",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "amount",
                    ]
                ]
                .to_records(index=False)
                .astype(board_bars_dtype)
            )

            bars = bars[~np.isnan(bars["open"])]
            await save_board_bars(bars)
            logger.info(
                "save day bars to influxdb for concept/%s (%s), bars: %d",
                _name,
                code,
                len(bars),
            )

    except Exception as e:
        logger.exception(e)
        return False

    return True


async def fetch_board_members(board_type: str):
    try:
        logger.info("start sync %s board members...", board_type)

        if board_type == "industry":
            IndustryBoard.fetch_board_members()
        else:
            ConceptBoard.fetch_board_members()
    except Exception as e:
        logger.exception(e)

    logger.info("sync %s board members finished...", board_type)
