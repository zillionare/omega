import datetime
import logging
from functools import cache
from itertools import product
from typing import List

import cfg4py
import pandas as pd
import tushare as ts
from coretypes import DayBarsSchema, FrameType, SecurityInfoSchema
from omicron import tf
from omicron.extensions import chunkify
from pandera.typing import DataFrame
from tushare.pro.client import DataApi

logger = logging.getLogger(__name__)

@cache
def pro_api() -> DataApi:
    """获取登录后的pro_api接口"""
    cfg = cfg4py.get_instance()
    ts.set_token(cfg.tushare.token)
    return ts.pro_api()


def fetch_stock_bars_daily(codes: List[str], start: datetime.date, end: datetime.date):
    """获取日线行情数据

    本API将获取日线行情(frame, OHLC, vol, money, factor, turnover)。
    本API获得的复权因子，在计算上遵循[复权方法](https://tushare.pro/document/2?doc_id=146)：当日收盘价 × 当日复权因子 / 最新复权因子
    """
    batch = 6000

    fields = ["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount"]
    extend_fields = [
        "ts_code",
        "trade_date",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    ]

    ohlc = []
    factors = []
    limits = []
    suspend = []
    extended = []
    # 获取OHLC数据和复权数据
    for frame in tf.get_frames(start, end, FrameType.DAY):
        sframe = str(frame)
        logger.info(f"fetching data for frame %s", frame)
        for sub in chunkify(codes, batch):
            ts_codes = ",".join(sub)
            df = pro_api().daily(
                ts_code=ts_codes, start_date=sframe, end_date=sframe, fields=fields
            )
            ohlc.append(df)

        # 复权数据，据tushare文档，此调用方法可以获得单日全部股票复权因子，未提及限制
        df = pro_api().adj_factor(trade_date=sframe)
        factors.append(df)

        # 获取涨跌停限价
        df = pro_api().stk_limit(trade_date=sframe)
        limits.append(df)

        # 获取停牌
        df = pro_api().suspend_d(
            suspend_type="S",
            trade_date=sframe,
            fields=["ts_code", "trade_date", "suspend_type"],
        )
        suspend.append(df)

        # 换手率及其它
        df = pro_api().daily_basic(trade_date=sframe, fields=extend_fields)
        extended.append(df)

    ohlc = pd.concat(ohlc)
    factors = pd.concat(factors)
    limits = pd.concat(limits)
    suspend = pd.concat(suspend)
    suspend["suspend_type"] = suspend.suspend_type.apply(lambda x: x == "S")
    extended = pd.concat(extended)

    left = ohlc
    for right in (factors, limits, suspend, extended):
        left = pd.merge(left, right, on=["ts_code", "trade_date"], how="left")

    df = left.rename({
        "ts_code": "code",
        "trade_date": "frame",
        "vol": "volume",
        "adj_factor": "factor",
        "up_limit": "hlimit",
        "down_limit": "llimit",
        "suspend_type": "suspended",
        "turnover_rate": "turnover"
    })

    df["suspended"] = df.suspended.astype(int)
