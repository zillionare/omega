import datetime
from typing import List, Tuple

import cfg4py
import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client
from coretypes import Frame, FrameType, SecurityType


class Haystore(object):
    def __init__(self):
        cfg = cfg4py.get_instance()
        host = cfg.clickhouse.host
        user = cfg.clickhouse.user
        password = cfg.clickhouse.password
        database = cfg.clickhouse.database
        self.client = clickhouse_connect.get_client(
            host=host, username=user, password=password, database=database
        )

    def close(self):
        """关闭clickhouse连接"""
        self.client.close()

    def save_bars(self, frame_type: FrameType, bars: pd.DataFrame):
        """保存行情数据。

        Args:
            frame_type: 行情数据的周期。只接受1分钟和日线
            bars: 行情数据，必须包括symbol, frame, OHLC, volume, money字段
        """
        assert frame_type in [FrameType.DAY, FrameType.MIN1]
        if frame_type == FrameType.DAY:
            table = HaystoreTbl.bars_1d
        else:
            table = HaystoreTbl.bars_1m

        self.client.insert_df(table, bars)

    def save_ashare_list(
        self,
        data: pd.DataFrame,
    ):
        """保存证券（股票、指数）列表

        Args:
            data: contains date, code, alias, ipo day, and type
        """
        self.client.insert_df(HaystoreTbl.securities, data)

    def get_bars(
        self, code: str, n: int, frame_type: FrameType, end: datetime.datetime
    ):
        """从clickhouse中获取持久化存储的行情数据

        Args:
            code: 股票代码，以.SZ/.SH结尾
            frame_type: 行情周期。必须为1分钟或者日线
            n: 记录数
            end: 记录截止日期

        """
        sql = "SELECT * from {table: Identifier} where frame < {frame: DateTime} and symbol = {symbol:String}"
        params = {"table": f"bars_{frame_type.value}", "frame": end, "symbol": code}
        return self.client.query_np(sql, parameters=params)

    def query_df(self, sql: str, **params) -> pd.DataFrame:
        """执行任意查询命令"""
        return self.client.query_df(sql, parameters=params)

    def update_factors(self, sec: str, factors: pd.Series):
        """更新复权因子。

        TODO:
            参考https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse进行优化。

        Args:
            sec: 待更新复权因子的证券代码
            factors: 以日期为索引，复权因子为值的Series
        """
        
        for dt, factor in factors.items():
            sql = "alter table bars_day update factor = %(v1)s where symbol = %(v2)s and frame = %(v3)s"

            self.client.command(sql, {"v1": factor, "v2": sec, "v3": dt})
            
