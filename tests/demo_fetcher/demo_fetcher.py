# -*- coding: utf-8 -*-
import datetime
import logging
from typing import Dict, List, Optional, Union

import numpy as np
from coretypes import Frame, FrameType, bars_dtype, bars_dtype_with_code

from omega.worker.quotes_fetcher import QuotesFetcher

logger = logging.getLogger(__file__)


class DemoFetcher(QuotesFetcher):
    @classmethod
    async def create_instance(cls, account: str, password: str, **kwargs):
        print("create_instance")

    async def get_security_list(
        self, date: datetime.date = None
    ) -> Optional[np.ndarray]:
        """获取证券列表

        按如下格式返回证券列表。

        code         display_name   name   start_date   end_date   type
        000001.XSHE   平安银行       PAYH   1991-04-03   2200-01-01 stock

        Args:
            date: 查询日期，如果为None，则返回最新的证券列表。
        Returns:
            `date`日对应的证券列表。
        """
        dtype = [
            ("code", "O"),
            ("display_name", "O"),
            ("name", "O"),
            ("start_date", "O"),
            ("end_date", "O"),
            ("type", "O"),
        ]
        data = [
            (
                "000001.XSHE",
                "平安银行",
                "PAYH",
                datetime.date(1991, 4, 3),
                datetime.date(2200, 1, 1),
                "stock",
            ),
            (
                "000001.XSHG",
                "上证指数",
                "SZZS",
                datetime.date(1991, 7, 15),
                datetime.date(2200, 1, 1),
                "index",
            ),
        ]
        return np.array(data, dtype=dtype)

    async def get_finance_xrxd_info(
        self, dt1: datetime.date, dt2: datetime.date
    ) -> List:
        """按如下格式返回分红送股公告事件。

        code, a_xr_date, board_plan_bonusnote, bonus_ratio_rmb, dividend_ratio, transfer_ratio,
            at_bonus_ratio_rmb, report_date, company_name, plan_progress, implementation_bonusnote, bonus_cancel_pub_date

        Returns:
            List: [description]
        """
        reports = []
        record = (
            "000001.XSHE",
            datetime.date(2022, 7, 22),
            "10派2.28元(含税)",
            2.28,
            0.0,
            0.0,
            2.28,
            datetime.date(2021, 12, 31),
            "实施方案",
            "10派2.28元(含税)",
            datetime.date(2099, 1, 1),
        )
        reports.append(record)
        return reports

    async def get_bars_batch(
        self,
        secs: List[str],
        end: Frame,
        n_bars: int,
        frame_type: FrameType,
        include_unclosed=True,
    ) -> Dict[str, np.ndarray]:
        data1 = [
            (
                "2022-09-07T00:00:00.000Z",
                12.319999694824219,
                12.4399995803833,
                12.300000190734863,
                12.359999656677246,
                62111692,
                768913483,
                123.9096908569336,
            ),
            (
                "2022-09-08T00:00:00.000Z",
                12.319999694824219,
                12.4399995803833,
                12.300000190734863,
                12.359999656677246,
                62111692,
                768913483,
                123.9096908569336,
            ),
        ]
        data2 = [
            (
                "2022-09-07T00:00:00.000Z",
                3245.56005859375,
                3253.699951171875,
                3233.800048828125,
                3235.590087890625,
                28168035800,
                328657091600,
                1,
            ),
            (
                "2022-09-08T00:00:00.000Z",
                3245.56005859375,
                3253.699951171875,
                3233.800048828125,
                3235.590087890625,
                28168035800,
                328657091600,
                1,
            ),
        ]
        results = {"000001.XSHE": np.array(data1, dtype=bars_dtype)}
        results["000001.XSHG"] = np.array(data2, dtype=bars_dtype)
        return results

    async def get_price(
        self,
        secs: List[str],
        end: Frame,
        n_bars: int,
        frame_type: FrameType,
    ) -> Dict[str, np.recarray]:
        data = (
            "000001.XSHE",
            "2022-09-08T00:00:00.000Z",
            12.319999694824219,
            12.4399995803833,
            12.300000190734863,
            12.359999656677246,
            62111692,
            768913483,
            123.9096908569336,
        )
        return np.array(data, dtype=bars_dtype_with_code)

    async def get_all_trade_days(self):
        return np.array(
            [
                datetime.date(2022, 9, 5),
                datetime.date(2022, 9, 6),
                datetime.date(2022, 9, 7),
                datetime.date(2022, 9, 8),
                datetime.date(2022, 9, 9),
                datetime.date(2022, 9, 13),
                datetime.date(2022, 9, 14),
                datetime.date(2022, 9, 15),
                datetime.date(2022, 9, 16),
                datetime.date(2022, 9, 19),
            ],
            dtype=object,
        )

    async def get_trade_price_limits(
        self, sec: Union[List, str], dt: Union[str, Frame]
    ) -> np.ndarray:
        dtype = [
            ("frame", "O"),
            ("code", "O"),
            ("high_limit", "<f4"),
            ("low_limit", "<f4"),
        ]
        # real data in
        data = (
            datetime.date(2022, 9, 8),
            "000001.XSHE",
            13.5600004196167,
            11.100000381469727,
        )
        return np.array(data, dtype=dtype)

    def get_quota_spare(self):
        return 1000 * 10000

    def get_quota(cls):
        return 2000 * 10000

    def result_size_limit(self, op: str) -> int:
        return 3000
