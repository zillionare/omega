import datetime
import logging
import os
import unittest
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import arrow
import cfg4py
import numpy as np
import omicron
from omicron import cache
from omicron.models.calendar import Calendar as cal
from omicron.core.types import FrameType

from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher as aq
from tests import init_test_env

logger = logging.getLogger(__name__)

cfg = cfg4py.get_instance()


class TestAbstractQuotesFetcher(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()

        await self.create_quotes_fetcher()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    def get_config_path(self):
        src_dir = os.path.dirname(__file__)
        return os.path.join(src_dir, "../omega/config")

    async def create_quotes_fetcher(self):
        fetcher_info = cfg.quotes_fetchers[0]
        impl = fetcher_info["impl"]
        params = fetcher_info["workers"][0]
        await aq.create_instance(impl, **params)

    async def clear_cache(self, sec: str, frame_type: FrameType):
        await cache.security.delete(f"{sec}:{frame_type.value}")

    async def test_get_security_list(self):
        secs = await aq.get_security_list()
        self.assertEqual("000001.XSHE", secs[0][0])

    async def test_get_bars_batch(self):
        secs = ["000001.XSHE", "000001.XSHG"]
        end_dt = arrow.get("2020-11-01").date()
        frame_type = FrameType.DAY

        bars = await aq.get_bars_batch(secs, end_dt, 5, frame_type)
        self.assertSetEqual(set(secs), set(bars.keys()))
        self.assertEqual(5, len(bars["000001.XSHE"]))
        self.assertAlmostEqual(18.2, bars["000001.XSHE"]["open"][0], places=2)

    async def test_get_all_trade_days(self):
        days = await aq.get_all_trade_days()
        self.assertIn(datetime.date(2020, 12, 31), days)

    async def test_foo(self):
        start = arrow.get("2020-11-02 15:00:00", tzinfo=cfg.tz).datetime
        stop = arrow.get("2020-11-06 14:30:00", tzinfo=cfg.tz).datetime
        frame_type = FrameType.MIN30
        code = "000001.XSHE"

        n = cal.count_frames(start, stop, frame_type)
        bars = await aq.get_bars(code, stop, n, frame_type)
        print(bars)

    async def test_get_fund_list(self):
        dtype = [
            ("code", "O"),
            ("name", "O"),
            ("advisor", "O"),
            ("trustee", "O"),
            ("operate_mode_id", "<i8"),
            ("operate_mode", "O"),
            ("start_date", "O"),
            ("end_date", "O"),
            ("underlying_asset_type_id", "<i8"),
            ("underlying_asset_type", "O"),
        ]
        fetcher = aq.get_instance()
        funds = np.array(
            [
                (
                    "512690",
                    "酒ETF",
                    "鹏华基金管理有限公司",
                    "中国建设银行股份有限公司",
                    401005,
                    "ETF",
                    datetime.date(2019, 4, 4),
                    datetime.date(2099, 1, 1),
                    402001,
                    "股票型",
                )
            ],
            dtype=dtype,
        )

        def side_effect(code):
            if not code or "512690" in code:
                return funds
            return np.array([], dtype=dtype)

        with mock.patch.object(fetcher, "get_fund_list", side_effect=side_effect):
            code = ["512690"]
            vals = await aq.get_fund_list(code=code)
            self.assertEqual(len(code), len(vals))

        with mock.patch.object(fetcher, "get_fund_list", side_effect=side_effect):
            vals = await aq.get_fund_list()
            self.assertTrue(len(vals))

        with mock.patch.object(fetcher, "get_fund_list", side_effect=side_effect):
            vals = await aq.get_fund_list(code=code, fields="code")
            self.assertEqual(len(code), len(vals))
            self.assertSequenceEqual(vals.dtype.names, ["code"])

        with mock.patch.object(fetcher, "get_fund_list", side_effect=side_effect):
            code = ["233333"]
            vals = await aq.get_fund_list(code=code)
            self.assertFalse(len(vals))

    async def test_get_fund_net_value(self):
        code = ["501206"]
        fund_net_values = np.array(
            [("501206", 1.0, 1.0, 1.0, 1.0, 1.0, datetime.date(2021, 1, 9))],
            dtype=[
                ("code", "O"),
                ("net_value", "<f8"),
                ("sum_value", "<f8"),
                ("factor", "<f8"),
                ("acc_factor", "<f8"),
                ("refactor_net_value", "<f8"),
                ("day", "O"),
            ],
        )
        fetcher = aq.get_instance()
        with mock.patch.object(
            fetcher, "get_fund_net_value", return_value=fund_net_values
        ):
            vals = await aq.get_fund_net_value(code=code)
            self.assertEqual(len(code), len(vals))

            vals = await aq.get_fund_net_value(code=code, fields="code")
            self.assertEqual(len(code), len(vals))
            self.assertSequenceEqual(vals.dtype.names, ["code"])

            vals = await aq.get_fund_net_value(
                code=code, day=arrow.get("2021-01-09").date()
            )
            self.assertTrue(len(vals))

    async def test_get_fund_share_daily(self):
        fetcher = aq.get_instance()
        code = ["512690"]
        day = arrow.get("2021-10-26").date()
        fund_share_dailies = np.array(
            [
                (
                    "512690",
                    7.86449196e09,
                    datetime.date(2021, 10, 26),
                    "鹏华中证酒交易型开放式指数证券投资基金",
                )
            ],
            dtype=[("code", "O"), ("total_tna", "<f8"), ("date", "O"), ("name", "O")],
        )

        def side_effect(code=None, day=None):
            return fund_share_dailies

        with mock.patch.object(
            fetcher, "get_fund_share_daily", side_effect=side_effect
        ):
            vals = await aq.get_fund_share_daily(code, day=day)
            self.assertTrue(len(vals))

        with mock.patch.object(
            fetcher, "get_fund_share_daily", side_effect=side_effect
        ):
            vals = await aq.get_fund_share_daily(code, day=day, fields="code")
            self.assertTrue(len(vals))
            self.assertSequenceEqual(vals.dtype.names, ["code"])

    async def test_get_fund_portfolio_stock(self):
        def side_effect(code=None, pub_date=None):
            dtype = [
                ("code", "O"),
                ("period_start", "O"),
                ("period_end", "O"),
                ("pub_date", "O"),
                ("report_type_id", "<i8"),
                ("report_type", "O"),
                ("rank", "<i8"),
                ("symbol", "O"),
                ("name", "O"),
                ("shares", "<f8"),
                ("market_cap", "<f8"),
                ("proportion", "<f8"),
                ("deadline", "O"),
            ]
            stocks = np.array(
                [
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        2,
                        "600690",
                        "海尔智家",
                        45141315.0,
                        1.31857781e09,
                        6.4,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        3,
                        "600031",
                        "三一重工",
                        32392600.0,
                        1.13160735e09,
                        5.49,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        1,
                        "601318",
                        "中国平安",
                        15876949.0,
                        1.38097702e09,
                        6.7,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        8,
                        "000895",
                        "双汇发展",
                        13913806.0,
                        6.43992558e08,
                        3.13,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        7,
                        "300413",
                        "芒果超媒",
                        9394546.0,
                        6.81104585e08,
                        3.31,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        4,
                        "000333",
                        "美的集团",
                        8171559.0,
                        8.04408268e08,
                        3.91,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        5,
                        "600309",
                        "万华化学",
                        7948547.0,
                        7.23635719e08,
                        3.51,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        9,
                        "688099",
                        "晶晨股份",
                        7575165.0,
                        5.88796540e08,
                        2.86,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        10,
                        "601012",
                        "隆基股份",
                        6707249.0,
                        5.83584958e08,
                        2.83,
                        datetime.date(2020, 12, 31),
                    ),
                    (
                        "150016",
                        datetime.date(2020, 10, 1),
                        datetime.date(2020, 12, 31),
                        datetime.date(2021, 1, 22),
                        403004,
                        "第四季度",
                        6,
                        "002594",
                        "比亚迪",
                        3581370.0,
                        6.95860191e08,
                        3.38,
                        datetime.date(2020, 12, 31),
                    ),
                ],
                dtype=dtype,
            )
            if pub_date == "2021-01-22" or code == ["150016"]:
                return stocks
            return np.array([], dtype=dtype)

        fetcher = aq.get_instance()
        with mock.patch.object(
            fetcher, "get_fund_portfolio_stock", side_effect=side_effect
        ):
            vals = await aq.get_fund_portfolio_stock(pub_date="2021-12-21")
            self.assertEqual(len(vals), 0)

            vals = await aq.get_fund_portfolio_stock(
                code=["150016"], pub_date="2021-01-22"
            )
            self.assertTrue(len(vals))

            vals = await aq.get_fund_portfolio_stock(code=["150016"])
            self.assertTrue(len(vals))

            vals = await aq.get_fund_portfolio_stock(code=["150016"], fields="code")
            self.assertTrue(len(vals))
            self.assertSequenceEqual(vals.dtype.names, ["code"])
