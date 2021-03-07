import arrow

from tests.interfaces.test_web_interfaces import TestWebInterfaces


class TestQuotes(TestWebInterfaces):
    async def test_get_security_list(self):
        secs = await self.server_get("quotes", "security_list")
        self.assertEqual("平安银行", secs[0][1])

    async def test_get_valuation(self):
        vals = await self.server_get(
            "quotes",
            "valuation",
            {
                "secs": "000001.XSHE",
                "fields": ["code", "frame"],
                "date": "2020-11-20",
                "n": 1,
            },
        )
        self.assertEqual(arrow.get("2020-11-20").date(), vals[0]["frame"])

    async def test_get_bars(self):
        bars = await self.server_get(
            "quotes",
            "bars",
            {
                "sec": "000001.XSHE",
                "end": "2020-11-20",
                "n_bars": 1,
                "frame_type": "1d",
                "include_unclosed": True,
            },
        )
        self.assertEqual(arrow.get("2020-11-20").date(), bars[0]["frame"])

    async def test_get_bars_batch(self):
        bars = await self.server_get(
            "quotes",
            "bars_batch",
            {
                "secs": ["000001.XSHE", "600001.XSHG"],
                "end": "2020-11-20",
                "n_bars": 1,
                "frame_type": "1d",
                "include_unclosed": True,
            },
        )
        self.assertEqual(
            arrow.get("2020-11-20").date(), bars["000001.XSHE"]["frame"][0]
        )
