import unittest
from omega.fetchers.akshare import fetch_stock_list
import datetime

class AkshareTest(unittest.TestCase):
    def test_fetch_stock_list(self):
        df = fetch_stock_list()
        self.assertTrue(len(df) >= 1000)
        self.assertSetEqual(set(df["type"]), set(["stock", "index"]))
        filter = df.code == "000003.SZ"
        self.assertEqual(df[filter].iloc[0]["exit"], datetime.date(2002, 6, 14))

        filter = df.code == "000001.SH"
        self.assertEqual(df[filter].iloc[0]["alias"], "上证指数")
