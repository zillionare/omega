import unittest
from unittest import mock

import cfg4py
import omicron

from omega.master.tasks.quota_utils import QuotaMgmt
from tests import init_test_env


class QuotaUtilsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        # await omicron.init()

    async def asyncTearDown(self) -> None:
        # await omicron.close()
        pass

    async def test_update_state(self):
        QuotaMgmt.quota_stat_q1 = 0
        QuotaMgmt.quota_stat_q2 = 0
        QuotaMgmt.quota_date = None
        param = {"account": "test", "quota": 1000, "total": 2000}
        QuotaMgmt.update_state(param)
        p = QuotaMgmt.work_state["test"]
        self.assertEqual(p, param)

        q, t = QuotaMgmt.get_quota()
        self.assertEqual(q, 1000)
        self.assertEqual(t, 2000)

    async def test_update_quota(self):
        QuotaMgmt.quota_stat_q1 = 0
        QuotaMgmt.quota_stat_q2 = 0
        QuotaMgmt.quota_date = None
        param = {"account": "test", "quota": 1500, "total": 2000}
        QuotaMgmt.update_state(param)

        QuotaMgmt.update_quota()
        self.assertEqual(QuotaMgmt.quota_stat_q1, 1000)
        self.assertEqual(QuotaMgmt.quota_stat_q2, 500)

        QuotaMgmt.update_quota()
        self.assertEqual(QuotaMgmt.quota_stat_q1, 1000)
        self.assertEqual(QuotaMgmt.quota_stat_q2, 500)

        # quota不够用
        QuotaMgmt.quota_stat_q1 += 100  # 增加100
        QuotaMgmt.update_quota()
        self.assertEqual(QuotaMgmt.quota_stat_q1, 1000)
        self.assertEqual(QuotaMgmt.quota_stat_q2, 500)

        QuotaMgmt.quota_stat_q1 = 500
        QuotaMgmt.quota_stat_q2 = 2000
        QuotaMgmt.update_quota()
        self.assertEqual(QuotaMgmt.quota_stat_q1, 0)
        self.assertEqual(QuotaMgmt.quota_stat_q2, 1500)

    async def test_update_quota2(self):
        QuotaMgmt.quota_stat_q1 = 0
        QuotaMgmt.quota_stat_q2 = 0
        QuotaMgmt.quota_date = None

        # reserved = 500, 剩余quota不够
        param = {"account": "test", "quota": 200, "total": 2000}
        QuotaMgmt.update_state(param)

        QuotaMgmt.update_quota()
        self.assertEqual(QuotaMgmt.quota_stat_q1, 0)
        self.assertEqual(QuotaMgmt.quota_stat_q2, 200)

    async def test_check_quota(self):
        QuotaMgmt.quota_stat_q1 = 0
        QuotaMgmt.quota_stat_q2 = 0
        QuotaMgmt.quota_date = None

        param = {"account": "test", "quota": 1500, "total": 2000}
        QuotaMgmt.update_state(param)
        # q1 = 1000, q2 = 500

        r1, r2, r3 = QuotaMgmt.check_quota(1, 100)
        self.assertEqual(r1, True)
        self.assertEqual(r2, 900)
        self.assertEqual(r3, 100)

        r1, r2, r3 = QuotaMgmt.check_quota(1, 1000)
        self.assertEqual(r1, False)
        self.assertEqual(r2, 900)
        self.assertEqual(r3, 1000)

        r1, r2, r3 = QuotaMgmt.check_quota(2, 100)
        self.assertEqual(r1, True)
        self.assertEqual(r2, 400)
        self.assertEqual(r3, 100)

        r1, r2, r3 = QuotaMgmt.check_quota(2, 500)
        self.assertEqual(r1, False)
        self.assertEqual(r2, 400)
        self.assertEqual(r3, 500)
