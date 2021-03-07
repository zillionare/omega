import logging

from tests.interfaces import TestWebInterfaces

logger = logging.getLogger(__name__)


class TestJobs(TestWebInterfaces):
    async def test_sync_calendar(self):
        try:
            await self.server_post("jobs", "sync_calendar", is_pickled=False)
            self.assertTrue(True, "no errors")
        except Exception as e:
            logger.exception(e)
            self.assertTrue(False)

    async def test_sync_security_list(self):
        try:
            await self.server_post("jobs", "sync_security_list", is_pickled=False)
            self.assertTrue(True, "no errors")
        except Exception as e:
            logger.exception(e)
            self.assertTrue(False)

    async def test_bars_sync(self):
        try:
            resp = await self.server_post("jobs", "sync_bars", is_pickled=False)
            self.assertTrue(resp.startswith("sync_bars with"))
        except Exception as e:
            logger.exception(e)
            self.assertTrue(False)
