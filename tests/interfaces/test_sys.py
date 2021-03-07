import logging

from omega import __version__
from tests.interfaces.test_web_interfaces import TestWebInterfaces

logger = logging.getLogger(__name__)


class TestSys(TestWebInterfaces):
    async def test_sever_version(self):
        ver = await self.server_get("sys", "version", is_pickled=False)
        self.assertEqual(__version__, ver)
