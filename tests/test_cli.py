import unittest

from omega import cli
from tests import init_test_env


class TestCLI(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.cfg = init_test_env()

    def _count_configured_sessions(self):
        count = 0
        for group in self.cfg.quotes_fetchers:
            workers = group.get("workers", None)
            if not workers:
                continue

            for worker in workers:
                count += worker.get("sessions", 1)

        return count

    def test_omega_lifecycle(self):
        cli.start("omega")
        procs = cli.find_fetcher_processes()
        self.assertTrue(len(procs) >= 1)

        cli.status()

        cli.stop("omega")
        procs = cli.find_fetcher_processes()
        self.assertEqual(0, len(procs))
