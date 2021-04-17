import io
import os
import shutil
import unittest
from unittest import mock

import aioredis
import arrow
import cfg4py
from omega import cli
from omega.config import get_config_dir
from pyemit import emit
from ruamel.yaml import YAML
from tests import init_test_env, start_archive_server, start_omega


class TestCLI(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.cfg = init_test_env()

        # disable catch up sync, since test_cli, jobs will be launched many times and cache might by empty
        redis = await aioredis.create_redis(self.cfg.redis.dsn)
        last_sync = arrow.now(self.cfg.tz).format("YYYY-MM-DD HH:mm:SS")
        await redis.set("jobs.bars_sync.stop", last_sync)

    async def asyncTearDown(self) -> None:
        await emit.stop()

    async def _start_servers(self):
        # 将server启动独立出来，加快单元测试速度
        self.omega = await start_omega()
        self.archive = await start_archive_server()

    async def _stop_servers(self):
        try:
            if self.omega:
                self.omega.kill()
        except AttributeError:
            pass

        try:
            if self.archive:
                self.archive.kill()
        except AttributeError:
            pass

    def yaml_dumps(self, settings):
        stream = io.StringIO()
        yaml = YAML()
        yaml.indent(sequence=4, offset=2)
        try:
            yaml.dump(settings, stream)
            return stream.getvalue()
        finally:
            stream.close()

    def _count_configured_sessions(self):
        count = 0
        for group in self.cfg.quotes_fetchers:
            workers = group.get("workers", None)
            if not workers:
                continue

            for worker in workers:
                count += worker.get("sessions", 1)

        return count

    async def test_omega_lifecycle(self):
        await cli.start("fetcher")
        procs = cli.find_fetcher_processes()
        self.assertTrue(len(procs) >= 1)

        await cli.restart("fetcher")
        await cli.status()

        await cli.stop("fetcher")
        procs = cli.find_fetcher_processes()
        self.assertEqual(0, len(procs))

        await cli.start()
        await cli.restart()
        await cli.stop()

    async def test_omega_jobs(self):
        await cli.start("jobs")
        await cli.status()
        await cli.stop("jobs")

    async def test_sync_sec_list(self):
        try:
            await self._start_servers()
            await cli.sync_sec_list()
        finally:
            await self._stop_servers()

    async def test_sync_calendar(self):
        try:
            await self._start_servers()
            await cli.sync_calendar()
        finally:
            await self._stop_servers()

    async def test_sync_bars(self):
        try:
            await self._start_servers()
            await cli.sync_bars("1d", codes="000001.XSHE")
        finally:
            await self._stop_servers()

    def test_load_factory_settings(self):
        settings = cli.load_factory_settings()
        self.assertTrue(len(settings) > 0)
        self.assertEqual("Asia/Shanghai", settings.get("tz"))

    def test_update_config(self):
        settings = cli.load_factory_settings()

        key = "postgres.dsn"
        value = "postgres://blah"
        cli.update_config(settings, key, value)
        self.assertEqual(settings["postgres"]["dsn"], value)

        key = "tz"
        value = "shanghai"
        cli.update_config(settings, key, value)
        self.assertEqual(settings[key], value)

    def test_config_fetcher(self):
        settings = {}
        with mock.patch(
            "builtins.input", side_effect=["account", "password", "1", "n", "c"]
        ):
            cli.config_fetcher(settings)

        impl = settings["quotes_fetchers"][0]["impl"]
        worker = settings["quotes_fetchers"][0]["workers"][0]
        self.assertEqual("jqadaptor", impl)
        self.assertEqual("account", worker["account"])

    def test_check_environment(self):
        os.environ[cfg4py.envar] = ""

        with mock.patch("builtins.input", side_effect=["C"]):
            os.environ[cfg4py.envar] = "PRODUCTION"
            self.assertTrue(cli.check_environment())

    def test_config_logging(self):
        settings = {}
        folder = "/tmp/omega/test"
        shutil.rmtree("/tmp/omega/test", ignore_errors=True)

        # 1. function normal
        with mock.patch("builtins.input", return_value=folder):
            cli.config_logging(settings)

        try:
            logfile = os.path.join(folder, "omega.log")
            with open(logfile, "rt") as f:
                content = f.read(-1)
                msg = f"logging output is writtern to {logfile} now"
                self.assertTrue(content.find(msg))
        except Exception as e:
            print(e)

        # # fixme: disable temporarily, patch('sh.contrib.sudo.mkdir') causes issue
        # # 2. no permission to mkdir.
        # with mock.patch("os.makedirs", side_effect=PermissionError()):
        #     with mock.patch("sh.contrib.sudo.mkdir"):
        #         with mock.patch("sh.contrib.sudo.chmod"):
        #             pass  # disable prompt to enable auto test

        # 3. raise other exception, need redo
        shutil.rmtree("/tmp/omega/test", ignore_errors=True)
        with mock.patch("os.makedirs", side_effect=[Exception("mocked"), mock.DEFAULT]):
            with mock.patch("builtins.input", return_value=folder):
                with mock.patch("omega.cli.choose_action", return_value="R"):
                    try:
                        cli.config_logging(settings)
                    except FileNotFoundError:
                        # since os.makdirs are mocked, so there's no logfile created
                        pass

            self.assertEqual(2, os.makedirs.call_count)

    async def test_config_postgres(self):
        settings = {}
        with mock.patch(
            "builtins.input",
            side_effect=["R", "127.0.0.1", "6380", "account", "password", "zillionare"],
        ):
            with mock.patch("omega.cli.check_postgres", side_effect=[True]):
                await cli.config_postgres(settings)
                expected = "postgres://account:password@127.0.0.1:6380/zillionare"
                self.assertEqual(expected, settings["postgres"]["dsn"])

        # test continure with wrong config
        with mock.patch(
            "builtins.input",
            side_effect=[
                "R",
                "127.0.0.1",
                "6380",
                "account",
                "password",
                "zillionare",
                "C",
            ],
        ):
            await cli.config_postgres(settings)
            expected = "postgres://account:password@127.0.0.1:6380/zillionare"
            self.assertEqual(expected, settings["postgres"]["dsn"])

        # check connection to postgres. Need provide right info in ut
        host = os.environ.get("POSTGRES_HOST")
        port = os.environ.get("POSTGRES_PORT")
        db = os.environ.get("POSTGRES_DB")
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")

        with mock.patch(
            "builtins.input",
            side_effect=["R", host, port, user, password, db],
        ):
            result = await cli.config_postgres(settings)
            # should be no exceptions
            self.assertTrue(result)

    async def test_config_redis(self):
        # 1. normla case
        settings = {}
        with mock.patch("builtins.input", side_effect=["localhost", "6379", ""]):
            with mock.patch("omega.cli.check_redis"):
                expected = "redis://localhost:6379"
                await cli.config_redis(settings)
                self.assertEqual(expected, settings["redis"]["dsn"])

        # 2. exception case
        settings = {}
        with mock.patch("builtins.input", side_effect=["localserver", "3180", "", "C"]):
            await cli.config_redis(settings)
            self.assertDictEqual({}, settings)

    async def test_setup(self):
        # clear cache to simulate first setup
        redis = await aioredis.create_redis("redis://localhost:6379")
        await redis.flushall()
        redis.close()
        await redis.wait_closed()

        # backup configuration files
        origin = os.path.join(get_config_dir(), "defaults.yaml")
        bak = os.path.join(get_config_dir(), "defaults.bak")

        def save_config(settings):
            os.rename(origin, bak)
            with open(origin, "w") as f:
                settings["omega"]["urls"]["archive"] = self.cfg.omega.urls.archive
                f.writelines(self.yaml_dumps(settings))

        with mock.patch("omega.cli.save_config", save_config):
            with mock.patch(
                "builtins.input",
                side_effect=[
                    "/var/log/zillionare/",  # logging
                    os.environ.get("JQ_ACCOUNT"),
                    os.environ.get("JQ_PASSWORD"),
                    "1",
                    "n",  # config no more account
                    os.environ.get("REDIS_HOST"),
                    os.environ.get("REDIS_PORT"),
                    "",  # redis password
                    "",  # continue on postgres config
                    os.environ.get("POSTGRES_HOST"),
                    os.environ.get("POSTGRES_PORT"),
                    os.environ.get("POSTGRES_USER"),
                    os.environ.get("POSTGRES_PASSWORD"),
                    os.environ.get("POSTGRES_DB"),
                    "1",  # download one month archive
                ],
            ):
                try:
                    self.archive = await start_archive_server()
                    await cli.setup(force=True)
                finally:
                    if self.archive:
                        self.archive.kill()
                    os.remove(origin)
                    os.rename(bak, origin)

                    # setup has started servers
                    print("stopping omega servers")
                    await cli.stop()

    async def test_download_archive(self):
        try:
            archive_server = await start_archive_server()
            with mock.patch("builtins.input", return_value="1"):
                await cli.download_archive()
        finally:
            if archive_server:
                archive_server.kill()

    async def test_bin_cut(self):
        arr = [1, 2, 3, 4, 5]

        expected = [
            [[1, 2, 3, 4, 5]],
            [[1, 3, 5], [2, 4]],
            [[1, 4], [2, 5], [3]],
            [[1], [2], [3], [4], [5]],
            [[1], [2], [3], [4], [5]],
        ]
        for i, bins in enumerate([1, 2, 3, 5, 10]):
            self.assertListEqual(expected[i], cli.bin_cut(arr, bins))
