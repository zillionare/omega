import asyncio
import logging
import subprocess
import sys
import unittest
from unittest import mock

import cfg4py
import omicron
import psutil
import rlog

import omega
from omega.core.constants import PROC_LOCK_OMEGA_MASTER
from omega.master.app import start_logging
from tests import init_test_env


def find_process(identity_string):
    for p in psutil.process_iter():
        try:
            cmd = " ".join(p.cmdline())
            if cmd.find(identity_string) != -1:
                return p.pid
        except (PermissionError, ProcessLookupError):
            pass
    return None


class AppTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_test_env()
        await omicron.init()

    async def asyncTearDown(self) -> None:
        await omicron.close()

    async def test_start_logging(self):
        cfg4py.update_config(
            {
                "logreceiver": {
                    "klass": "omega.logging.receiver.redis.RedisLogReceiver",
                    "dsn": "redis://localhost:6379",
                    "channel": "test_redis_logging",
                    "filename": "/tmp/log/zillionare/omega.log",
                    "backup_count": 1,
                    "max_bytes": 100,
                }
            }
        )

        cfg = cfg4py.get_instance()
        root = logging.getLogger()
        root.handlers.clear()

        redis_logger = logging.getLogger("test_redis")
        fmt = "%(asctime)s %(levelname)-1.1s %(process)d %(name)s:%(funcName)s:%(lineno)s | %(message)s"

        handler = rlog.RedisHandler(
            channel=cfg.logreceiver.channel,
            level=logging.DEBUG,
            host="localhost",
            port="6379",
            formatter=logging.Formatter(fmt),
        )
        redis_logger.addHandler(handler)

        receiver = await start_logging()
        msg = "redis log receiving should be ready"
        redis_logger.info(msg)

        await asyncio.sleep(0.5)
        await receiver.stop()
        with open(cfg.logreceiver.filename, "r") as f:
            content = f.read(-1)
            self.assertTrue(content.find(msg) != -1)

    @mock.patch("omega.master.app.start_logging")
    @mock.patch("omicron.init")
    @mock.patch("omega.scripts.load_lua_script")
    @mock.patch("apscheduler.schedulers.asyncio.AsyncIOScheduler.add_job")
    @mock.patch("apscheduler.schedulers.asyncio.AsyncIOScheduler.start")
    @mock.patch("pyemit.emit.register")
    @mock.patch("pyemit.emit.start")
    @mock.patch("omega.master.app.load_cron_task")
    @mock.patch("omega.master.app.heartbeat")
    async def test_init(
        self,
        mock_heartbeat,
        mock_load_cron_task,
        mock_emit_start,
        mock_emit_register,
        mock_start_scheduler,
        mock_add_job,
        mock_load_lua_script,
        mock_omicron_init,
        mock_start_logging,
    ):
        # 测试的目的是保证在初始化时，各项动作均已执行
        # 所以不需要测试具体的动作
        await omega.master.app.init()
        mock_start_logging.assert_called_once()
        mock_omicron_init.assert_called_once()
        mock_load_lua_script.assert_called_once()
        mock_load_cron_task.assert_called_once()
        mock_start_scheduler.assert_called_once()
        self.assertTrue(
            str(mock_add_job.call_args_list).find("rebuild_unclosed_bars") != -1
        )
        self.assertTrue(str(mock_add_job.call_args_list).find("heartbeat") != -1)
        mock_emit_start.assert_called_once()
        mock_emit_register.assert_called_once()
        mock_heartbeat.assert_called()

    async def test_start(self):
        await omicron.cache.sys.delete(PROC_LOCK_OMEGA_MASTER)
        # 测试能否在本机正常启动，退出时能否释放锁
        proc = subprocess.Popen(
            [sys.executable, "-m", "omega.master.app", "start"], shell=False
        )

        # do the check
        for _ in range(60):
            result = await omicron.cache.sys.get(PROC_LOCK_OMEGA_MASTER)
            if result:
                break
            await asyncio.sleep(0.5)
        else:  # failed to start omega master in 30 seconds
            self.assertTrue(False)

        # kill proc, check if the lock is released
        await asyncio.sleep(0.5)
        self.assertTrue(find_process("omega.master"))
        proc.terminate()
        proc.wait()
        for _ in range(60):
            result = await omicron.cache.sys.get(PROC_LOCK_OMEGA_MASTER)
            if result is None:
                break
            await asyncio.sleep(0.5)
        else:
            self.assertTrue(False)

        # 无法启动
        await omicron.cache.sys.set(PROC_LOCK_OMEGA_MASTER, 1)
        proc = subprocess.Popen(
            [sys.executable, "-m", "omega.master.app", "start"],
        )

        await asyncio.sleep(3)
        _pid = find_process("omega.master")
        self.assertTrue(_pid != proc.pid)
        await omicron.cache.sys.delete(PROC_LOCK_OMEGA_MASTER)
