import json
import logging
import os
import signal
import subprocess
import sys
import time

import aiohttp
import cfg4py

from omega.config import get_config_dir

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


def init_test_env():
    import logging

    logging.captureWarnings(True)

    os.environ[cfg4py.envar] = "DEV"
    os.environ["REDIS_HOST"] = "localhost"
    os.environ["REDIS_PORT"] = "6379"
    os.environ["POSTGRES_USER"] = "zillionare"
    os.environ["POSTGRES_PASSWORD"] = "123456"

    cfg4py.init(get_config_dir(), False)
    # enable postgres for unittest
    cfg.postgres.enabled = True
    return cfg


async def is_local_omega_alive(port: int = 3181):
    try:
        url = f"http://localhost:{port}/sys/version"
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as resp:
                if resp.status == 200:
                    return True
    except Exception:
        pass

    return False


async def start_omega(port: int = 3181):
    if await is_local_omega_alive(port):
        msg = (
            "omega is running on localhost. However, it may fails the test due to it's"
            "not started with configurations required by unittest"
        )
        logger.warning(msg)
        return None

    cfg.omega.urls.quotes_server = f"http://localhost:{port}"
    account = os.environ["JQ_ACCOUNT"]
    password = os.environ["JQ_PASSWORD"]

    # hack: by default postgres is disabled, but we need it enabled for ut
    cfg_ = json.dumps({"postgres": {"dsn": cfg.postgres.dsn, "enabled": "true"}})

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "omega.app",
            "start",
            "--impl=jqadaptor",
            f"-cfg={cfg_}",
            f"--account={account}",
            f"--password={password}",
            f"--port={port}",
        ],
        env=os.environ,
    )
    for i in range(60, 0, -1):
        time.sleep(1)
        if await is_local_omega_alive():
            # return the process id, the caller should shutdown it later
            logger.info("omega sever started: %s", process.pid)
            return process

    os.kill(process.pid, signal.SIGINT)
    raise TimeoutError("Omega server is not started.")


async def is_local_archive_server_alive(port):
    try:
        url = f"http://localhost:{port}/index.yml"
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as resp:
                if resp.status == 200:
                    return True
    except Exception:
        pass

    return False


async def start_archive_server(port=8003):
    _dir = os.path.join(os.path.dirname(__file__), "data")

    process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "-d", _dir, str(port)]
    )

    for i in range(5, 0, -1):
        time.sleep(1)
        if await is_local_archive_server_alive(port):
            return process

    raise TimeoutError("Archieved Bars server not started")
