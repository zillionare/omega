import aioredis
import asyncio
import json
import logging
import os
import signal
import socket
import subprocess
import sys
from contextlib import closing

import aiohttp
import arrow
import cfg4py
from omega.config import get_config_dir

cfg = cfg4py.get_instance()
logger = logging.getLogger(__name__)


def init_test_env():
    import logging

    logging.captureWarnings(True)

    os.environ[cfg4py.envar] = "DEV"

    cfg4py.init(get_config_dir(), False)
    # enable postgres for unittest
    cfg.postgres.enabled = True

    return cfg


async def is_local_omega_alive():
    try:
        url = f"{cfg.omega.urls.quotes_server}/sys/version"
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as resp:
                if resp.status == 200:
                    return True
    except Exception:
        pass

    return False


async def start_omega(timeout=60):
    port = find_free_port()

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
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for i in range(timeout, 0, -1):
        await asyncio.sleep(1)
        if process.poll() is not None:
            # already exit, due to finish or fail
            out, err = process.communicate()
            logger.info("subprocess %s: %s", process.pid, out.decode("utf-8"))
            raise subprocess.SubprocessError(err.decode("utf-8"))

        if await is_local_omega_alive():
            # return the process id, the caller should shutdown it later
            logger.info(
                "omega server(%s) is listen on %s",
                process.pid,
                cfg.omega.urls.quotes_server,
            )

            return process

    os.kill(process.pid, signal.SIGINT)
    raise TimeoutError("Omega server is not started.")


async def is_local_job_server_alive(port):
    try:
        url = f"http://localhost:{port}/jobs/status"
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as resp:
                if resp.status == 200:
                    return True
    except Exception:
        pass

    return False


async def start_job_server(port, timeout=30):
    process = subprocess.Popen(
        [sys.executable, "-m", "omega.jobs", "start", f"--port={port}"],
        env=os.environ,
    )

    for i in range(timeout, 0, -1):
        await asyncio.sleep(1)
        if await is_local_job_server_alive(port):
            return process

    raise TimeoutError("jobs server not started")


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


async def start_archive_server(timeout=30):
    port = find_free_port()
    cfg.omega.urls.archive = f"http://localhost:{port}"
    _dir = os.path.join(os.path.dirname(__file__), "data")

    process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "-d", _dir, str(port)]
    )

    for i in range(timeout, 0, -1):
        await asyncio.sleep(1)
        if await is_local_archive_server_alive(port):
            return process

    raise TimeoutError("Archieved Bars server not started")


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        # s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]
        return port
