import os
import subprocess
import sys
import time

import aiohttp
import cfg4py

from omega.core import get_config_dir


def init_test_env():
    os.environ[cfg4py.envar] = "DEV"

    cfg4py.init(get_config_dir(), False)
    # enable postgres for unittest
    cfg = cfg4py.get_instance()
    cfg.postgres.enabled = True
    return cfg


async def is_local_omega_alive(port: int = 3181):
    try:
        url = f"http://localhost:{port}/sys/version"
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as resp:
                if resp.status == 200:
                    return await resp.text()
        return True
    except Exception:
        return False


async def start_omega(port: int = 3181):
    if await is_local_omega_alive(port):
        return None

    account = os.environ["jq_account"]
    password = os.environ["jq_password"]

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "omega.app",
            "start",
            "jqadaptor",
            f"--account={account}",
            f"--password={password}",
            f"--port={port}",
        ],
        env=os.environ,
    )
    for i in range(5, 0, -1):
        time.sleep(2)
        if await is_local_omega_alive():
            # return the process id, the caller should shutdown it later
            return process

    raise EnvironmentError("Omega server is not started.")
