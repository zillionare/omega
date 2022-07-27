#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
管理应用程序生命期、全局对象、任务、全局消息响应
"""
import asyncio
import logging
import os
import subprocess
import sys
from typing import Any, Callable, List, Union

import cfg4py
import fire
from termcolor import colored

from omega.config import get_config_dir

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


def factory_config_dir():
    import omega

    module_dir = os.path.dirname(omega.__file__)
    return os.path.join(module_dir, "config")


def update_config(settings: dict, root_key: str, conf: Any):
    keys = root_key.split(".")
    current_item = settings
    for key in keys[:-1]:
        v = current_item.get(key, {})
        current_item[key] = v
        current_item = current_item[key]

    if isinstance(conf, dict):
        if current_item.get(keys[-1]):
            current_item[keys[-1]].update(conf)
        else:
            current_item[keys[-1]] = conf
    else:
        current_item[keys[-1]] = conf


def print_title(msg):
    print(colored(msg, "green"))
    print(colored("".join(["-"] * len(msg)), "green"))


async def first_init(service: str = ""):
    print(f"正在初始化系统数据 {colored(service, 'green')}...")

    config_dir = get_config_dir()
    cfg4py.init(config_dir, False)

    from omega.worker.app import init_data

    if not cfg.quotes_fetchers:  # 无数据，直接退出
        print("系统数据初始化错误，请配置正确的quotes_fetcher...")
        return

    fetcher = cfg.quotes_fetchers[0]
    impl = fetcher.get("impl")
    account = fetcher.get("account")
    password = fetcher.get("password")
    await init_data(impl, account=account, password=password)

    print(f"系统数据初始化完毕 {colored(service, 'green')}...")


async def start_worker():
    print("prepare to start Omega worker process ...")

    config_dir = get_config_dir()
    cfg4py.init(config_dir, False)

    if not cfg.quotes_fetchers:  # 无数据，直接退出
        print("系统数据初始化错误，请配置正确的quotes_fetcher...")
        return

    fetcher = cfg.quotes_fetchers[0]
    impl = fetcher.get("impl")
    account = fetcher.get("account")
    password = fetcher.get("password")
    subprocess.Popen(
        [
            sys.executable,
            "-m",
            "omega.worker.app",
            "start",
            f"--impl={impl}",
            f"--account={account}",
            f"--password={password}",
        ],
        stdout=subprocess.DEVNULL,
    )

    print("Omega worker process started ...")


def run(func):
    def wrapper(*args, **kwargs):
        asyncio.run(func(*args, **kwargs))

    return wrapper


def main():
    import warnings

    warnings.simplefilter("ignore")
    fire.Fire(
        {
            "init": run(first_init),
            "worker": run(start_worker),
        }
    )


if __name__ == "__main__":
    main()
