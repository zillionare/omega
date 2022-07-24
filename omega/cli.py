#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
管理应用程序生命期、全局对象、任务、全局消息响应
"""
import asyncio
import logging
import os
from typing import Any, Callable, List, Union
import cfg4py
import fire
import omicron

from pyemit import emit
from ruamel.yaml import YAML
from termcolor import colored

from omega.common.process_utils import find_jobs_process
from omega.config import get_config_dir
from omega.worker.abstract_quotes_fetcher import AbstractQuotesFetcher

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

    for fetcher in cfg.quotes_fetchers:
        impl = fetcher.get("impl")
        workers = fetcher.get("workers")
        for group in workers:
            account = group.get("account")
            password = group.get("password")
            await init_data(impl, account=account, password=password)
            break

    print(f"系统数据初始化完毕 {colored(service, 'green')}...")


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
        }
    )


if __name__ == "__main__":
    main()
