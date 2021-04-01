#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging
import os
import sys
from os import path

import cfg4py
from termcolor import colored

logger = logging.getLogger(__name__)


def get_config_dir():
    server_role = os.environ.get(cfg4py.envar)

    if server_role == "DEV":
        _dir = path.normpath(path.join(path.dirname(__file__), "../config"))
    elif server_role == "TEST":
        _dir = path.expanduser("~/.zillionare/omega/config")
    else:
        _dir = path.expanduser("~/zillionare/omega/config")

    sys.path.insert(0, _dir)
    return _dir


def check_env():
    server_roles = ["PRODUCTION", "TEST", "DEV"]
    if os.environ.get(cfg4py.envar) not in ["PRODUCTION", "TEST", "DEV"]:
        print(
            f"请设置环境变量{colored(cfg4py.envar, 'red')}为["
            f"{colored(server_roles, 'red')}]之一。"
        )
        sys.exit(-1)
