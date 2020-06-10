#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors: 

"""
import logging
import os
from os import path

import cfg4py

logger = logging.getLogger(__name__)


def get_config_dir():
    if os.environ[cfg4py.envar] == 'PRODUCTION':
        _dir = path.expanduser('~/zillionare/omega/config')
    elif os.environ[cfg4py.envar] == 'TEST':
        _dir = path.expanduser('~/.zillionare/omega/config')
    else:
        _dir = path.normpath(path.join(path.dirname(__file__), '../config'))

    return _dir
