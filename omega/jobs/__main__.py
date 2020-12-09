#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

    required coz we want the package is executable
"""
import logging

import fire

from omega.jobs import start

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    fire.Fire({"start": start})
