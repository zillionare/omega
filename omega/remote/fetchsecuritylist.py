#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging

import cfg4py
from pyemit.remote import Remote
import numpy as np

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from omega.config.cfg4py_auto_gen import Config
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


class FetchSecurityList(Remote):
    def __init__(self):
        super().__init__()

    async def server_impl(self):
        secs = await AbstractQuotesFetcher.get_security_list()
        await super().respond(secs)

    async def invoke(self) -> np.array:
        # just for type hint, no special implementation needed
        return await super().invoke()
