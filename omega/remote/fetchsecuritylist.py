#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging
from typing import TYPE_CHECKING

import numpy as np
from pyemit.remote import Remote

if TYPE_CHECKING:
    pass
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


class FetchSecurityList(Remote):
    def __init__(self):
        super().__init__(timeout=20)

    async def server_impl(self):
        secs = await AbstractQuotesFetcher.get_security_list()
        await super().respond(secs)

    async def invoke(self) -> np.array:
        # just for type hint, no special implementation needed
        return await super().invoke()
