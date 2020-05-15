#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging

from arrow import Arrow
from omicron.core.types import FrameType
from pyemit.remote import Remote

from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


class FetchQuotes(Remote):
    def __init__(self, sec: str, end: Arrow, n_bars: int, frame_type: FrameType):
        super().__init__(timeout=20)
        self.sec = sec
        self.end = end
        self.n_bars = n_bars
        self.frame_type = frame_type

    async def server_impl(self):
        logger.debug(">>>fetcher %s (%s, %s, %s)", self.sec, self.end, self.n_bars,
                     self.frame_type)
        result = await AbstractQuotesFetcher.get_bars(self.sec, self.end, self.n_bars,
                                                      self.frame_type)
        logger.debug("<<<fetcher %s (%s, %s, %s) returns: %s", self.sec, self.end,
                     self.n_bars, self.frame_type, result)
        await super().respond(result)
