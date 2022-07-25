# -*- coding: utf-8 -*-
# @Author   : xiaohuzi
# @Time     : 2022-01-14 16:37
import logging

logger = logging.getLogger(__name__)


class WorkerException(Exception):
    msg = "未知错误"

    def __init__(self, msg=None):
        super(Exception, self).__init__()
        self.msg = msg or self.msg


class GotNoneData(WorkerException):
    msg = "Got None Data"
