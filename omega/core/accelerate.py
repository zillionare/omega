#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

things need speed
"""
import logging

logger = logging.getLogger(__name__)


def merge(left, right, by):
    """merge two numpy structured arrays by `by` key

    njit fail if one of left, right contains object, not plain type, but the loop is
    very fast, cost 0.0001 seconds

    Args:
        left ([type]): [description]
        right ([type]): [description]
        by ([type]): [description]

    Returns:
        [type]: [description]
    """
    i, j = 0, 0

    while j < len(right) and i < len(left):
        if right[j][by] < left[by][i]:
            j += 1
        elif right[j][by] == left[by][i]:
            left[i] = right[j]
            i += 1
            j += 1
        else:
            i += 1

    return left
