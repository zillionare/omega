#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import logging
import warnings

from numba import njit, NumbaPendingDeprecationWarning

warnings.filterwarnings("ignore", category=NumbaPendingDeprecationWarning)

logger = logging.getLogger(__name__)


@njit
def left_join(left, right, by):
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
