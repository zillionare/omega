# -*- coding: utf-8 -*-
# !/usr/bin/env python
import fire

from .app import start

if __name__ == '__main__':
    fire.Fire({"start": start})
