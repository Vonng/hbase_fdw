#!/usr/bin/env python
# -*- coding: utf-8 -*- #
__author__ = 'Vonng (fengruohang@outlook.com)'

import json
import requests
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log


class HappyBaseFdw(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(HappyBaseFdw, self).__init__(options, columns)
        self.columns = columns
        self.options = options
        self.mode = self.options.get('mode', 'dev')

    def execute(self, quals, columns, sortkeys=None):
        log("You are brave to dive into here")
        yield {}
