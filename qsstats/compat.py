# -*- coding: utf-8 -*-


import datetime
try:
    from django.utils.timezone import now
except ImportError:
    now = datetime.datetime.now
