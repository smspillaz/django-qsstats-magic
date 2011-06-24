#!/usr/bin/env python
# -*- coding: utf-8 -*-

from django.conf import settings
from django.core.management import call_command
import sys

engine = sys.argv[1]

settings.configure(
    INSTALLED_APPS=('qsstats', 'django.contrib.auth', 'django.contrib.contenttypes'),
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.' + engine,
            'NAME': 'test'
        }
    }
)

if __name__ == "__main__":
    call_command('test', 'qsstats')
