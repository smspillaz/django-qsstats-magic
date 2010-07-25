#!/usr/bin/env python
from distutils.core import setup

setup(
    name='django-qsstats-magic',
    version='0.5.0',
    description='A django microframework that eases the generation of aggregate data for querysets.',
    author='Matt Croydon, Mikhail Korobov',
    author_email='mcroydon@gmail.com, kmike84@gmail.com',
    url='http://bitbucket.org/kmike/django-qsstats-magic/',
    packages=['qsstats'],
    requires=['dateutil(>=1.4.1)'],
)
