#!/usr/bin/env python
# encoding: UTF-8

from distutils.core import setup

setup(
    name = 'db_migration',
    version = 'VERSION',
    author = 'Michel Casabianca',
    author_email = 'casa@sweetohm.net',
    packages = ['db_migration'],
    url = 'http://pypi.python.org/pypi/db_migration/',
    license = 'Apache Software License',
    description = 'db_migration is a database migration tool',
    long_description=open('README.rst').read(),
)
