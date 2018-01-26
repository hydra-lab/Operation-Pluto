#!/usr/bin/env python

""" Set-up """

import io
import os
from setuptools import setup

NAME = 'Operation-Pluto'
VERSION = "0.0.9"
DESCRIPTION = 'Grab and rinse financial and economic data.'
URL = 'https://github.com/hydra-lab/Operation-Pluto'
EMAIL = ''
AUTHOR = 'Operation Pluto contributors'

REQUIRED = [
    'luigi', 'python-daemon', 'requests', 'pandas', 'beautifulsoup4', 'lxml', 'xlrd', 'sqlalchemy']

HERE = os.path.abspath(os.path.dirname(__file__))

with io.open(os.path.join(HERE, 'README.rst'), encoding='utf-8') as f:
    LONG_DESCRIPTION = '\n' + f.read()

ABOUT = {}
with open(os.path.join(HERE, 'pluto', '__version__.py')) as f:
    exec(f.read(), ABOUT)

setup(
    name=NAME,
    version=ABOUT['__version__'],
    author=AUTHOR,
    author_email='',
    description=DESCRIPTION,
    url=URL,
    keywords="data-pipeline finance market",
    long_description=LONG_DESCRIPTION,
    license="AGPL-3.0",
    classifiers=[
        'Topic :: Office/Business :: Financial :: Investment',
        'Programming Language :: Python :: 3.5',
        'Development Status :: 2 - Pre-Alpha',
    ],
    packages=['pluto', 'test'],
    install_requires=REQUIRED,
)
