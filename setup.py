#!/usr/bin/env python
"""
Setup for soundforest package for setuptools
"""

import os
import glob
from setuptools import setup, find_packages

VERSION ='3.4.2'

setup(
    name = 'soundforest',
    keywords = 'Sound Audio File Tree Codec Database',
    description = 'Audio file library manager',
    version = VERSION,
    author = 'Ilkka Tuohela',
    author_email = 'hile@iki.fi',
    license = 'PSF',
    url = 'http://tuohela.net/packages/soundforest',
    packages = find_packages(),
    scripts = glob.glob('bin/*'),
    install_requires = (
        'setproctitle',
        'sqlalchemy',
        'requests',
        'lxml',
        'pytz',
        'mutagen',
        'pillow',
    ),
)
