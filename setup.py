#!/usr/bin/env python
"""
Setup for soundforest package for setuptools
"""

import glob
import os

from setuptools import setup, find_packages
from soundforest import __version__

setup(
    name = 'soundforest',
    keywords = 'Sound Audio File Tree Codec Database',
    description = 'Audio file library manager',
    author = 'Ilkka Tuohela',
    author_email = 'hile@iki.fi',
    license = 'PSF',
    url = 'https://github.com/hile/soundforest',
    packages = find_packages(),
    scripts = glob.glob('bin/*'),
    version = __version__,
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
