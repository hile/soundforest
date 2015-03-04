#!/usr/bin/env python
"""
Setup for soundforest package for setuptools
"""

import glob
from setuptools import setup, find_packages

VERSION ='3.4.5'

setup(
    name = 'soundforest',
    keywords = 'Sound Audio File Tree Codec Database',
    description = 'Audio file library manager',
    version = VERSION,
    author = 'Ilkka Tuohela',
    author_email = 'hile@iki.fi',
    license = 'PSF',
    url = 'https://github.com/hile/soundforest',
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
