#!/usr/bin/env python
"""
Setup for soundforest package for setuptools
"""

import os,glob
from setuptools import setup,find_packages

VERSION='3.3.2'

setup(
    name = 'soundforest',
    keywords = 'Sound Audio File Tree Codec Database',
    description = 'Audio file library manager',
    version = VERSION,
    author = 'Ilkka Tuohela',
    author_email = 'hile@iki.fi',
    license = 'PSF',
    url = 'http://tuohela.net/packages/soundforest',
    zip_safe = False,
    packages = ['soundforest'] + 
        ['soundforest.%s' % p for p in find_packages('soundforest')],
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
