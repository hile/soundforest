#!/usr/bin/env python
"""
Setup for soundforest package for setuptools
"""

import os,glob
from setuptools import setup,find_packages

VERSION='2.0.0'
README = open(os.path.join(os.path.dirname(__file__),'README.txt'),'r').read()

setup(
    name = 'soundforest',
    keywords = 'Sound Audio File Tree Codec Database',
    description = 'Audio file library manager',
    long_description = README,
    version = VERSION,
    author = 'Ilkka Tuohela',
    author_email = 'hile@iki.fi',
    license = 'PSF',
    url = 'http://tuohela.net/packages/soundforest',
    zip_safe = False,
    packages = ['soundforest']+ ['soundforest.%s' % p for p in find_packages('soundforest')],
    install_requires = ['musa>=2.0.1','sqlalchemy'],
    scripts = glob.glob('bin/*'),
)
