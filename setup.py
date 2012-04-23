#!/usr/bin/env python
"""
Setup for soundforest package for setuptools
"""

import os,glob
from setuptools import setup

VERSION='1.0.1'
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
    install_requires = [ 'systematic>=1.4.1', 'PIL', 'mutagen' ],
    scripts = glob.glob('bin/*'),
    packages = ['soundforest','soundforest.tags','soundforest.tags.formats'],
)
