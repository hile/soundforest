#!/usr/bin/env python

import os,glob
from setuptools import setup

VERSION='1.0.1'
README = open(os.path.join(os.path.dirname(__file__),'README.txt'),'r').read()

setup(
    name = 'soundforest',
    version = VERSION,
    license = 'PSF',
    keywords = 'Sound File Tree',
    url = 'http://tuohela.net/packages/soundforest',
    zip_safe = False,
    install_requires = [ 'systematic' ],
    scripts = glob.glob('bin/*'),
    packages = [ 'soundforest' ],
    author = 'Ilkka Tuohela', 
    author_email = 'hile@iki.fi',
    description = 'Detect audio file libraries',
    long_description = README,

)   

