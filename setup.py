"""
Setup for soundforest package for setuptools
"""

import glob
from setuptools import setup, find_packages
from soundforest import __version__

setup(
    name = 'soundforest',
    keywords = 'Sound Audio File Tree Codec Database',
    description = 'Audio file library manager',
    version = __version__,
    author = 'Ilkka Tuohela',
    author_email = 'hile@iki.fi',
    license = 'PSF',
    url = 'https://github.com/hile/soundforest',
    packages = find_packages(),
    scripts = glob.glob('bin/*'),
    install_requires = (
        'sqlalchemy>=1.0.11',
        'setproctitle',
        'requests',
        'lxml',
        'pytz',
        'mutagen',
        'pillow',
    ),
)
