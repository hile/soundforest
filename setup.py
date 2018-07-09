"""
Setup for soundforest package for setuptools
"""

import glob
from setuptools import setup, find_packages
from soundforest import __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='soundforest',
    keywords='Sound Audio File Tree Codec Database',
    description='Audio file library manager',
    long_description=long_description,
    version=__version__,
    author='Ilkka Tuohela',
    author_email='hile@iki.fi',
    license='PSF',
    python_requires='>=3.4',
    url='https://github.com/hile/soundforest',
    packages=find_packages(),
    scripts=glob.glob('bin/*'),
    install_requires=(
        'sqlalchemy>=1.0.11',
        'setproctitle',
        'requests',
        'lxml',
        'pytz',
        'mutagen',
        'pillow',
    ),
    classifiers=(
        "Programming Language :: Python :: 3",
    )
)
