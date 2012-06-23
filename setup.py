#!/usr/bin/env python

from __future__ import division, print_function

import sys

from os.path import abspath, join, split
from setuptools import setup

sys.path.insert(0, join(split(abspath(__file__))[0], 'lib'))
from hppy import __version__ as _hppy_version

setup(name='hppy',
      version=_hppy_version,
      description='An intuitive HyPhy interface for Python',
      author='N Lance Hepler',
      author_email='nlhepler@gmail.com',
      url='http://github.com/nlhepler/hppy',
      license='GNU GPL version 3',
      packages=['hppy'],
      package_dir={'hppy': 'lib/hppy'},
      requires=['HyPhy (>=0.1)']
     )
