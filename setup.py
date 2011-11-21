#!/usr/bin/env python

import sys

from os.path import abspath, join, split
from setuptools import setup

sys.path.insert(0, join(split(abspath(__file__))[0], 'lib'))
from hypy import __version__ as _hypy_version

setup(name='hypy',
      version=_hypy_version,
      description='An intuitive HyPhy interface for Python',
      author='N Lance Hepler',
      author_email='nlhepler@gmail.com',
      url='http://github.com/nlhepler/hypy',
      license='GNU GPL version 3',
      packages=['hypy'],
      package_dir={'hypy': 'lib/hypy'}
     )
