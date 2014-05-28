#!/usr/bin/env python

from distutils.core import setup

setup(name='zmeter',
      version='0.2.4',
      description='Send system metrics via ZeroMQ socket',
      author='Min Yu',
      author_email='miniway@gmail.com',
      url='http://github.com/miniway/zmeter',
      packages=['zmeter', 'zmeter.plugins'],
      package_dir = {'zmeter': 'lib'}
     )
