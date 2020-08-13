#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-x-y',
      version='0.0.1',
      description='Singer.io tap for extracting data from the XY API',
      author='scott.coleman@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_ms_teams'],
      install_requires=[
          'singer-python==5.9.0',
          'backoff==1.8.0',
          'requests==2.23.0',
          'pyhumps==1.6.1'
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'nose',
          ]
      },
      python_requires='>=3.5.6',
      entry_points='''
          [console_scripts]
          tap-x-y=tap_x_y:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_x_y': [
              'schemas/*.json'
          ]
      })
