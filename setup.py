#!/usr/bin/env python

from setuptools import setup

version = "1.0.0b1"

setup(name = 'infinispan',
      version = version,
      description = 'Native python client for Infinispan, over the Hot Rod wire protocol',
      author = 'Galder Zamarreno',
      author_email = 'galder@jboss.org',
      url = 'http://infinispan.org',
      py_modules = ['infinispan.remotecache', 'infinispan.unsigned'],
      classifiers = [
          "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
          "Programming Language :: Python",
          "Development Status :: 4 - Beta",
          "Intended Audience :: Developers",
          "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      keywords = 'infinispan hotrod nosql datagrid',
      license = 'LGPL',
      install_requires = [
        'setuptools',
        'greenlet',
      ],
      long_description = "See `Infinispan Native Python Client <https://github.com/infinispan/python-client>`_ for more information."
      )