#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup

version = "1.0.0.BETA1"

setup(name = 'infinispan',
      version = version,
      description = 'Native python client for Infinispan, over the Hot Rod wire protocol',
      author = 'Galder Zamarreño',
      author_email = 'galder@jboss.org',
      url = 'http://infinispan.org',
      py_modules = ['infinispan.remotecache', 'infinispan.unsigned']
      )