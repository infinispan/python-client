#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Functional Python Hot Rod client test.

Copyright (c) 2010  Galder Zamarreño
"""

import unittest

from hotrod import HotRodClient

class FunctionalTest(unittest.TestCase):

   def setUp(self):
      self.hr = HotRodClient()

   def tearDown(self):
      self.hr.stop()

   def testSimplePut(self):
      """Test a simple put."""
      self.assertEquals(self.hr.put(self.k(), self.v()), 0)

   def testSimplePutGet(self):
      """Test a simple put and get."""
      self.assertEquals(self.hr.put(self.k(), self.v()), 0)
      self.assertEquals(self.hr.get(self.k()), self.v())

   def k(self):
      return self.withMethod("k-")

   def v(self):
      return self.withMethod("v-")

   def withMethod(self, prefix):
      return prefix + self._testMethodName

if __name__ == '__main__':
    unittest.main()