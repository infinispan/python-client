#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Functional Python Hot Rod client test.

Copyright (c) 2010  Galder Zamarreño
"""

import unittest
import time

from hotrod import HotRodClient

class FunctionalTest(unittest.TestCase):

   def setUp(self):
      self.hr = HotRodClient()

   def tearDown(self):
      self.hr.stop()

   def testPutBasic(self):
      self.assertEquals(self.hr.put(self.k(), self.v()), 0)

   def testPutWithLifespan(self):
      self.assertEquals(self.hr.put(self.k(), self.v(), 1, 0), 0)
      time.sleep(2)
      self.assertEquals(self.hr.get(self.k()), None)

   def testPutWithMaxIdle(self):
      self.assertEquals(self.hr.put(self.k(), self.v(), 0, 1), 0)
      time.sleep(2)
      self.assertEquals(self.hr.get(self.k()), None)

   def testPutWithMaxIdle(self):
      self.assertEquals(self.hr.put(self.k(), self.v(), 0, 1), 0)
      time.sleep(2)
      self.assertEquals(self.hr.get(self.k()), None)

   def testPutWithLifespanSmallerThanMaxLive(self):
      self.assertEquals(self.hr.put(self.k(), self.v(), 1, 3), 0)
      time.sleep(2)
      self.assertEquals(self.hr.get(self.k()), None)

   def testPutWithMaxLiveSmallerThanLifespan(self):
      self.assertEquals(self.hr.put(self.k(), self.v(), 3, 1), 0)
      time.sleep(2)
      self.assertEquals(self.hr.get(self.k()), None)

   def testPutReturningPrevious(self):
      prev = self.v()
      self.assertEquals(self.hr.put(self.k(), prev), 0)
      self.assertEquals(self.hr.put(self.k(), self.withMethod("v2-"), 0, 0, True), prev)

   def testGetNotPresent(self):
      self.assertEquals(self.hr.get(self.k()), None)

   def testGetBasic(self):
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