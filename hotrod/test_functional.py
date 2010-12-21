#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Functional Python Hot Rod client test.

Copyright (c) 2010  Galder Zamarreño
"""

import unittest
import time
import hotrod

from hotrod import HotRodClient
from hotrod import HotRodError

class FunctionalTest(unittest.TestCase):
  def setUp(self):
    self.hr = HotRodClient()

  def tearDown(self):
    self.hr.clear()
    self.hr.stop()

  def test_put(self):
    self.assertEquals(self.hr.put(self._k(), self._v()), 0)

  def test_put_with_lifespan(self):
    self.assertEquals(self.hr.put(self._k(), self._v(), 1, 0), 0)
    time.sleep(2)
    self.assertEquals(self.hr.get(self._k()), None)

  def test_put_with_max_idle(self):
    self.assertEquals(self.hr.put(self._k(), self._v(), 0, 1), 0)
    time.sleep(2)
    self.assertEquals(self.hr.get(self._k()), None)

  def test_put_with_lifespan_smaller_than_max_idle(self):
    self.assertEquals(self.hr.put(self._k(), self._v(), 1, 3), 0)
    time.sleep(2)
    self.assertEquals(self.hr.get(self._k()), None)

  def test_put_with_max_idle_smaller_than_lifespan(self):
    self.assertEquals(self.hr.put(self._k(), self._v(), 3, 1), 0)
    time.sleep(2)
    self.assertEquals(self.hr.get(self._k()), None)

  def test_put_returning_previous(self):
    prev = self._v()
    self.assertEquals(self.hr.put(self._k(), prev), 0)
    new = self._v(2)
    self.assertEquals(self.hr.put(self._k(), new, 0, 0, True), prev)
    self.assertEquals(self.hr.get(self._k()), new)

  def test_get_not_present(self):
    self.assertEquals(self.hr.get(self._k()), None)

  def test_get(self):
    self.assertEquals(self.hr.put(self._k(), self._v()), 0)
    self.assertEquals(self.hr.get(self._k()), self._v())

  def test_clear(self):
    for i in range(10):
      self.assertEquals(self.hr.put(self._k(i), self._v(i)), 0)
      self.assertEquals(self.hr.get(self._k(i)), self._v(i))
    self.assertEquals(self.hr.clear(), None)
    for i in range(10):
      self.assertEquals(self.hr.get(self._k(i)), None)

  def test_operate_on_named_cache(self):
    cache_name = "AnotherCache"
    self.another_hr = HotRodClient('127.0.0.1', 11222, cache_name)
    try:
      self.assertEquals(self.hr.get(self._k()), None)
      self.assertEquals(self.another_hr.get(self._k()), None)
      # Store only in default cache
      self.assertEquals(self.hr.put(self._k(), self._v()), 0)
      self.assertEquals(self.hr.get(self._k()), self._v())
      self.assertEquals(self.another_hr.get(self._k()), None)
      # Clear default and put on another cache
      self.assertEquals(self.hr.clear(), None)
      self.assertEquals(self.another_hr.put(self._k(), self._v()), 0)
      self.assertEquals(self.another_hr.get(self._k()), self._v())
      self.assertEquals(self.hr.get(self._k()), None)
      # Clear another cache
      self.assertEquals(self.another_hr.clear(), None)
      self.assertEquals(self.hr.get(self._k()), None)
      self.assertEquals(self.another_hr.get(self._k()), None)
    finally:
      self.another_hr.clear()
      self.another_hr.stop()

  def test_operate_on_undefined_cache(self):
    self._operate_on_undefined_cache(self._hr_get)
    self._operate_on_undefined_cache(self._hr_put)
    self._operate_on_undefined_cache(self._hr_clear)

  # TODO: test put on a topology cache and see error handling
  # TODO: test put if absent with: exist, no exist, with lifespan/maxidle, and return previous
  # TODO: test replace with: exist, no exist, with lifespan/maxidle, and return previous
  # TODO: test get with version, replace if unmodified, remove, remove if umodified,
  # TODO: test contains key, stats, ping, bulk get

  def _k(self, index=-1):
    if index == -1:
      return self._with_method("k-")
    else:
      return self._with_method("k%d-" % index)

  def _v(self, index=-1):
    if index == -1:
      return self._with_method("v-")
    else:
      return self._with_method("v%d-" % index)

  def _with_method(self, prefix):
    return prefix + self._testMethodName

  def _hr_get(self, hr):
    hr.get(self._k())

  def _hr_put(self, hr):
    hr.put(self._k(), self._v())

  def _hr_clear(self, hr):
    hr.clear()

  def _operate_on_undefined_cache(self, hrf):
    cache_name = "UndefinedCache"
    self.another_hr = HotRodClient('127.0.0.1', 11222, cache_name)
    try:
      hrf(self.another_hr)
      raise "Operating on an undefined cache should have returned error"
    except HotRodError, e:
      self.assertEquals(e.status, hotrod.SERVER_ERROR)
    finally:
      self.another_hr.stop()

if __name__ == '__main__':
  unittest.main()