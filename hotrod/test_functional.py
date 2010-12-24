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
from hotrod import SUCCESS, NOT_EXECUTED

class FunctionalTest(unittest.TestCase):
  def setUp(self):
    self.hr = HotRodClient()

  def tearDown(self):
    self.hr.clear()
    self.hr.stop()

  def test_put(self):
    self.assertEquals(self.hr.put(self.k(), self.v()), SUCCESS)

  def test_put_with_lifespan(self):
    self.assertEquals(self.hr.put(self.k(), self.v(), 1, 0), SUCCESS)
    time.sleep(1.1)
    self.assertEquals(self.hr.get(self.k()), None)

  def test_put_with_max_idle(self):
    self.assertEquals(self.hr.put(self.k(), self.v(), 0, 1), SUCCESS)
    time.sleep(1.1)
    self.assertEquals(self.hr.get(self.k()), None)

  def test_put_with_lifespan_smaller_than_max_idle(self):
    self.assertEquals(self.hr.put(self.k(), self.v(), 1, 3), SUCCESS)
    time.sleep(1.1)
    self.assertEquals(self.hr.get(self.k()), None)

  def test_put_with_max_idle_smaller_than_lifespan(self):
    self.assertEquals(self.hr.put(self.k(), self.v(), 3, 1), SUCCESS)
    time.sleep(1.1)
    self.assertEquals(self.hr.get(self.k()), None)

  def test_put_returning_previous(self):
    old = self.v()
    self.assertEquals(self.hr.put(self.k(), old), SUCCESS)
    new = self.v(2)
    self.assertEquals(self.hr.put(self.k(), new, 0, 0, True), (SUCCESS, old))
    self.assertEquals(self.hr.get(self.k()), new)
    self.assertEquals(self.hr.put(self.k(2), new, 0, 0, True), (SUCCESS, None))

  def test_get_not_present(self):
    self.assertEquals(self.hr.get(self.k()), None)

  def test_get(self):
    self.assertEquals(self.hr.put(self.k(), self.v()), SUCCESS)
    self.assertEquals(self.hr.get(self.k()), self.v())

  def test_clear(self):
    for i in range(10):
      self.assertEquals(self.hr.put(self.k(i), self.v(i)), SUCCESS)
      self.assertEquals(self.hr.get(self.k(i)), self.v(i))
    self.assertEquals(self.hr.clear(), SUCCESS)
    for i in range(10):
      self.assertEquals(self.hr.get(self.k(i)), None)

  def test_operate_on_named_cache(self):
    cache_name = "AnotherCache"
    self.another_hr = HotRodClient('127.0.0.1', 11222, cache_name)
    try:
      self.assertEquals(self.hr.get(self.k()), None)
      self.assertEquals(self.another_hr.get(self.k()), None)
      # Store only in default cache
      self.assertEquals(self.hr.put(self.k(), self.v()), SUCCESS)
      self.assertEquals(self.hr.get(self.k()), self.v())
      self.assertEquals(self.another_hr.get(self.k()), None)
      # Clear default and put on another cache
      self.assertEquals(self.hr.clear(), SUCCESS)
      self.assertEquals(self.another_hr.put(self.k(), self.v()), SUCCESS)
      self.assertEquals(self.another_hr.get(self.k()), self.v())
      self.assertEquals(self.hr.get(self.k()), None)
      # Clear another cache
      self.assertEquals(self.another_hr.clear(), SUCCESS)
      self.assertEquals(self.hr.get(self.k()), None)
      self.assertEquals(self.another_hr.get(self.k()), None)
    finally:
      self.another_hr.clear()
      self.another_hr.stop()

  def test_operate_on_undefined_cache(self):
    expr = lambda msg: "CacheNotFoundException" in msg
    self._expect_error(lambda hr: hr.get(self.k()), expr)
    self._expect_error(lambda hr: hr.put(self.k(), self.v()), expr)
    self._expect_error(lambda hr: hr.clear(), expr)

  def test_operate_on_topology_cache(self):
    expr = lambda msg: "Remote requests are not allowed to topology cache." in msg
    cache_name = "___hotRodTopologyCache"
    self._expect_error(lambda hr: hr.get(self.k()), expr, cache_name)
    self._expect_error(lambda hr: hr.put(self.k(), self.v()), expr, cache_name)
    self._expect_error(lambda hr: hr.clear(), expr, cache_name)

  def test_put_if_absent(self):
    # If key not present and then when present
    self.assertEquals(self.hr.put_if_absent(self.k(), self.v()), SUCCESS)
    self.assertEquals(self.hr.put_if_absent(self.k(), self.v(2)), NOT_EXECUTED)
    # With lifespan
    self.assertEquals(self.hr.put_if_absent(self.k(2), self.v(), 1, 0), SUCCESS)
    time.sleep(1.1)
    self.assertEquals(self.hr.get(self.k(2)), None)
    # With maxidle
    self.assertEquals(self.hr.put_if_absent(self.k(3), self.v(), 0, 1), SUCCESS)
    time.sleep(1.1)
    self.assertEquals(self.hr.get(self.k(3)), None)
    # Returning previous
    old = self.v()
    self.assertEquals(self.hr.put_if_absent(self.k(4), old), SUCCESS)
    new = self.v(2)
    self.assertEquals(self.hr.put_if_absent(self.k(4), new, 0, 0, True), (NOT_EXECUTED, old))
    self.assertEquals(self.hr.get(self.k()), old)
    # Return previous on a previously non-existing key
    self.assertEquals(self.hr.put_if_absent(self.k(5), new, 0, 0, True), (SUCCESS, None))

  def test_replace(self):
    # If key not present and then when present
    self.assertEquals(self.hr.replace(self.k(), self.v()), NOT_EXECUTED)
    self.assertEquals(self.hr.put_if_absent(self.k(2), self.v()), SUCCESS)
    self.assertEquals(self.hr.replace(self.k(2), self.v(2)), SUCCESS)
    # With lifespan
    self.assertEquals(self.hr.put_if_absent(self.k(3), self.v()), SUCCESS)
    self.assertEquals(self.hr.replace(self.k(3), self.v(2), 1, 0), SUCCESS)
    time.sleep(1.1)
    self.assertEquals(self.hr.get(self.k(3)), None)
    # With maxidle
    self.assertEquals(self.hr.put_if_absent(self.k(4), self.v()), SUCCESS)
    self.assertEquals(self.hr.replace(self.k(4), self.v(2), 0, 1), SUCCESS)
    time.sleep(1.1)
    self.assertEquals(self.hr.get(self.k(4)), None)
    # Returning previous
    old = self.v()
    self.assertEquals(self.hr.put_if_absent(self.k(5), old), SUCCESS)
    new = self.v(2)
    self.assertEquals(self.hr.replace(self.k(5), new, 0, 0, True), (SUCCESS, old))
    self.assertEquals(self.hr.get(self.k(5)), new)
    # Return previous on a previously non-existing key
    self.assertEquals(self.hr.replace(self.k(6), new, 0, 0, True), (NOT_EXECUTED, None))

  def test_get_with_version(self):
    self.assertEquals(self.hr.get_versioned(self.k()), (None, 0))
    self.assertEquals(self.hr.put_if_absent(self.k(), self.v()), SUCCESS)
    (value, version) = self.hr.get_versioned(self.k())
    self.assertEquals(value, self.v())
    self.assertTrue(version > 0)

  # TODO test replace if unmodified, remove, remove if umodified,
  # TODO test contains key, stats, ping, bulk get
  # TODO Test get() calls that return longish values!
  # TODO Test with put that returns previous as well, so that previous is rather long as well

#  asEq = assertEquals

  def _expect_error(self, hr_f, expr, cache_name="UndefinedCache"):
    self.another_hr = HotRodClient('127.0.0.1', 11222, cache_name)
    try:
      hr_f(self.another_hr)
      raise "Operating on an undefined cache should have returned error"
    except HotRodError, e:
      self.assertEquals(e.status, hotrod.SERVER_ERROR)
      self.assertTrue(expr(e.msg))
    finally:
      self.another_hr.stop()

  def k(self, index=-1):
    if index == -1:
      return self._with_method("k-")
    else:
      return self._with_method("k%d-" % index)

  def v(self, index=-1):
    if index == -1:
      return self._with_method("v-")
    else:
      return self._with_method("v%d-" % index)

  def _with_method(self, prefix):
    return prefix + self._testMethodName

if __name__ == '__main__':
  unittest.main()