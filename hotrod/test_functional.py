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
from hotrod import SUCCESS, NOT_EXECUTED, KEY_DOES_NOT_EXIST

class FunctionalTest(unittest.TestCase):
  def setUp(self):
    self.hr = HotRodClient()

  def tearDown(self):
    self.hr.clear()
    self.hr.stop()

  def test_put(self):
    self.eq(self.hr.put(self.k(), self.v()), SUCCESS)

  def test_put_with_lifespan(self):
    self.eq(self.hr.put(self.k(), self.v(), 1, 0), SUCCESS)
    time.sleep(1.1)
    self.eq(self.hr.get(self.k()), None)

  def test_put_with_max_idle(self):
    self.eq(self.hr.put(self.k(), self.v(), 0, 1), SUCCESS)
    time.sleep(1.1)
    self.eq(self.hr.get(self.k()), None)

  def test_put_with_lifespan_smaller_than_max_idle(self):
    self.eq(self.hr.put(self.k(), self.v(), 1, 3), SUCCESS)
    time.sleep(1.1)
    self.eq(self.hr.get(self.k()), None)

  def test_put_with_max_idle_smaller_than_lifespan(self):
    self.eq(self.hr.put(self.k(), self.v(), 3, 1), SUCCESS)
    time.sleep(1.1)
    self.eq(self.hr.get(self.k()), None)

  def test_put_returning_previous(self):
    old = self.v()
    self.eq(self.hr.put(self.k(), old), SUCCESS)
    new = self.v(2)
    self.eq(self.hr.put(self.k(), new, 0, 0, True), (SUCCESS, old))
    self.eq(self.hr.get(self.k()), new)
    self.eq(self.hr.put(self.k(2), new, 0, 0, True), (SUCCESS, None))

  def test_get_not_present(self):
    self.eq(self.hr.get(self.k()), None)

  def test_get(self):
    self.eq(self.hr.put(self.k(), self.v()), SUCCESS)
    self.eq(self.hr.get(self.k()), self.v())

  def test_clear(self):
    for i in range(10):
      self.eq(self.hr.put(self.k(i), self.v(i)), SUCCESS)
      self.eq(self.hr.get(self.k(i)), self.v(i))
    self.eq(self.hr.clear(), SUCCESS)
    for i in range(10):
      self.eq(self.hr.get(self.k(i)), None)

  def test_operate_on_named_cache(self):
    cache_name = "AnotherCache"
    self.another_hr = HotRodClient('127.0.0.1', 11222, cache_name)
    try:
      self.eq(self.hr.get(self.k()), None)
      self.eq(self.another_hr.get(self.k()), None)
      # Store only in default cache
      self.eq(self.hr.put(self.k(), self.v()), SUCCESS)
      self.eq(self.hr.get(self.k()), self.v())
      self.eq(self.another_hr.get(self.k()), None)
      # Clear default and put on another cache
      self.eq(self.hr.clear(), SUCCESS)
      self.eq(self.another_hr.put(self.k(), self.v()), SUCCESS)
      self.eq(self.another_hr.get(self.k()), self.v())
      self.eq(self.hr.get(self.k()), None)
      # Clear another cache
      self.eq(self.another_hr.clear(), SUCCESS)
      self.eq(self.hr.get(self.k()), None)
      self.eq(self.another_hr.get(self.k()), None)
    finally:
      self.another_hr.clear()
      self.another_hr.stop()

  def test_operate_on_undefined_cache(self):
    expr = lambda msg: "CacheNotFoundException" in msg
    self._expect_error(lambda hr: hr.get(self.k()), expr)
    self._expect_error(lambda hr: hr.put(self.k(), self.v()), expr)
    self._expect_error(lambda hr: hr.clear(), expr)

  def test_operate_on_topology_cache(self):
    expr = lambda m: "Remote requests are not allowed to topology cache." in m
    cache_name = "___hotRodTopologyCache"
    self._expect_error(lambda hr: hr.get(self.k()), expr, cache_name)
    self._expect_error(lambda hr: hr.put(self.k(), self.v()), expr, cache_name)
    self._expect_error(lambda hr: hr.clear(), expr, cache_name)

  def test_put_if_absent(self):
    # If key not present and then when present
    self.eq(self.hr.put_if_absent(self.k(), self.v()), SUCCESS)
    self.eq(self.hr.put_if_absent(self.k(), self.v(2)), NOT_EXECUTED)
    # With lifespan
    self.eq(self.hr.put_if_absent(self.k(2), self.v(), 1, 0), SUCCESS)
    time.sleep(1.1)
    self.eq(self.hr.get(self.k(2)), None)
    # With maxidle
    self.eq(self.hr.put_if_absent(self.k(3), self.v(), 0, 1), SUCCESS)
    time.sleep(1.1)
    self.eq(self.hr.get(self.k(3)), None)
    # Returning previous
    old = self.v()
    self.eq(self.hr.put_if_absent(self.k(4), old), SUCCESS)
    new = self.v(2)
    (status, prev) = self.hr.put_if_absent(self.k(4), new, 0, 0, True)
    self.eq((status, prev), (NOT_EXECUTED, old))
    self.eq(self.hr.get(self.k()), old)
    # Return previous on a previously non-existing key
    self.eq(self.hr.put_if_absent(self.k(5), new, 0, 0, True), (SUCCESS, None))

  def test_replace(self):
    # If key not present and then when present
    self.eq(self.hr.replace(self.k(), self.v()), NOT_EXECUTED)
    self.eq(self.hr.put_if_absent(self.k(2), self.v()), SUCCESS)
    self.eq(self.hr.replace(self.k(2), self.v(2)), SUCCESS)
    # With lifespan
    self.eq(self.hr.put_if_absent(self.k(3), self.v()), SUCCESS)
    self.eq(self.hr.replace(self.k(3), self.v(2), 1, 0), SUCCESS)
    time.sleep(1.1)
    self.eq(self.hr.get(self.k(3)), None)
    # With maxidle
    self.eq(self.hr.put_if_absent(self.k(4), self.v()), SUCCESS)
    self.eq(self.hr.replace(self.k(4), self.v(2), 0, 1), SUCCESS)
    time.sleep(1.1)
    self.eq(self.hr.get(self.k(4)), None)
    # Returning previous
    old = self.v()
    self.eq(self.hr.put_if_absent(self.k(5), old), SUCCESS)
    new = self.v(2)
    self.eq(self.hr.replace(self.k(5), new, 0, 0, True), (SUCCESS, old))
    self.eq(self.hr.get(self.k(5)), new)
    # Return previous on a previously non-existing key
    self.eq(self.hr.replace(self.k(6), new, 0, 0, True), (NOT_EXECUTED, None))

  def test_get_with_version(self):
    self.eq(self.hr.get_versioned(self.k()), (0, None))
    self.eq(self.hr.put_if_absent(self.k(), self.v()), SUCCESS)
    self.assert_get_versioned()

  def test_replace_if_unmodified(self):
    # First, test the case where the key does not exist
    status = self.hr.replace_with_version(self.k(), self.v(), 0)
    self.eq(status, KEY_DOES_NOT_EXIST)
    # Standard replace if unmodified call
    self.eq(self.hr.put_if_absent(self.k(), self.v()), SUCCESS)
    version = self.assert_get_versioned()
    status = self.hr.replace_with_version(self.k(), self.v(2), version)
    self.eq(status, SUCCESS)
    self.eq(self.hr.get(self.k()), self.v(2))
    # Test situation where another operation modified it concurrently
    self.eq(self.hr.put_if_absent(self.k(1), self.v()), SUCCESS)
    version = self.assert_get_versioned(1)
    status = self.hr.replace_with_version(self.k(1), self.v(2), version)
    self.eq(status, SUCCESS)
    new_version = self.assert_get_versioned(1, 2)
    status = self.hr.replace_with_version(self.k(1), self.v(3), version)
    self.eq(status, NOT_EXECUTED)
    status = self.hr.replace_with_version(self.k(1), self.v(3), new_version)
    self.eq(status, SUCCESS)
    # Finally, test that it can return previous value
    (s, p) = self.hr.replace_with_version(self.k(2), self.v(), 0, 0, 0, True)
    self.eq((s, p), (KEY_DOES_NOT_EXIST, None))
    self.eq(self.hr.put_if_absent(self.k(2), self.v()), SUCCESS)
    version = self.assert_get_versioned(2)
    (s, p) = self.hr.replace_with_version(self.k(2), self.v(2), version, 0, 0, True)
    self.eq((s, p), (SUCCESS, self.v()))
    new_version = self.assert_get_versioned(2, 2)
    (s, p) = self.hr.replace_with_version(self.k(2), self.v(3), version, 0, 0, True)
    self.eq((s, p), (NOT_EXECUTED, self.v(2)))
    (s, p) = self.hr.replace_with_version(self.k(2), self.v(3), new_version, 0, 0, True)
    self.eq((s, p), (SUCCESS, self.v(2)))

  def test_remove(self):
    self.eq(self.hr.remove(self.k()), KEY_DOES_NOT_EXIST)
    self.eq(self.hr.remove(self.k(), True), (KEY_DOES_NOT_EXIST, None))
    self.eq(self.hr.put_if_absent(self.k(), self.v()), SUCCESS)
    self.eq(self.hr.remove(self.k()), SUCCESS)
    self.eq(self.hr.put_if_absent(self.k(2), self.v()), SUCCESS)
    self.eq(self.hr.remove(self.k(2), True), (SUCCESS, self.v()))

  def test_remove_if_unmodified(self):
    # First, test the case where the key does not exist
    status = self.hr.remove_with_version(self.k(), 0)
    self.eq(status, KEY_DOES_NOT_EXIST)
    # Standard remove if unmodified call
    self.eq(self.hr.put_if_absent(self.k(), self.v()), SUCCESS)
    version = self.assert_get_versioned()
    status = self.hr.remove_with_version(self.k(), version)
    self.eq(status, SUCCESS)
    self.eq(self.hr.get(self.k()), None)
    # Test situation where another operation modified it concurrently
    self.eq(self.hr.put_if_absent(self.k(1), self.v()), SUCCESS)
    version = self.assert_get_versioned(1)
    status = self.hr.remove_with_version(self.k(1), 9999)
    self.eq(status, NOT_EXECUTED)
    status = self.hr.remove_with_version(self.k(1), version)
    self.eq(status, SUCCESS)
    # Finally, test that it can return previous value
    (s, p) = self.hr.remove_with_version(self.k(2), 0, True)
    self.eq((s, p), (KEY_DOES_NOT_EXIST, None))
    self.eq(self.hr.put_if_absent(self.k(2), self.v()), SUCCESS)
    version = self.assert_get_versioned(2)
    (s, p) = self.hr.remove_with_version(self.k(2), 7890, True)
    self.eq((s, p), (NOT_EXECUTED, self.v()))
    (s, p) = self.hr.remove_with_version(self.k(2), version, True)
    self.eq((s, p), (SUCCESS, self.v()))

  def test_contains_key(self):
    self.eq(self.hr.contains_key(self.k()), KEY_DOES_NOT_EXIST)
    self.eq(self.hr.put_if_absent(self.k(), self.v()), SUCCESS)
    self.eq(self.hr.contains_key(self.k()), SUCCESS)

  # TODO test stats, ping, bulk get
  # TODO Test get() calls that return longish values!
  # TODO Test with put that returns previous as well, so that previous is rather long as well
  # TODO test reaction to passing None as parameters to put/get methods

  def _expect_error(self, hr_f, expr, cache_name="UndefinedCache"):
    self.another_hr = HotRodClient('127.0.0.1', 11222, cache_name)
    try:
      hr_f(self.another_hr)
      raise "Operating on an undefined cache should have returned error"
    except HotRodError, e:
      self.eq(e.status, hotrod.SERVER_ERROR)
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

  def eq(self, first, second, msg=None):
    self.assertEquals(first, second, msg)

  def assert_get_versioned(self, k_index=-1, v_index=-1):
    if k_index > 0:
      (version, value) = self.hr.get_versioned(self.k(k_index))
    else:
      (version, value) = self.hr.get_versioned(self.k())

    if v_index > 0:
      self.eq(value, self.v(v_index))
    else:
      self.eq(value, self.v())

    self.assertTrue(version > 0)
    return version

  def _with_method(self, prefix):
    return prefix + self._testMethodName

if __name__ == '__main__':
  unittest.main()