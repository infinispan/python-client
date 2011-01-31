#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Functional Infinispan's remote cache client test.

This test suite requires unittest2 module to be installed.

Due to a problem with IntelliJ and unittest2, this test can't currently be run
from the IDE: http://youtrack.jetbrains.net/issue/PY-2809

So, to run the test, go to the root of the project and execute:

$ ./run_test_functional
"""

__author__ = "Galder Zamarreño"
__copyright__ = "(C) 2010-2011 Red Hat Inc."

import unittest2
import time

from remotecache import RemoteCache
from remotecache import ServerError
from infinispan import SERVER_ERROR
from test_hotrod import HotRodServer

# TODO Test reaction to passing None as parameters to put/get methods

class FunctionalTest(unittest2.TestCase):

  @classmethod
  def setUpClass(cls):
    cls._hrs = HotRodServer()
    cls._hrs.start_local()
    time.sleep(5)

  @classmethod
  def tearDownClass(cls):
    cls._hrs.stop()

  def setUp(self):
    self.rc = RemoteCache()

  def tearDown(self):
    self.rc.clear()
    self.rc.stop()

  def test_put(self):
    self.true(self.rc.put(self.k(), self.v()))

  def test_put_with_lifespan(self):
    self.true(self.rc.put(self.k(), self.v(), 1, 0))
    time.sleep(1.1)
    self.eq(self.rc.get(self.k()), None)

  def test_put_with_max_idle(self):
    self.true(self.rc.put(self.k(), self.v(), 0, 1))
    time.sleep(1.1)
    self.eq(self.rc.get(self.k()), None)

  def test_put_with_lifespan_smaller_than_max_idle(self):
    self.true(self.rc.put(self.k(), self.v(), 1, 3))
    time.sleep(1.1)
    self.eq(self.rc.get(self.k()), None)

  def test_put_with_max_idle_smaller_than_lifespan(self):
    self.true(self.rc.put(self.k(), self.v(), 3, 1))
    time.sleep(1.1)
    self.eq(self.rc.get(self.k()), None)

  def test_put_returning_previous(self):
    old = self.v()
    self.true(self.rc.put(self.k(), old))
    new = self.v(2)
    self.eq(self.rc.put(self.k(), new, 0, 0, True), old)
    self.eq(self.rc.get(self.k()), new)
    self.eq(self.rc.put(self.k(2), new, 0, 0, True), None)

  def test_get_not_present(self):
    self.eq(self.rc.get(self.k()), None)

  def test_get(self):
    self.true(self.rc.put(self.k(), self.v()))
    self.eq(self.rc.get(self.k()), self.v())

  def test_clear(self):
    for i in range(10):
      self.true(self.rc.put(self.k(i), self.v(i)))
      self.eq(self.rc.get(self.k(i)), self.v(i))
    self.true(self.rc.clear())
    for i in range(10):
      self.eq(self.rc.get(self.k(i)), None)

  def test_operate_on_named_cache(self):
    cache_name = "AnotherCache"
    self.another_hr = RemoteCache('127.0.0.1', 11222, cache_name)
    try:
      self.eq(self.rc.get(self.k()), None)
      self.eq(self.another_hr.get(self.k()), None)
      # Store only in default cache
      self.true(self.rc.put(self.k(), self.v()))
      self.eq(self.rc.get(self.k()), self.v())
      self.eq(self.another_hr.get(self.k()), None)
      # Clear default and put on another cache
      self.true(self.rc.clear())
      self.true(self.another_hr.put(self.k(), self.v()))
      self.eq(self.another_hr.get(self.k()), self.v())
      self.eq(self.rc.get(self.k()), None)
      # Clear another cache
      self.true(self.another_hr.clear())
      self.eq(self.rc.get(self.k()), None)
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
    self.true(self.rc.put_if_absent(self.k(), self.v()))
    self.false(self.rc.put_if_absent(self.k(), self.v(2)))
    # With lifespan
    self.true(self.rc.put_if_absent(self.k(2), self.v(), 1, 0))
    time.sleep(1.1)
    self.eq(self.rc.get(self.k(2)), None)
    # With maxidle
    self.true(self.rc.put_if_absent(self.k(3), self.v(), 0, 1))
    time.sleep(1.1)
    self.eq(self.rc.get(self.k(3)), None)
    # Returning previous
    old = self.v()
    self.true(self.rc.put_if_absent(self.k(4), old))
    new = self.v(2)
    (result, prev) = self.rc.put_if_absent(self.k(4), new, 0, 0, True)
    self.eq((result, prev), (False, old))
    self.eq(self.rc.get(self.k()), old)
    # Return previous on a previously non-existing key
    self.eq(self.rc.put_if_absent(self.k(5), new, 0, 0, True), (True, None))

  def test_replace(self):
    # If key not present and then when present
    self.false(self.rc.replace(self.k(), self.v()))
    self.true(self.rc.put_if_absent(self.k(2), self.v()))
    self.true(self.rc.replace(self.k(2), self.v(2)))
    # With lifespan
    self.true(self.rc.put_if_absent(self.k(3), self.v()))
    self.true(self.rc.replace(self.k(3), self.v(2), 1, 0))
    time.sleep(1.1)
    self.eq(self.rc.get(self.k(3)), None)
    # With maxidle
    self.true(self.rc.put_if_absent(self.k(4), self.v()))
    self.true(self.rc.replace(self.k(4), self.v(2), 0, 1))
    time.sleep(1.1)
    self.eq(self.rc.get(self.k(4)), None)
    # Returning previous
    old = self.v()
    self.true(self.rc.put_if_absent(self.k(5), old))
    new = self.v(2)
    self.eq(self.rc.replace(self.k(5), new, 0, 0, True), (True, old))
    self.eq(self.rc.get(self.k(5)), new)
    # Return previous on a previously non-existing key
    self.eq(self.rc.replace(self.k(6), new, 0, 0, True), (False, None))

  def test_get_with_version(self):
    self.eq(self.rc.get_versioned(self.k()), (0, None))
    self.true(self.rc.put_if_absent(self.k(), self.v()))
    self.assert_get_versioned()

  def test_replace_if_unmodified(self):
    # First, test the case where the key does not exist
    self.false(self.rc.replace_with_version(self.k(), self.v(), 0))
    # Standard replace if unmodified call
    self.true(self.rc.put_if_absent(self.k(), self.v()))
    version = self.assert_get_versioned()
    self.true(self.rc.replace_with_version(self.k(), self.v(2), version))
    self.eq(self.rc.get(self.k()), self.v(2))
    # Test situation where another operation modified it concurrently
    self.true(self.rc.put_if_absent(self.k(1), self.v()))
    version = self.assert_get_versioned(1)
    self.true(self.rc.replace_with_version(self.k(1), self.v(2), version))
    new_version = self.assert_get_versioned(1, 2)
    self.not_executed(self.rc.replace_with_version(self.k(1), self.v(3), version))
    self.true(self.rc.replace_with_version(self.k(1), self.v(3), new_version))
    # Finally, test that it can return previous value
    (s, p) = self.rc.replace_with_version(self.k(2), self.v(), 0, 0, 0, True)
    self.eq((s, p), (False, None))
    self.true(self.rc.put_if_absent(self.k(2), self.v()))
    version = self.assert_get_versioned(2)
    (s, p) = self.rc.replace_with_version(self.k(2), self.v(2), version, 0, 0, True)
    self.eq((s, p), (True, self.v()))
    new_version = self.assert_get_versioned(2, 2)
    (s, p) = self.rc.replace_with_version(self.k(2), self.v(3), version, 0, 0, True)
    self.eq((s, p), (-1, self.v(2)))
    (s, p) = self.rc.replace_with_version(self.k(2), self.v(3), new_version, 0, 0, True)
    self.eq((s, p), (True, self.v(2)))

  def test_remove(self):
    self.false(self.rc.remove(self.k()))
    self.eq(self.rc.remove(self.k(), True), (False, None))
    self.true(self.rc.put_if_absent(self.k(), self.v()))
    self.true(self.rc.remove(self.k()))
    self.true(self.rc.put_if_absent(self.k(2), self.v()))
    self.eq(self.rc.remove(self.k(2), True), (True, self.v()))

  def test_remove_if_unmodified(self):
    # First, test the case where the key does not exist
    self.false(self.rc.remove_with_version(self.k(), 0))
    # Standard remove if unmodified call
    self.true(self.rc.put_if_absent(self.k(), self.v()))
    version = self.assert_get_versioned()
    self.true(self.rc.remove_with_version(self.k(), version))
    self.eq(self.rc.get(self.k()), None)
    # Test situation where another operation modified it concurrently
    self.true(self.rc.put_if_absent(self.k(1), self.v()))
    version = self.assert_get_versioned(1)
    self.not_executed(self.rc.remove_with_version(self.k(1), 9999))
    self.true(self.rc.remove_with_version(self.k(1), version))
    # Finally, test that it can return previous value
    (s, p) = self.rc.remove_with_version(self.k(2), 0, True)
    self.eq((s, p), (False, None))
    self.true(self.rc.put_if_absent(self.k(2), self.v()))
    version = self.assert_get_versioned(2)
    (s, p) = self.rc.remove_with_version(self.k(2), 7890, True)
    self.eq((s, p), (-1, self.v()))
    (s, p) = self.rc.remove_with_version(self.k(2), version, True)
    self.eq((s, p), (True, self.v()))

  def test_contains_key(self):
    self.false(self.rc.contains_key(self.k()))
    self.true(self.rc.put_if_absent(self.k(), self.v()))
    self.true(self.rc.contains_key(self.k()))

  def test_stats(self):
    stats = self.rc.stats()
    self.eq(len(stats), 9)
    self.eq(stats['timeSinceStart'], '-1')
    self.eq(stats['currentNumberOfEntries'], '-1')
    self.eq(stats['totalNumberOfEntries'], '-1')
    self.eq(stats['stores'], '-1')
    self.eq(stats['retrievals'], '-1')
    self.eq(stats['hits'], '-1')
    self.eq(stats['misses'], '-1')
    self.eq(stats['removeHits'], '-1')
    self.eq(stats['removeMisses'], '-1')

  def test_ping(self):
    self.true(self.rc.ping())

  def test_bulk_get(self):
    for i in range(10):
      self.true(self.rc.put(self.k(i), self.v(i)))
      self.eq(self.rc.get(self.k(i)), self.v(i))
    bulk_data = self.rc.bulk_get()
    self.eq(len(bulk_data), 10)
    for i in range(10):
      self.eq(bulk_data[self.k(i)], self.v(i))

    bulk_data = self.rc.bulk_get(5)
    self.eq(len(bulk_data), 5)
    for i in range(5):
      if self.k(i) in bulk_data:
        self.eq(bulk_data[self.k(i)], self.v(i))

  def _expect_error(self, hr_f, expr, cache_name="UndefinedCache"):
    self.another_hr = RemoteCache('127.0.0.1', 11222, cache_name)
    try:
      hr_f(self.another_hr)
      raise "Operating on an undefined cache should have returned error"
    except ServerError, e:
      self.eq(e.status, SERVER_ERROR)
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

  def true(self, expr, msg=None):
    self.assertTrue(expr, msg)

  def false(self, expr, msg=None):
    self.assertFalse(expr, msg)

  def not_executed(self, first, msg=None):
    self.assertEquals(-1, first, msg)

  def assert_get_versioned(self, k_index=-1, v_index=-1):
    if k_index > 0:
      (version, value) = self.rc.get_versioned(self.k(k_index))
    else:
      (version, value) = self.rc.get_versioned(self.k())

    if v_index > 0:
      self.eq(value, self.v(v_index))
    else:
      self.eq(value, self.v())

    self.assertTrue(version > 0)
    return version

  def _with_method(self, prefix):
    return prefix + self._testMethodName

if __name__ == '__main__':
  unittest2.main()