#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Functional Infinispan's remote cache client test.
"""

__author__ = "Galder Zamarreño"
__copyright__ = "(C) 2010-2011 Red Hat Inc."

import urllib
import zipfile
import sys
import os
import time
import shlex

from subprocess import Popen, PIPE

VERSION = "4.2.1.CR1"
DOWNLOAD = "http://downloads.sourceforge.net/project/infinispan/infinispan"
ZIP = "-bin.zip"

class HotRodServer:

  def start_local(self):
    self.target_dir = "/tmp"
    self._unzip(self.target_dir)
    self.p = self._start("infinispan/test_local_config.xml", self.target_dir)

  def stop(self):
    java_pid = self._find_pid(self.p.pid)
    kill = Popen(["kill", "-2", java_pid.strip()], stdout=PIPE, stderr=PIPE)
    out, err = kill.communicate()
    if err != '':
      self.prettyprint("HotRod server stop failed with: %s" % err, "ERROR")
    elif out != '':
      self.prettyprint("HotRod server stopped with output: %s" % out, "INFO")
    else:
      self.prettyprint("HotRod server stopped successfully", "INFO", True)

  def _find_pid(self, shell_pid):
    ps = Popen(["ps", "-ef"], stdout=PIPE)
    out, err = ps.communicate()
    lines = out.split("\n")
    java_proc = filter(lambda l: l.find(str(shell_pid)) > -1 and
                                 l.find("java") > -1, lines)
    return java_proc[0].split(' ')[3]

  def _start(self, config, target_dir):
    script = "%s/infinispan-%s/bin/startServer.sh" % (target_dir, VERSION)
    os.chmod(script, 0755)
    p = Popen([script, "-r", "hotrod", "-c", config])
    self.prettyprint("HotRod server started", "INFO")
    return p

  def _unzip(self, target_dir):
    fname = "%s/infinispan-%s%s" % (target_dir, VERSION, ZIP)
    try:
      file = open(fname)
    except IOError, e:
      self.prettyprint("%s not found" % fname, "INFO")
      download = "%s/%s/infinispan-%s%s" % (DOWNLOAD, VERSION, VERSION, ZIP)
      self.prettyprint("Downloading %s..." % download, "INFO")
      self.percent = 0
      file, hdrs = urllib.urlretrieve(download, fname, reporthook=self._dlProgress)

    self._extract(file, target_dir)

  def _dlProgress(self, count, blockSize, totalSize):
    percent = int(count * blockSize * 100 / totalSize)
    if self.percent != percent:
      self.percent = percent
      self.prettyprint("%s/infinispan-%s-%s...%d%%"
                       % (self.target_dir, VERSION, ZIP, percent), "DEBUG")

  def _extract(self, file, target_dir):
    if os.path.isdir("%s/infinispan-%s" % (target_dir, VERSION)):
      self.prettyprint("Infinispan distribution already unzipped", "INFO")
      return
    z = zipfile.ZipFile(file)
    # Rudimentary but it works, as opposed to extractall which is buggy:
    # http://bugs.python.org/issue4710
    for n in z.namelist():
      dest = os.path.join(target_dir, n)
      destdir = os.path.dirname(dest)
      if not os.path.isdir(destdir):
        os.makedirs(destdir)
      data = z.read(n)
      if not os.path.isdir(dest):
        f = open(dest, 'w')
        f.write(data)
        f.close()
    z.close()
    self.prettyprint("Infinispan distribution extracted", "INFO")

  def prettyprint(self, message, level, withCR=False):
    if withCR:
      print "\r\n[%s] %s" % (level, message)
    else:
      print "[%s] %s" % (level, message)

if __name__ == '__main__':
  hrs = HotRodServer()
  hrs.start_local()
  print "Sleeping for 5 seconds"
  time.sleep(5)
  hrs.stop()
