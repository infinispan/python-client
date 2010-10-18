#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binary Hot Rod client.

Copyright (c) 2010  Galder Zamarreño
"""

import socket
import struct

from constants import *
import constants
import encoder
import decoder
import exceptions

class HotRodClient(object):
   def __init__(self, host='127.0.0.1', port=11311):
      self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.s.connect_ex((host, port))

   def stop(self):
      self.s.close()

   def put(self, key, val):
      return self._mutate(constants.PUT_REQ, key, val)

   def get(self, key):
      return self._doKeyCmd(constants.GET_REQ, key)

   def _mutate(self, cmd, key, val):
      return self._doKeyValCmd(cmd, key, val)

   def _doKeyValCmd(self, cmd, key, val):
      """Send a command and await its response."""
      self._sendKeyValCmd(cmd, key, val)
      return self._handleEmptyResponse()

   def _doKeyCmd(self, cmd, key):
      """Send a command and await its response."""
      self._sendKeyCmd(cmd, key)
      return self._handleResponse()

   def _handleEmptyResponse(self):
      response = ""
      while len(response) < RES_HEADER_LEN:
         data = self.s.recv(RES_HEADER_LEN - len(response))
         if data == '':
            raise exceptions.EOFError("Got empty data (remote died?).")
         response += data
      assert len(response) == RES_HEADER_LEN
      magic, msgid, opcode, status, topomark = struct.unpack(RES_HEADER_FMT, response)

      assert (magic == RES_MAGIC), "Got magic: %d" % magic
      if status != 0:
         raise HotRodError(errcode, rv)
      return status

   def _handleResponse(self):
      response = ""
      while len(response) < GET_HEADER_LEN:
         data = self.s.recv(GET_HEADER_LEN - len(response))
         if data == '':
            raise exceptions.EOFError("Got empty data (remote died?).")
         response += data
      assert len(response) == GET_HEADER_LEN
      magic, msgid, opcode, status, topomar, remaining = struct.unpack(GET_HEADER_FMT, response)

      local_DecodeVarint = decoder._DecodeVarint32

      (length, pos) = local_DecodeVarint(remaining, 0)

      rv = ""
      while length > 0:
         data = self.s.recv(length)
         if data == '':
            raise exceptions.EOFError("Got empty data (remote died?).")
         rv += data
         length -= len(data)

      assert (magic == RES_MAGIC), "Got magic: %d" % magic
      if status != 0:
         raise HotRodError(errcode, rv)
      return rv

   def _sendKeyCmd(self, cmd, key):
      msg = struct.pack(REQ_FMT, REQ_MAGIC, 0x01, VERSION_10,
                        cmd, 0, 0, 0x01, 0, 0)
      self.s.send(msg + encoder._VarintBytes(len(key)) + key)

   def _sendKeyValCmd(self, cmd, key, val):
      msg = struct.pack(REQ_FMT, REQ_MAGIC, 0x01, VERSION_10,
                        cmd, 0, 0, 0x01, 0, 0)
      self.s.send(msg + encoder._VarintBytes(len(key)) + key +
                  "0" + "0" + encoder._VarintBytes(len(val)) + val)

class HotRodError(exceptions.Exception):
   """Error raised when a command fails."""

   def __init__(self, status, msg):
      supermsg = 'Hot Rod error #' + `status`
      if msg: supermsg += ":  " + msg
      exceptions.Exception.__init__(self, supermsg)

      self.status = status
      self.msg = msg

   def __repr__(self):
      return "<Hot Rod error #%d ``%s''>" % (self.status, self.msg)
