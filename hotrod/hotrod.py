#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binary Hot Rod client.

Copyright (c) 2010  Galder Zamarreño
"""

import socket
import struct

import exceptions

REQ_MAGIC = 0xA0
RES_MAGIC = 0xA1

VERSION_10 = 10
PUT_REQ = 0x01
PUT_RES = 0x02
GET_REQ = 0x03
GET_RES = 0x04
CLEAR_REQ = 0x13
CLEAR_RES = 0x14

SERVER_ERROR = 0x85

# Without cache name
# magic, msg_id, version, op_code,
# cache name length (0),
# flag, clientint, topoid, txtypeid
REQ_FMT = ">BBBBBBBBB"

# With cache name, separate the start and end of header
# start: magic, msg_id, version, op_code,
REQ_START_FMT = ">BBBB"
# end: flag, clientint, topoid, txtypeid
REQ_END_FMT = ">BBBB"
# And in between them: cache_name_length + cache name

# magic, msg_id, op_code, status, topology_mark
RES_H_FMT = ">BBBBB"
RES_H_LEN = struct.calcsize(RES_H_FMT)

# TODO: Find a way to, given the fmt, calculate the len and viceversa without relying on constants for both

class HotRodClient(object):
  def __init__(self, host='127.0.0.1', port=11222, cache_name=''):
    self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.s.connect_ex((host, port))
    self.cache_name = cache_name

  def stop(self):
    self.s.close()

  def put(self, key, val, lifespan=0, max_idle=0, ret_prev=False):
    return self._do_cmd(PUT_REQ, key, val, lifespan, max_idle, ret_prev)

  def get(self, key):
    return self._do_cmd(GET_REQ, key, '', 0, 0, False)

  def clear(self):
    return self._do_cmd(CLEAR_REQ, '', '', 0, 0, False)

  def _do_cmd(self, cmd, key, val, lifespan, max_idle, ret_prev):
    self._send_cmd(cmd, key, val, lifespan, max_idle, ret_prev)
    return self._get_resp(key, val, ret_prev)

  def _send_cmd(self, cmd, key, val, lifespan, max_idle, ret_prev):
    if ret_prev:
      flag = 0x01
    else:
      flag = 0

      # TODO: Make message id counter variable and atomic(?)
    if self.cache_name == '':
      msg = struct.pack(REQ_FMT, REQ_MAGIC, 0x01, VERSION_10, cmd,
                        0, flag, 0x01, 0, 0)
    else:
      start = struct.pack(REQ_START_FMT, REQ_MAGIC, 0x01, VERSION_10, cmd)
      end = struct.pack(REQ_END_FMT, flag, 0x01, 0, 0)
      msg = start + to_varint(len(self.cache_name)) + self.cache_name + end

    if key == '':
      self.s.send(msg) # i.e. clear
    else:
      if val == '': # i.e. get, contains_key...
        self.s.send(msg + to_varint(len(key)) + key)
      else:
        self.s.send(msg + to_varint(len(key)) + key +
                    to_varint(lifespan) + to_varint(max_idle) +
                    to_varint(len(val)) + val) # i.e. put

  def _get_resp(self, key, val, ret_prev):
    header = self._read_bytes(RES_H_LEN)
    magic, msg_id, op_code, st, topo_mark = struct.unpack(RES_H_FMT, header)
    assert (magic == RES_MAGIC), "Got magic: %d" % magic

    if key == '':
      if not st:
        return
      else:
        self._raise_error(st)
    else:
      if val == '':
        return self._get_retrieval_resp(st)
      else:
        return self._get_store_resp(st, ret_prev)

  def _get_retrieval_resp(self, status):
    if status == 2:
      return None
    else:
      if not status:
        return self._read_ranged_bytes()
      else:
        self._raise_error(status)

  def _get_store_resp(self, status, ret_prev):
    if not status:
      if ret_prev:
        return self._read_ranged_bytes()
      else:
         return status
    else:
        self._raise_error(status)

  def _read_ranged_bytes(self):
    return self._read_bytes(from_varint(self.s))

  def _read_bytes(self, expected_len):
    bytes = ""
    bytes_len = expected_len
    while len(bytes) < bytes_len:
      tmp = self.s.recv(bytes_len - len(bytes))
      if tmp == '':
        raise exceptions.EOFError("Got empty data (remote died?).")
      bytes += tmp
    assert len(bytes) == bytes_len
    return bytes

  def _raise_error(self, status):
    error = self._read_ranged_bytes()
    raise HotRodError(status, error)

class HotRodError(exceptions.Exception):
  """Error raised when a command fails."""

  def __init__(self, status, msg):
    super_msg = 'Hot Rod error #' + `status`
    if msg: super_msg += ":  " + msg
    exceptions.Exception.__init__(self, super_msg)

    self.status = status
    self.msg = msg

  def __repr__(self):
    return "<Hot Rod error #%d ``%s''>" % (self.status, self.msg)

def to_varint(value):
  """Encode the given integer as a varint and return the bytes.
  This is only called at startup time so it doesn't need to be fast."""
  pieces = []
  _encode_varint(pieces.append, value)
  return "".join(pieces)

def _encode_varint(write, value):
  bits = value & 0x7f
  value >>= 7
  while value:
    write(chr(0x80|bits))
    bits = value & 0x7f
    value >>= 7
  return write(chr(bits))

def from_varint(socket):
  """ Decode a varint from the socket reading one byte at the time """
  return _decode_varint((1 << 32) - 1, socket)

def _decode_varint(mask, socket):
  result = 0
  shift = 0
  while 1:
    b = ord(socket.recv(1))
    result |= ((b & 0x7f) << shift)
    if not (b & 0x80):
      result &= mask
      return result
    shift += 7
    if shift >= 64:
      raise DecodeError("Too many bytes when decoding varint.")

# TODO Add methods to encode/decode varlong and check the spec to see where they need applying

class DecodeError(Exception): pass