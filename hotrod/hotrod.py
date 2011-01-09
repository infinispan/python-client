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
PUT_IF_ABSENT_REQ = 0x05
PUT_IF_ABSENT_RES = 0x06
REPLACE_REQ = 0x07
REPLACE_RES = 0x08
REPLACE_IF_REQ = 0x09
REPLACE_IF_RES = 0x0A
REMOVE_REQ = 0x0B
REMOVE_RES = 0x0C
REMOVE_IF_REQ = 0x0D
REMOVE_IF_RES = 0x0E
CONTAINS_REQ = 0x0F
CONTAINS_RES = 0x10
GET_WITH_VERSION_REQ = 0x11
GET_WITH_VERSION_RES = 0x12
CLEAR_REQ = 0x13
CLEAR_RES = 0x14
ERROR_RES = 0x50

SUCCESS = 0x00
NOT_EXECUTED = 0x01
KEY_DOES_NOT_EXIST = 0x02
OK_STATUS = [SUCCESS, NOT_EXECUTED, KEY_DOES_NOT_EXIST]

KEY_LESS_SEND = lambda s, m, k, v, l, i, ver: \
  s.send(m)
KEY_ONLY_SEND = lambda s, m, k, v, l, i, ver: \
  s.send(m + to_varint(len(k)) + k)
KEY_VALUE_SEND = lambda s, m, k, v, l, i, ver: \
  s.send(
    m + to_varint(len(k)) + k +
    to_varint(l) + to_varint(i) +
    to_varint(len(v)) + v
  )
REPLACE_IF_REQ_SEND = lambda s, m, k, v, l, i, ver: \
  s.send(
    m + to_varint(len(k)) + k +
    to_varint(l) + to_varint(i) +
    struct.pack(VERSION_FMT, ver) +
    to_varint(len(v)) + v
  )

REMOVE_IF_REQ_SEND = lambda s, m, k, v, l, i, ver: \
  s.send(
    m + to_varint(len(k)) + k +
    struct.pack(VERSION_FMT, ver)
  )

SEND = {
  CLEAR_REQ            : KEY_LESS_SEND,
  GET_REQ              : KEY_ONLY_SEND,
  GET_WITH_VERSION_REQ : KEY_ONLY_SEND,
  PUT_REQ              : KEY_VALUE_SEND,
  PUT_IF_ABSENT_REQ    : KEY_VALUE_SEND,
  REPLACE_REQ          : KEY_VALUE_SEND,
  REPLACE_IF_REQ       : REPLACE_IF_REQ_SEND,
  REMOVE_REQ           : KEY_ONLY_SEND,
  REMOVE_IF_REQ        : REMOVE_IF_REQ_SEND,
  CONTAINS_REQ         : KEY_ONLY_SEND
}

KEY_LESS_RECV = lambda hr, st, ret_prev: st

KEY_ONLY_RECV = lambda hr, st, ret_prev: \
  None if st == KEY_DOES_NOT_EXIST else hr._read_ranged_bytes()

GET_WITH_VERSION_RECV = lambda hr, st, ret_prev: \
  (0, None) if st == KEY_DOES_NOT_EXIST else \
  (struct.unpack(VERSION_FMT, hr._read_bytes(VERSION_LEN))[0],
   hr._read_ranged_bytes())

KEY_VALUE_RECV = lambda hr, st, ret_prev: \
  st if st in OK_STATUS and not ret_prev else (st, hr._read_ranged_bytes())

ERROR_RECV = lambda hr, st, ret_prev: hr._raise_error(st)

RECV = {
  CLEAR_RES            : KEY_LESS_RECV,
  GET_RES              : KEY_ONLY_RECV,
  GET_WITH_VERSION_RES : GET_WITH_VERSION_RECV,
  PUT_RES              : KEY_VALUE_RECV,
  PUT_IF_ABSENT_RES    : KEY_VALUE_RECV,
  REPLACE_RES          : KEY_VALUE_RECV,
  REPLACE_IF_RES       : KEY_VALUE_RECV,
  ERROR_RES            : ERROR_RECV,
  REMOVE_RES           : KEY_VALUE_RECV,
  REMOVE_IF_RES        : KEY_VALUE_RECV,
  CONTAINS_RES         : KEY_LESS_RECV
}

INVALID_MAGIC_MSG_ID = 0x81
UNKNOWN_CMD = 0x82
UNKNOWN_VERSION = 0x83
PARSE_ERROR = 0x84
SERVER_ERROR = 0x85
CMD_TIMED_OUT = 0x86

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

VERSION_FMT = ">Q"
VERSION_LEN = struct.calcsize(VERSION_FMT)

# TODO Add methods to encode/decode varlong and check the spec to see where they need applying
# TODO implement client intelligence = 2 (cluster formation interest)
# TODO implement client intelligence = 3 (hash distribution interest)

class HotRodClient(object):
  def __init__(self, host='127.0.0.1', port=11222, cache_name=''):
    self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.s.connect_ex((host, port))
    self.cache_name = cache_name

  def stop(self):
    self.s.close()

  def put(self, key, val, lifespan=0, max_idle=0, ret_prev=False):
    """ Associates the specified value with the specified key in the
    remote cache. Optionally, it takes two parameters that control expiration
    this cache entry: lifespan indicates the number of seconds the cache entry
    should live in memory, and max idle time indicates the number of seconds
    since last time the cache entry entry has been touched after which the
    cache entry is considered up for expiration. If you pass 0 as parameter
    for lifespan, it means that the entry has no lifespan and can live forever.
    Same thing happens for max idle parameter.

    Unless returning previous value has been enabled, this operation returns
    the result of the operation as a byte. The possible values are specified
    in the Hot Rod protocol. When return previous has been enabled, this
    operation returns a tuple containing the result of the operation and the
    previous value if exists. If the key was not associated with any previous
    value, it will return None in the second parameter of the tuple. """
    return self._do_op(PUT_REQ,
                       key, val, lifespan, max_idle, ret_prev)

  # TODO: Shall we return just true/false? Ask list about 2/3 diff value returns
  def contains_key(self, key):
    """ Returns a byte indicating whether the key is present in the remote
    cache. If it returns 0, it means that they was present and if returns 2,
    it means that it does not exist in the remote cache. """
    return self._do_op(CONTAINS_REQ, key, '', 0, 0, False)

  def get(self, key):
    """ Returns the value associated with the given key in the remote cache.
    If the key is not present, this operation returns None. """
    return self._do_op(GET_REQ, key, '', 0, 0, False)

  def put_if_absent(self, key, val, lifespan=0, max_idle=0, ret_prev=False):
    return self._do_op(PUT_IF_ABSENT_REQ,
                       key, val, lifespan, max_idle, ret_prev)

  def replace(self, key, val, lifespan=0, max_idle=0, ret_prev=False):
    return self._do_op(REPLACE_REQ, key, val, lifespan, max_idle, ret_prev)

  def get_versioned(self, key):
    """ Returns the version associated with this key in the remote cache and
    the value associated with the given key and . The return is actually a
    tuple where the version is the first element and value is the second. If
    the key is not found, this method returns (0, None). """
    return self._do_op(GET_WITH_VERSION_REQ, key, '', 0, 0, False)

  def replace_with_version(self, key, val, version, lifespan=0, max_idle=0,
                           ret_prev=False):
    """ Replaces the value associated with a key with the value passed as
    parameter if, and only if, the version of the cache entry matches the
    version passed. This type of operation is generally used to guarantee that
    when the cache entry is to be replaced, nobody has changed the contents
    of the cache entry since last time it was read. Normally, the version that
    is passed comes from the output of calling get_versioned() operation.

    As with other similar operations, optional lifespan, max_idle parameters
    can be provided to control the lifetime of the cache entry, and it can
    return the previous value associated with the cache entry is ret_prev is
    passed as True. """
    return self._do_op(REPLACE_IF_REQ,
                       key, val, lifespan, max_idle, ret_prev, version)

  def remove(self, key, ret_prev=False):
    """ Remove the key and the value associated to it from the remote cache.
    Unless returning previous value has been enabled, this operation returns
    the result of the operation as a byte. The possible values are specified
    in the Hot Rod protocol. When return previous has been enabled, this
    operation returns a tuple containing the result of the operation and the
    previous value if exists. If the key was not associated with any previous
    value, it will return None in the second parameter of the tuple. """
    return self._do_op(REMOVE_REQ, key, '', 0, 0, ret_prev)

  def remove_with_version(self, key, version, ret_prev=False):
    """ Removes the key and its associated value for the key passed as
    parameter if, and only if, the version of the cache entry matches the
    version passed. This type of operation is generally used to guarantee that
    when the cache entry is to be removed, nobody has changed the contents
    of the cache entry since last time it was read. Normally, the version that
    is passed comes from the output of calling get_versioned() operation.

    This function can return the previous value associated with the cache
    entry is ret_prev is passed as True."""
    return self._do_op(REMOVE_IF_REQ, key, '', 0, 0, ret_prev, version)

  def clear(self):
    return self._do_op(CLEAR_REQ, '', '', 0, 0, False)

  def _do_op(self, op, key, val, lifespan, max_idle, ret_prev, version=-1):
    self._send_op(op, key, val, lifespan, max_idle, ret_prev, version)
    return self._get_resp(ret_prev)

  def _send_op(self, op, key, val, lifespan, max_idle, ret_prev, version):
    if ret_prev:
      flag = 0x01
    else:
      flag = 0

      # TODO: Make message id counter variable and atomic(?)
    if self.cache_name == '':
      msg = struct.pack(REQ_FMT, REQ_MAGIC, 0x01, VERSION_10, op,
                        0, flag, 0x01, 0, 0)
    else:
      start = struct.pack(REQ_START_FMT, REQ_MAGIC, 0x01, VERSION_10, op)
      end = struct.pack(REQ_END_FMT, flag, 0x01, 0, 0)
      msg = start + to_varint(len(self.cache_name)) + self.cache_name + end

    SEND[op](self.s, msg, key, val, lifespan, max_idle, version)

  def _get_resp(self, ret_prev):
    header = self._read_bytes(RES_H_LEN)
    magic, msg_id, op, st, topo_mark = struct.unpack(RES_H_FMT, header)
    assert (magic == RES_MAGIC), "Got magic: %d" % magic
    return RECV[op](self, st, ret_prev)

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
    if bytes == '':
      return None
    else:
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

class DecodeError(Exception): pass