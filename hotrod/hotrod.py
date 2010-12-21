#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binary Hot Rod client.

Copyright (c) 2010  Galder Zamarreño
"""

import socket
import struct

import encoder
import decoder
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
GET_RES_FMT = ">s"
GET_RES_LEN = struct.calcsize(GET_RES_FMT)
PUT_WITH_PREV_FMT = ">s"
PUT_WITH_PREV_LEN = struct.calcsize(PUT_WITH_PREV_FMT)
ERROR_FMT = ">s"
ERROR_LEN = struct.calcsize(ERROR_FMT)

# TODO: Find a way to consolidate GET_RES, PUT_WITH_PREV and ERROR
# TODO: Find a way to, given the fmt, calculate the len and viceversa without relying on constants for both

class HotRodClient(object):
  def __init__(self, host='127.0.0.1', port=11222, c_name=''):
    self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.s.connect_ex((host, port))
    self.c_name = c_name

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
    if (ret_prev):
      flag = 0x01
    else:
      flag = 0

      # TODO: Make message id counter variable and atomic(?)
    if (self.c_name == ''):
      msg = struct.pack(REQ_FMT, REQ_MAGIC, 0x01, VERSION_10, cmd,
                        0, flag, 0x01, 0, 0)
    else:
      start = struct.pack(REQ_START_FMT, REQ_MAGIC, 0x01, VERSION_10, cmd)
      end = struct.pack(REQ_END_FMT, flag, 0x01, 0, 0)
      msg = start + encoder._VarintBytes(len(self.c_name)) + self.c_name + end

    if key == '':
      self.s.send(msg) # i.e. clear
    else:
      if val == '': # i.e. get, contains_key...
        self.s.send(msg + encoder._VarintBytes(len(key)) + key)
      else:
        self.s.send(msg + encoder._VarintBytes(len(key)) + key +
                    encoder._VarintBytes(lifespan) + encoder._VarintBytes(max_idle) +
                    encoder._VarintBytes(len(val)) + val) # i.e. put

  def _get_resp(self, key, val, ret_prev):
    header = self._read_data(RES_H_LEN)
    magic, msg_id, op_code, st, topo_mark = struct.unpack(RES_H_FMT, header)
    assert (magic == RES_MAGIC), "Got magic: %d" % magic

    if (key == ''):
      if st == 0:
        return
      else:
        self._raise_error(status) # TODO test
    else:
      if val == '':
        return self._get_retrieval_resp(st)
      else:
        return self._get_store_resp(st, ret_prev)

  def _get_retrieval_resp(self, status):
    if status == 2:
      return None
    else:
      if status == 0:
        return self._read_value(GET_RES_LEN, GET_RES_FMT)
      else:
        self._raise_error(status) #TODO test

  def _get_store_resp(self, status, ret_prev):
    if status == 0:
      if (ret_prev):
        return self._read_value(PUT_WITH_PREV_LEN, PUT_WITH_PREV_FMT)
      else:
         return status
    else:
        self._raise_error(status) # TODO test

  def _read_data(self, expected_len):
    data = ""
    data_len = expected_len
    while len(data) < data_len:
      tmp = self.s.recv(data_len - len(data))
      if tmp == '':
        raise exceptions.EOFError("Got empty data (remote died?).")
      data += tmp
    assert len(data) == data_len
    return data

  def _read_value(self, expected_len, expected_fmt):
    response = self._read_data(expected_len)
    value_with_len = struct.unpack(expected_fmt, response)
    local_DecodeVarint = decoder._DecodeVarint32
    (length, pos) = local_DecodeVarint(value_with_len, 0)
    value = ""
    while length > 0:
      data = self.s.recv(length)
      if data == '':
        raise exceptions.EOFError("Got empty data (remote died?).")
      value += data
      length -= len(data)
    return value

  def _raise_error(self, status):
    error = self._read_value(ERROR_LEN, ERROR_FMT)
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
