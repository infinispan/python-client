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

# magic, msg_id, version, op_code, cache name legth + cache, flag, clientint, topoid, txtypeid,
REQ_FMT = ">BBBBBBBBB"
# magic, msg_id, op_code, status, topology_mark
RES_HEADER_FMT = ">BBBBB"
RES_HEADER_LEN = struct.calcsize(RES_HEADER_FMT)
GET_RES_FMT = ">s"
GET_RES_LEN = struct.calcsize(GET_RES_FMT)
PUT_WITH_PREV_FMT = ">s"
PUT_WITH_PREV_LEN = struct.calcsize(PUT_WITH_PREV_FMT)

# TODO: Find a way to consolidate GET_RES and PUT_WITH_PREV
# TODO: Find a way to, given the fmt, calculate the len and viceversa without relying on constants for both

class HotRodClient(object):
  def __init__(self, host='127.0.0.1', port=11222):
    self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.s.connect_ex((host, port))

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
    msg = struct.pack(REQ_FMT, REQ_MAGIC, 0x01, VERSION_10,
                      cmd, 0, flag, 0x01, 0, 0)

    if key == '':
      self.s.send(msg) # i.e. clear
    else:
      if val == '':
        self.s.send(msg + encoder._VarintBytes(len(key)) + key) # i.e. get, contains_key...
      else:
        self.s.send(msg + encoder._VarintBytes(len(key)) + key +
                    encoder._VarintBytes(lifespan) + encoder._VarintBytes(max_idle) +
                    encoder._VarintBytes(len(val)) + val) # i.e. put

  def _get_resp(self, key, val, ret_prev):
    header = self._read_data(RES_HEADER_LEN)
    magic, msg_id, op_code, status, topology_mark = struct.unpack(RES_HEADER_FMT, header)
    assert (magic == RES_MAGIC), "Got magic: %d" % magic

    if (key == ''):
      if status == 0:
        return
      else:
        raise HotRodError(status, rv) # TODO test and sort out rv
    else:
      if val == '':
        return self._get_retrieval_resp(status)
      else:
        return self._get_store_resp(status, ret_prev)

  def _get_retrieval_resp(self, status):
    if status == 2:
      return None
    else:
      if status == 0:
        return self._read_value(GET_RES_LEN, GET_RES_FMT)
      else:
        raise HotRodError(status, rv)

  def _get_store_resp(self, status, ret_prev):
    if status == 0:
      if (ret_prev):
        return self._read_value(PUT_WITH_PREV_LEN, PUT_WITH_PREV_FMT)
      else:
         return status
    else:
        raise HotRodError(status, rv) # TODO test and sort out rv

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
