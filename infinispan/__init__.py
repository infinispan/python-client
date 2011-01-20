# -*- coding: utf-8 -*-

"""
Constant definition
"""

__author__ = "Galder Zamarreño"
__copyright__ = "(C) 2011 Red Hat Inc."

import struct
from unsigned import to_varint

MAGIC = 0xA0, 0xA1
VERSION = 10

PUT = 0x01, 0x02
GET = 0x03, 0x04
PUT_IF_ABSENT = 0x05, 0x06
REPLACE = 0x07, 0x08
REPLACE_IF = 0x09, 0x0A
REMOVE = 0x0B, 0x0C
REMOVE_IF = 0x0D, 0x0E
CONTAINS = 0x0F, 0x10
GET_WITH_VERSION = 0x11, 0x12
CLEAR = 0x13, 0x14
STATS = 0x15, 0x16
PING = 0x17, 0x18
BULK_GET = 0x19, 0x1A
ERROR = 0x50

SUCCESS = 0x00
NOT_EXECUTED = 0x01
KEY_DOES_NOT_EXIST = 0x02
OK_STATUS = [SUCCESS, NOT_EXECUTED, KEY_DOES_NOT_EXIST]

INVALID_MAGIC_MSG_ID = 0x81
UNKNOWN_CMD = 0x82
UNKNOWN_VERSION = 0x83
PARSE_ERROR = 0x84
SERVER_ERROR = 0x85
CMD_TIMED_OUT = 0x86

# magic, msg_id, op_code, status, topology_mark
HEADER_RES_FMT = ">BBBBB"
HEADER_RES_LEN = struct.calcsize(HEADER_RES_FMT)

VERSION_FMT = ">Q"
VERSION_LEN = struct.calcsize(VERSION_FMT)

MAX_VLONG = 0x7FFFFFFFFFFFFFFF
MAX_VINT = 0xFFFFFFFF


KEY_LESS_SEND = lambda s, m, k, v, l, i, ver, c: \
  s.send(m)
KEY_ONLY_SEND = lambda s, m, k, v, l, i, ver, c: \
  s.send(m + to_varint(len(k)) + k)
KEY_VALUE_SEND = lambda s, m, k, v, l, i, ver, c: \
  s.send(
    m + to_varint(len(k)) + k +
    to_varint(l) + to_varint(i) +
    to_varint(len(v)) + v
  )
REPLACE_IF_SEND = lambda s, m, k, v, l, i, ver, c: \
  s.send(
    m + to_varint(len(k)) + k +
    to_varint(l) + to_varint(i) +
    struct.pack(VERSION_FMT, ver) +
    to_varint(len(v)) + v
  )

REMOVE_IF_SEND = lambda s, m, k, v, l, i, ver, c: \
  s.send(
    m + to_varint(len(k)) + k +
    struct.pack(VERSION_FMT, ver)
  )

BULK_GET_SEND = lambda s, m, k, v, l, i, ver, c: \
  s.send(m + to_varint(c))

SEND = {
  CLEAR[0]            : KEY_LESS_SEND,
  GET[0]              : KEY_ONLY_SEND,
  GET_WITH_VERSION[0] : KEY_ONLY_SEND,
  PUT[0]              : KEY_VALUE_SEND,
  PUT_IF_ABSENT[0]    : KEY_VALUE_SEND,
  REPLACE[0]          : KEY_VALUE_SEND,
  REPLACE_IF[0]       : REPLACE_IF_SEND,
  REMOVE[0]           : KEY_ONLY_SEND,
  REMOVE_IF[0]        : REMOVE_IF_SEND,
  CONTAINS[0]         : KEY_ONLY_SEND,
  STATS[0]            : KEY_LESS_SEND,
  PING[0]             : KEY_LESS_SEND,
  BULK_GET[0]         : BULK_GET_SEND,
}

KEY_LESS_RECV = lambda rc, st, ret_prev: \
  True if not st else False

KEY_ONLY_RECV = lambda rc, st, ret_prev: \
  None if st == KEY_DOES_NOT_EXIST else rc._read_ranged_bytes()

GET_WITH_VERSION_RECV = lambda rc, st, ret_prev: \
  (0, None) if st == KEY_DOES_NOT_EXIST else \
  (struct.unpack(VERSION_FMT, rc._read_bytes(VERSION_LEN))[0],
   rc._read_ranged_bytes())

# Hot Rod protocol status=0x00 becomes True, and since there's no other
# possible status return, when previous must be returned, just return previous
PUT_RECV = lambda rc, st, ret_prev: \
  True if not st and not ret_prev else rc._read_ranged_bytes()

TWO_WAY_RES = {
  SUCCESS      : True,
  NOT_EXECUTED : False,
}

TWO_WAY_RECV = lambda rc, st, rp: MULTI_WAY_RECV(rc, st, rp, TWO_WAY_RES)

THREE_WAY_RES = {
  SUCCESS             : True,
  NOT_EXECUTED        : -1,
  KEY_DOES_NOT_EXIST  : False,
}

THREE_WAY_RECV = lambda rc, st, rp: MULTI_WAY_RECV(rc, st, rp, THREE_WAY_RES)

MULTI_WAY_RECV = lambda rc, st, rp, convert: \
  convert[st] if not rp else (convert[st], rc._read_ranged_bytes())

STATS_RECV = lambda rc, st, rp: rc._read_bounded_map()

BULK_GET_RECV = lambda rc, st, rp: rc._read_map()

ERROR_RECV = lambda rc, st, rp: rc._raise_error(st)

RECV = {
  CLEAR[1]               : KEY_LESS_RECV,
  GET[1]                 : KEY_ONLY_RECV,
  GET_WITH_VERSION[1]    : GET_WITH_VERSION_RECV,
  PUT[1]                 : PUT_RECV,
  PUT_IF_ABSENT[1]       : TWO_WAY_RECV,
  REPLACE[1]             : TWO_WAY_RECV,
  REPLACE_IF[1]          : THREE_WAY_RECV,
  ERROR                  : ERROR_RECV,
  REMOVE[1]              : THREE_WAY_RECV,
  REMOVE_IF[1]           : THREE_WAY_RECV,
  CONTAINS[1]            : KEY_LESS_RECV,
  STATS[1]               : STATS_RECV,
  PING[1]                : KEY_LESS_RECV,
  BULK_GET[1]            : BULK_GET_RECV,
}