#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Hot Rod constants.

Copyright (c) 2010  Galder Zamarreño
"""

import struct

REQ_MAGIC = 0xA0
RES_MAGIC = 0xA1

# VERSION = ?
VERSION_10 = 10

PUT_REQ = 0x01
PUT_RES = 0x02
GET_REQ = 0x03
GET_RES = 0x04

# TODO: sort out format
REQ_FMT = ">BBBBBBBBB"
#EMPTY_RES_FMT = ">BBBBB"
RES_FMT = ">BBBBBs"
MIN_RES_PKT = struct.calcsize(RES_FMT)

RES_HEADER_FMT = ">BBBBB"
RES_HEADER_LEN = struct.calcsize(RES_HEADER_FMT)

#GET_HEADER_FMT = RES_HEADER_FMT + "s"
GET_HEADER_FMT = RES_HEADER_FMT + "s"
GET_HEADER_LEN = struct.calcsize(GET_HEADER_FMT)