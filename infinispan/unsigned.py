# -*- coding: utf-8 -*-

"""
Encoder/decoder functions for unsigned variable length numbers.
"""
__author__ = "Galder Zamarreño"
__copyright__ = "(C) 2010-2011 Red Hat Inc."

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