def _VarintDecoder(mask):
  """Return an encoder for a basic varint value (does not include tag).

  Decoded values will be bitwise-anded with the given mask before being
  returned, e.g. to limit them to 32 bits.  The returned decoder does not
  take the usual "end" parameter -- the caller is expected to do bounds checking
  after the fact (often the caller can defer such checking until later).  The
  decoder returns a (value, new_pos) pair.
  """

  local_ord = ord
  def DecodeVarint(buffer, pos):
    result = 0
    shift = 0
    while 1:
      b = local_ord(buffer[pos])
      result |= ((b & 0x7f) << shift)
      pos += 1
      if not (b & 0x80):
        result &= mask
        return (result, pos)
      shift += 7
      if shift >= 64:
        raise _DecodeError('Too many bytes when decoding varint.')
  return DecodeVarint

_DecodeVarint32 = _VarintDecoder((1 << 32) - 1)
