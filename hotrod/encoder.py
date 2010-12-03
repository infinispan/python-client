# TODO: refactor to hotrod.py

def _VarintEncoder():
  """Return an encoder for a basic varint value (does not include tag)."""

  local_chr = chr
  def EncodeVarint(write, value):
    bits = value & 0x7f
    value >>= 7
    while value:
      write(local_chr(0x80|bits))
      bits = value & 0x7f
      value >>= 7
    return write(local_chr(bits))

  return EncodeVarint

_EncodeVarint = _VarintEncoder()

def _VarintBytes(value):
  """Encode the given integer as a varint and return the bytes.  This is only
  called at startup time so it doesn't need to be fast."""

  pieces = []
  _EncodeVarint(pieces.append, value)
  return "".join(pieces)

  