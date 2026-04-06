"""Audio codec helpers that do not depend on external media libraries."""

from typing import Optional


def decode_g711_alaw_byte(value: int) -> int:
    """Decode one G.711 A-law byte into a signed 16-bit PCM sample."""

    value ^= 0x55
    sign = value & 0x80
    exponent = (value & 0x70) >> 4
    mantissa = value & 0x0F
    sample = mantissa << 4
    if exponent == 0:
        sample += 8
    else:
        sample += 0x108
        sample <<= exponent - 1
    return -sample if sign else sample


def decode_g711_mulaw_byte(value: int) -> int:
    """Decode one G.711 mu-law byte into a signed 16-bit PCM sample."""

    value = ~value & 0xFF
    sign = value & 0x80
    exponent = (value >> 4) & 0x07
    mantissa = value & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


def decode_g711_to_pcm16_bytes(data: bytes, is_alaw: bool) -> bytes:
    """Decode G.711 A-law or mu-law payload into little-endian PCM16 bytes."""

    if not data:
        return b''
    decoder = decode_g711_alaw_byte if is_alaw else decode_g711_mulaw_byte
    pcm = bytearray(len(data) * 2)
    offset = 0
    for byte in data:
        sample = decoder(byte)
        pcm[offset:offset + 2] = int(sample).to_bytes(2, byteorder='little', signed=True)
        offset += 2
    return bytes(pcm)

