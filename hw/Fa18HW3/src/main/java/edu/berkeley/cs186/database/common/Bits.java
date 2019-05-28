package edu.berkeley.cs186.database.common;

import java.nio.ByteBuffer;

public class Bits {
  public enum Bit { ZERO, ONE };

  /**
   * Get the ith bit of a byte where the 0th bit is the most significant bit
   * and the 7th bit is the least significant bit. Some examples:
   *
   *   - getBit(0b10000000, 7) == ZERO
   *   - getBit(0b10000000, 0) == ONE
   *   - getBit(0b01000000, 1) == ONE
   *   - getBit(0b00100000, 1) == ONE
   */
  public static Bit getBit(byte b, int i) {
    assert(0 <= i && i < 8);
    return ((b >> (7 - i)) & 1) == 0 ? Bit.ZERO : Bit.ONE;
  }

  /**
   * Get the ith bit of a byte array where the 0th bit is the most significat
   * bit of the first byte. Some examples:
   *
   *   - getBit(new byte[]{0b10000000, 0b00000000}, 0) == ONE
   *   - getBit(new byte[]{0b01000000, 0b00000000}, 1) == ONE
   *   - getBit(new byte[]{0b00000000, 0b00000001}, 15) == ONE
   */
  public static Bit getBit(byte[] bytes, int i) {
    String err = String.format("bytes.length = %d; i = %d.", bytes.length, i);
    assert (bytes.length > 0) : err;
    assert (0 <= i && i < bytes.length * 8) : err;
    return getBit(bytes[i/8], i % 8);
  }

  /**
   * Set the ith bit of a byte where the 0th bit is the most significant bit
   * and the 7th bit is the least significant bit. Some examples:
   *
   *   - setBit(0b00000000, 0, ONE) == ZERO
   *   - setBit(0b00000000, 1, ONE) == ONE
   *   - setBit(0b00000000, 2, ONE) == ONE
   */
  public static byte setBit(byte b, int i, Bit bit) {
    assert(0 <= i && i < 8);
    byte mask = (byte) (1 << (7 - i));
    switch (bit) {
      case ZERO: { return (byte) (b & ~mask); }
      case ONE: { return (byte) (b | mask); }
      default: { throw new IllegalArgumentException("Unreachable code."); }
    }
  }

  /**
   * Set the ith bit of a byte buffer where the 0th bit is the most significant
   * bit of the first byte read using buf.get(). The position of the buffer is
   * left unchanged. An example:
   *
   *   ByteBuffer buf = ByteBuffer.allocate(16);
   *   setBit(buf, 0, ONE);
   *   buf.array(); // [0b10000000, 0b00000000]
   *   setBit(buf, 1, ONE);
   *   buf.array(); // [0b11000000, 0b00000000]
   *   setBit(buf, 2, ONE);
   *   buf.array(); // [0b11100000, 0b00000000]
   *   setBit(buf, 15, ONE);
   *   buf.array(); // [0b11100000, 0b00000001]
   *
   * Note that setBit uses relative positioning based on the current position
   * of the byte buffer. For example:
   *
   *   ByteBuffer buf = ByteBuffer.allocate(16);
   *   // Advance the position of the buffer.
   *   buf.position(8);
   *   // This sets the 8th bit of the buffer, not the first.
   *   setBit(buf, 0, ONE);
   *   buf.array(); // [0b00000000, 0b10000000]
   */
  public static void setBit(ByteBuffer buf, int i, Bit bit) {
    // ByteBuffer does not have a relative single byte get and set, so we have
    // to use the relative bulk get and set. Every time we read or write anything,
    byte b = buf.get(buf.position() + (i / 8));
    b = setBit(b, i % 8, bit);
    buf.put(buf.position() + (i / 8), b);
  }
}
