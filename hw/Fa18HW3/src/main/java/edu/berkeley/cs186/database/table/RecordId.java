package edu.berkeley.cs186.database.table;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A Record in a particular table is uniquely identified by its page number
 * (the number of the page on which it resides) and its entry number (the
 * record's index in the page). A RecordId is a pair of the page number and
 * entry number.
 */
public class RecordId implements Comparable<RecordId> {
  private int pageNum;
  private short entryNum;

  public RecordId(int pageNum, short entryNum) {
    this.pageNum = pageNum;
    this.entryNum = entryNum;
  }

  public int getPageNum() {
    return this.pageNum;
  }

  public short getEntryNum() {
    return this.entryNum;
  }

  public static int getSizeInBytes() {
    // See toBytes.
    return Integer.BYTES + Short.BYTES;
  }

  public byte[] toBytes() {
    // A RecordId is serialized as its 4-byte page number followed by its
    // 2-byte short.
    return ByteBuffer.allocate(getSizeInBytes())
                     .putInt(pageNum)
                     .putShort(entryNum)
                     .array();
  }

  public static RecordId fromBytes(ByteBuffer buf) {
    return new RecordId(buf.getInt(), buf.getShort());
  }

  @Override
  public String toString() {
    return String.format("RecordId(%d, %d)", pageNum, entryNum);
  }

  public String toSexp() {
    return String.format("(%d %d)", pageNum, entryNum);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof RecordId)) {
      return false;
    }
    RecordId r = (RecordId) o;
    return pageNum == r.pageNum && entryNum == r.entryNum;
  }

  @Override
  public int hashCode() {
    return Objects.hash(pageNum, entryNum);
  }

  @Override
  public int compareTo(RecordId r) {
    int x = Integer.compare(pageNum, r.pageNum);
    return x == 0 ? Integer.compare(entryNum, r.entryNum) : x;
  }
}
