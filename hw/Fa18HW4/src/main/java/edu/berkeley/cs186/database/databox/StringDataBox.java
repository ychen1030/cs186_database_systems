package edu.berkeley.cs186.database.databox;

import java.nio.charset.Charset;

public class StringDataBox extends DataBox {
  // Strings are only allowed to contain the following characters. Note that
  // the characters are sorted. That is ' ' < '0' < '1' < ...  < '9' < 'A' <
  // ... < 'Z' < 'a' < ... < 'z'.
  private static String SPACE = " ";
  private static String NUMBERS = "0123456789";
  private static String UPPERCASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static String LOWERCASE = "abcdefghijklmnopqrstuvwxyz";
  public static String ALLOWABLE_CHARACTERS = SPACE + NUMBERS + UPPERCASE + LOWERCASE;

  private String s;

  // Construct an m-byte string. If s has more than m-bytes, it is truncated to
  // its first m bytes. If s has fewer than m bytes, it is padded with spaces
  // until it is exactly m bytes long. Strings may only contain characters in
  // ALLOWABLE_CHARACTERS.
  //
  //   - new StringDataBox("123", 5).getString()     == "123  "
  //   - new StringDataBox("12345", 5).getString()   == "12345"
  //   - new StringDataBox("1234567", 5).getString() == "12345"
  public StringDataBox(String s, int m) {
    if (m <= 0) {
      String msg = String.format("Cannot construct a %d-byte string. " +
                                 "Strings must be at least one byte.", m);
      throw new DataBoxException(msg);
    }

    for (int i = 0; i < s.length(); ++i) {
      char c = s.charAt(i);
      if (ALLOWABLE_CHARACTERS.indexOf(c) == -1) {
        String msg = String.format("The string '%s' contains an illegal " +
                                   "character. The only legal characters are " +
                                   "'%s'.", s, ALLOWABLE_CHARACTERS);
        throw new DataBoxException(msg);
      }
    }

		if (m < s.length()) {
    	this.s = s.substring(0, m);
    } else {
 			this.s = String.format("%-" + m + "s", s);
    }
    assert(this.s.length() == m);
  }

  @Override
  public Type type() {
    return Type.stringType(s.length());
  }

  @Override
  public String getString() {
    return s;
  }

  @Override
  public byte[] toBytes() {
    return s.getBytes(Charset.forName("UTF-8"));
  }

  @Override
  public String toString() {
    return s;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StringDataBox)) {
      return false;
    }
    StringDataBox s = (StringDataBox) o;
    return this.s.equals(s.s);
  }

  @Override
  public int hashCode() {
    return s.hashCode();
  }

  @Override
  public int compareTo(DataBox d) {
    if (!(d instanceof StringDataBox)) {
      String err = String.format("Invalid comparison between %s and %s.",
                                 toString(), d.toString());
      throw new DataBoxException(err);
    }
    StringDataBox s = (StringDataBox) d;
    return this.s.compareTo(s.s);
  }
}
