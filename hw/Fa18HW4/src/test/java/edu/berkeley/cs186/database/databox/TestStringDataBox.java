package edu.berkeley.cs186.database.databox;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

public class TestStringDataBox {
  @Test
  public void testAllowableCharactersAreSorted() {
    String s = StringDataBox.ALLOWABLE_CHARACTERS;
    for (int i = 0; i < s.length() - 1; ++i) {
      assertTrue(s.charAt(i) < s.charAt(i + 1));
    }
  }

  @Test(expected = DataBoxException.class)
  public void testEmptyString() {
    new StringDataBox("", 0);
  }

  @Test(expected = DataBoxException.class)
  public void testNonAlphanumericString() {
    new StringDataBox("#", 1);
  }

  @Test
  public void testLegalStrings() {
    new StringDataBox(" ", 1);
    new StringDataBox("0", 1);
    new StringDataBox("A", 1);
    new StringDataBox("a", 1);
  }

  @Test
  public void testType() {
    assertEquals(Type.stringType(3), new StringDataBox("foo", 3).type());
  }

  @Test(expected = DataBoxException.class)
  public void testGetBool() {
    new StringDataBox("foo", 3).getBool();
  }

  @Test(expected = DataBoxException.class)
  public void testGetInt() {
    new StringDataBox("foo", 3).getInt();
  }

  @Test(expected = DataBoxException.class)
  public void testGetFloat() {
    new StringDataBox("foo", 3).getFloat();
  }

  @Test
  public void testGetString() {
    assertEquals("f", new StringDataBox("foo", 1).getString());
    assertEquals("fo", new StringDataBox("foo", 2).getString());
    assertEquals("foo", new StringDataBox("foo", 3).getString());
    assertEquals("foo ", new StringDataBox("foo", 4).getString());
    assertEquals("foo  ", new StringDataBox("foo", 5).getString());
  }

  @Test
  public void testToAndFromBytes() {
    for (String s : new String[]{"foo", "bar", "baz"}) {
      StringDataBox d = new StringDataBox(s, 3);
      byte[] bytes = d.toBytes();
      assertEquals(d, DataBox.fromBytes(ByteBuffer.wrap(bytes), Type.stringType(3)));
    }
  }

  @Test
  public void testEquals() {
    StringDataBox foo = new StringDataBox("foo", 3);
    StringDataBox zoo = new StringDataBox("zoo", 3);
    assertEquals(foo, foo);
    assertEquals(zoo, zoo);
    assertNotEquals(foo, zoo);
    assertNotEquals(zoo, foo);
  }

  @Test
  public void testCompareTo() {
    StringDataBox foo = new StringDataBox("foo", 3);
    StringDataBox zoo = new StringDataBox("zoo", 3);
    assertTrue(foo.compareTo(foo) == 0);
    assertTrue(foo.compareTo(zoo) < 0);
    assertTrue(zoo.compareTo(zoo) == 0);
    assertTrue(zoo.compareTo(foo) > 0);
  }
}
