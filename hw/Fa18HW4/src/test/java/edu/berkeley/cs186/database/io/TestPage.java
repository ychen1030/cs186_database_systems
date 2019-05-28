package edu.berkeley.cs186.database.io;

import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;

/**
* Tests Page.java
* Should be optional tests for student unless for debugging
* @author  Sammy Sidhu
* @version 1.0
*/

public class TestPage {
  private final String fName = "TestPage.temp";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void TestPageConstructor() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
    Page p = new Page(fc, 0, 0);
    assertEquals(0, p.getPageNum());
    fc.close();
  }

  @Test
  public void TestPageZeroOnNew() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
    Page p = new Page(fc, 0, 0);
    assertEquals(0, p.getPageNum());
    byte[] b = p.readBytes();
    assertEquals(Page.pageSize, b.length);
    for (int i = 0; i < b.length; i++) {
      assertEquals(0, b[i]);
    }
    fc.close();
  }

  @Test
  public void TestPageWriteReadByte() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
    Page p = new Page(fc, 0, 0);
    assertEquals(0, p.getPageNum());
    byte[] b = p.readBytes();
    assertEquals(Page.pageSize, b.length);
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i % 256);
    }
    p.writeBytes(0, Page.pageSize, b);
    byte[] b2 = p.readBytes();
    assertEquals(Page.pageSize, b.length);
    for (int i = 0; i < b.length; i++) {
      assertEquals(b[i], b2[i]);
    }
    fc.close();
  }

  @Test
  public void TestPageWriteReadByteDurable() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
    Page p = new Page(fc, 0, 0);
    assertEquals(0, p.getPageNum());
    byte[] b = p.readBytes();
    assertEquals(Page.pageSize, b.length);
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i % 256);
    }
    p.writeBytes(0, Page.pageSize, b);
    p.flush();
    p = new Page(fc, 0, 0);
    byte[] b2 = p.readBytes();
    assertEquals(Page.pageSize, b.length);
    for (int i = 0; i < b.length; i++) {
      assertEquals(b[i], b2[i]);
    }
    fc.close();
  }

  @Test
  public void TestPageWriteWipe() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
    Page p = new Page(fc, 0, 0);
    assertEquals(0, p.getPageNum());
    byte[] b = p.readBytes();
    assertEquals(Page.pageSize, b.length);
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i % 256);
    }
    p.writeBytes(0, Page.pageSize, b);
    byte[] b2 = p.readBytes();
    assertEquals(Page.pageSize, b.length);
    for (int i = 0; i < b.length; i++) {
      assertEquals(b[i], b2[i]);
    }
    p.wipe();
    b = p.readBytes();
    assertEquals(Page.pageSize, b.length);
    for (int i = 0; i < b.length; i++) {
      assertEquals((byte) 0, b[i]);
    }

    fc.close();
  }

  @Test
  public void TestPageOutOfBounds() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
    Page p = new Page(fc, 0, 0);
    assertEquals(0, p.getPageNum());

    boolean thrown = false;
    try {
      byte b = p.readByte(-1);
    } catch (PageException e) {
      thrown = true;
    }
    assertTrue(thrown);

    thrown = false;
    try {
      byte b = p.readByte(Page.pageSize);
    } catch (PageException e) {
      thrown = true;
    }
    assertTrue(thrown);

    fc.close();
  }

}
