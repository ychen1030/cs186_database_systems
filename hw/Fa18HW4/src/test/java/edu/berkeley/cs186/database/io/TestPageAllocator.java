package edu.berkeley.cs186.database.io;

import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
* Tests PageAllocator.java
* Should be optional tests for student unless for debugging
* @author  Sammy Sidhu
* @version 1.0
*/

public class TestPageAllocator {
  private final String fName = "TestPageAllocator.temp";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  static private int byteEstimate(int pageNum) {
    return Page.pageSize*((pageNum/Page.pageSize)*(Page.pageSize + 1) + (pageNum % Page.pageSize) + 2);
  }

  @Test
  public void TestPageAllocatorConstructor() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);
    pA.close();
    FileChannel fc = new RandomAccessFile(tempFile, "r").getChannel();

    assertEquals(Page.pageSize, fc.size());

    ByteBuffer bb = ByteBuffer.allocate(Page.pageSize);
    fc.read(bb, 0);
    byte[] masterPage = bb.array();
    for (int i = 0; i < Page.pageSize; i++) {
      assertEquals((byte) 0, masterPage[i]);
    }
  }

  @Test
  public void TestPageAllocatorConstructorTwice() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);
    pA.close();
    FileChannel fc = new RandomAccessFile(tempFile, "r").getChannel();

    assertEquals(Page.pageSize, fc.size());

    ByteBuffer bb = ByteBuffer.allocate(Page.pageSize);
    fc.read(bb, 0);
    byte[] masterPage = bb.array();
    for (int i = 0; i < Page.pageSize; i++) {
      assertEquals((byte) 0, masterPage[i]);
    }
    fc.close();

    pA = new PageAllocator(tempFile.getAbsolutePath(), false, false);
    pA.close();
    fc = new RandomAccessFile(tempFile, "r").getChannel();


    bb = ByteBuffer.allocate(Page.pageSize);
    fc.read(bb, 0);
    masterPage = bb.array();
    for (int i = 0; i < Page.pageSize; i++) {
      assertEquals((byte) 0, masterPage[i]);
    }
    fc.close();
  }

  @Test
  public void TestPageAllocatorAllocPage() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    assertEquals(0, pA.allocPage());
    pA.close();
    FileChannel fc = new RandomAccessFile(tempFile, "r").getChannel();
    assertEquals(byteEstimate(1), fc.size());
    fc.close();
  }

  @Test
  public void TestPageAllocatorAllocPageMass() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 9000; i++) {
      assertEquals(i, pA.allocPage());
    }

    pA.close();
    FileChannel fc = new RandomAccessFile(tempFile, "r").getChannel();
    assertEquals(byteEstimate(9000), fc.size());
    fc.close();
  }

  @Test
  public void TestPageAllocatorFetchPage() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 4097; i++) {
      assertEquals(i, pA.allocPage());
      Page p = pA.fetchPage(i);

      for (int j = 0; j < 1024; j++) {
        int count = i*1024 + j;
        byte[] countBytes = ByteBuffer.allocate(4).putInt(count).array();
        p.writeBytes(j*4, 4, countBytes);
      }
    }
    pA.close();
    FileChannel fc = new RandomAccessFile(tempFile, "r").getChannel();
    assertEquals(byteEstimate(4097), fc.size());
    fc.close();

    pA = new PageAllocator(tempFile.getAbsolutePath(), false, false);

    for (int i = 0; i < 4097; i++) {
      Page p = pA.fetchPage(i);
      assertEquals(i, p.getPageNum());

      for (int j = 0; j < 1024; j++) {
        byte[] countBytes = p.readBytes(j*4,4);
        int count = ByteBuffer.wrap(countBytes).getInt();
        assertEquals(i*1024 + j, count);
      }
    }
  }

  @Test
  public void TestPageAllocatorFetchPageWithFree() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 10; i++) {
      assertEquals(i, pA.allocPage());
      Page p = pA.fetchPage(i);

      for (int j = 0; j < 1024; j++) {
        int count = i*1024 + j;
        byte[] countBytes = ByteBuffer.allocate(4).putInt(count).array();
        p.writeBytes(j*4, 4, countBytes);
      }
    }
    pA.close();
    FileChannel fc = new RandomAccessFile(tempFile, "r").getChannel();
    assertEquals(byteEstimate(10), fc.size());
    fc.close();

    pA = new PageAllocator(tempFile.getAbsolutePath(), false, false);

    for (int i = 0; i < 5; i++) {
      assertTrue(pA.freePage(i));
    }

    for (int i = 0; i < 5; i++) {
      boolean thrown = false;

      try {
        pA.fetchPage(i);
      } catch (PageException e) {
        thrown = true;
      }
      assertTrue(thrown);
    }

    for (int i = 5; i < 10; i++) {
      Page p = pA.fetchPage(i);
      assertEquals(i, p.getPageNum());

      for (int j = 0; j < 1024; j++) {
        byte[] countBytes = p.readBytes(j*4,4);
        int count = ByteBuffer.wrap(countBytes).getInt();
        assertEquals(i*1024 + j, count);
      }
    }
    pA.close();
  }

  @Test
  public void TestPageAllocatorFetchPageInvalid() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = -5; i < 5; i++) {
      boolean thrown = false;

      try {
        pA.fetchPage(i);
      } catch (PageException e) {
        thrown = true;
      }
      assertTrue(thrown);
    }

    pA.close();
  }

  @Test
  public void TestPageAllocatorFetchPageInvalid2() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 10; i++) {
      assertEquals(i, pA.allocPage());
    }
    boolean thrown = false;

    for (int i = 0; i < 10; i++) {
      assertEquals(i, pA.fetchPage(i).getPageNum());
    }

    try {
      pA.fetchPage(10);
    } catch (PageException e) {
      thrown = true;
    }
    assertTrue(thrown);

    pA.close();
  }

  @Test
  public void TestPageAllocatorFreePageReturn() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 10; i++) {
      assertFalse(pA.freePage(i));
    }
    for (int i = 0; i < 10; i++) {
      assertEquals(i,pA.allocPage());
    }
    for (int i = 0; i < 10; i++) {
      assertTrue(pA.freePage(i));
    }
    for (int i = 0; i < 10; i++) {
      assertFalse(pA.freePage(i));
    }

    pA.close();
  }
  @Test
  public void TestPageAllocatorFreePageReAlloc() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 10; i++) {
      assertEquals(i,pA.allocPage());
    }
    for (int i = 0; i < 5; i++) {
      assertTrue(pA.freePage(i));
    }
    for (int i = 0; i < 5; i++) {
      assertEquals(i,pA.allocPage());
    }

    pA.close();
  }
  @Test
  public void TestPageAllocatorFreePageReAlloc2() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 20; i++) {
      assertEquals(i,pA.allocPage());
    }
    for (int i = 0; i < 20; i += 2) {
      assertTrue(pA.freePage(i));
    }
    for (int i = 0; i < 20; i += 2) {
      assertEquals(i,pA.allocPage());
    }

    pA.close();
  }
  @Test
  public void TestPageAllocatorIterator() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 20; i++) {
      assertEquals(i,pA.allocPage());
    }
    Iterator<Page> pI= pA.iterator();

    for (int i = 0; i < 20; i++) {
      assertEquals(i, pI.next().getPageNum());
    }
    assertFalse(pI.hasNext());
    pA.close();
  }
  @Test
  public void TestPageAllocatorIteratorGap() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 20; i++) {
      assertEquals(i,pA.allocPage());
    }

    for (int i = 0; i < 20; i += 2) {
      assertTrue(pA.freePage(i));
    }

    Iterator<Page> pI= pA.iterator();

    for (int i = 1; i < 20; i += 2) {
      assertEquals(i, pI.next().getPageNum());
    }
    assertFalse(pI.hasNext());
    pA.close();
  }

  @Test
  public void TestPageAllocatorIteratorGap2() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 20; i++) {
      assertEquals(i,pA.allocPage());
    }

    for (int i = 0; i < 10; i++) {
      assertTrue(pA.freePage(i));
    }

    Iterator<Page> pI= pA.iterator();

    for (int i = 10; i < 20; i++) {
      assertEquals(i, pI.next().getPageNum());
    }
    assertFalse(pI.hasNext());
    pA.close();
  }

  @Test
  public void TestPageAllocatorIteratorGap3() throws IOException, FileNotFoundException {
    File tempFile = tempFolder.newFile(fName);
    PageAllocator pA = new PageAllocator(tempFile.getAbsolutePath(), true, false);

    for (int i = 0; i < 20; i++) {
      assertEquals(i,pA.allocPage());
    }

    for (int i = 10; i < 20; i++) {
      assertTrue(pA.freePage(i));
    }

    Iterator<Page> pI= pA.iterator();

    for (int i = 0; i < 10; i++) {
      assertEquals(i, pI.next().getPageNum());
    }
    assertFalse(pI.hasNext());
    pA.close();
  }

  @Test
  public void TestPageAllocatorMultiPageAlloc() throws IOException, FileNotFoundException {
    List<PageAllocator> allocs = new ArrayList<PageAllocator>();
    byte[] data = new byte[Page.pageSize];
    File[] tempFiles = new File[16];
    for (int i = 0; i < 16; i++) {
      final String FName = fName + i;
      tempFiles[i] = tempFolder.newFile(FName);
      PageAllocator pA = new PageAllocator(tempFiles[i].getAbsolutePath(), true, false);
      allocs.add(pA);

      for (int j = 0; j < Page.pageSize; j++) {
        data[j] = (byte) i;
      }

      for (int j = 0; j < 256; j++) {
        assertEquals(j, pA.allocPage());
        Page p = pA.fetchPage(j);
        p.writeBytes(0, Page.pageSize, data);
      }
    }

    for (int j = 0; j < 256; j++) {
      for (int i = 0; i < 16; i++) {
        byte[] bytes = allocs.get(i).fetchPage(j).readBytes();
        for (byte b : bytes) {
          assertEquals((byte) i, b);
        }
      }
    }

    for (int i = 0; i < 16; i++) {
      allocs.get(i).close();
    }
  }

  @Test
  public void TestPageAllocatorMultiPageAllocClose() throws IOException, FileNotFoundException {
    List<PageAllocator> allocs = new ArrayList<PageAllocator>();
    byte[] data = new byte[Page.pageSize];
    File[] tempFiles = new File[16];
    for (int i = 0; i < 16; i++) {
      final String FName = fName + i;
      tempFiles[i] = tempFolder.newFile(FName);
      PageAllocator pA = new PageAllocator(tempFiles[i].getAbsolutePath(), true, false);
      allocs.add(pA);

      for (int j = 0; j < Page.pageSize; j++) {
        data[j] = (byte) i;
      }

      for (int j = 0; j < 256; j++) {
        assertEquals(j, pA.allocPage());
        Page p = pA.fetchPage(j);
        p.writeBytes(0, Page.pageSize, data);
      }
    }
    for (int i = 0; i < 16; i++) {
      for (int j = 0; j < 256; j++) {
        byte[] bytes = allocs.get(i).fetchPage(j).readBytes();
        for (byte b : bytes) {
          assertEquals((byte) i, b);
        }
      }
      allocs.get(i).close();
    }
  }


}
