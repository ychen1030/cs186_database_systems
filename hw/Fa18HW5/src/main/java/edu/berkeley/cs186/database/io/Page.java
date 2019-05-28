package edu.berkeley.cs186.database.io;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.common.AbstractBuffer;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.concurrency.*;
import edu.berkeley.cs186.database.table.Table;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;

/**
 * General-purpose wrapper for interacting with the memory-mapped bytes on a page.
 */
public class Page {
    public static final int pageSize = 4096;

    private LockContext lockContext;
    private MappedByteBuffer pageData;
    private int pageNum;
    private boolean durable;

    /**
     * Create a new page using fc with at offset blockNum with virtual page number pageNum
     *
     * @param lockContext the lock context
     * @param fc the file channel for this Page
     * @param blockNum the block in the file for this page
     * @param pageNum the virtual page number
     */
    public Page(LockContext lockContext, FileChannel fc, int blockNum, int pageNum) {
        this(lockContext, fc, blockNum, pageNum, true);
    }

    public Page(FileChannel fc, int blockNum, int pageNum) {
        this(fc, blockNum, pageNum, true);
    }

    public Page(FileChannel fc, int blockNum, int pageNum, boolean durable) {
        this(new DummyLockContext(new DummyLockContext()), fc, blockNum, pageNum, durable);
    }

    public Page(LockContext lockContext, FileChannel fc, int blockNum, int pageNum,
                boolean durable) {
        this.lockContext = lockContext;
        this.pageNum = pageNum;
        this.durable = durable;
        PageAllocator.incrementCacheMisses();
        try {
            this.pageData = fc.map(FileChannel.MapMode.READ_WRITE, blockNum * Page.pageSize, Page.pageSize);
        } catch (IOException e) {
            throw new PageException("Can't mmap page: " + pageNum + "at block: " + blockNum + " ; " +
                                    e.getMessage());
        }
    }

    public Buffer getBuffer(BaseTransaction transaction) {
        return new PageBuffer(transaction);
    }

    /**
     * Reads num bytes from offset position into buf.
     *
     * @param position the offset in the page to read from
     * @param num the number of bytes to read
     * @param buf the buffer to put the bytes into
     */
    private void readBytes(int position, int num, byte[] buf) {
        if (position < 0 || num < 0) {
            throw new PageException("position or num can't be negative");
        }
        if (Page.pageSize < position + num) {
            throw new PageException("readBytes is out of bounds");
        }
        if (buf.length < num) {
            throw new PageException("num bytes to read is longer than buffer");
        }
        pageData.position(position);
        pageData.get(buf, 0, num);
    }

    /**
     * Read all the bytes in file.
     *
     * @return a new byte array with all the bytes in the file
     */
    public byte[] readBytes(BaseTransaction transaction) {
        byte[] data = new byte[Page.pageSize];
        getBuffer(transaction).get(data);
        return data;
    }

    /**
     * Write num bytes from buf at offset position.
     *
     * @param position the offest in the file to write to
     * @param num the number of bytes to write
     * @param buf the source for the write
     */
    private void writeBytes(int position, int num, byte[] buf) {
        if (buf.length < num) {
            throw new PageException("num bytes to write is longer than buffer");
        }

        if (position < 0 || num < 0) {
            throw new PageException("position or num can't be negative");
        }

        if (Page.pageSize < num + position) {
            throw new PageException("writeBytes would go out of bounds");
        }

        pageData.position(position);
        pageData.put(buf, 0, num);
    }

    /**
     * Write all the bytes in file.
     */
    public void writeBytes(BaseTransaction transaction, byte[] data) {
        getBuffer(transaction).put(data);
    }

    /**
     * Completely wipe (zero out) the page.
     */
    public void wipe(BaseTransaction transaction) {
        byte[] zeros = new byte[Page.pageSize];
        writeBytes(transaction, zeros);
    }

    /**
     * Force the page to disk.
     */
    public void flush() {
        if (this.durable) {
            PageAllocator.incrementCacheMisses();
            this.pageData.force();
        }
    }

    /**
     * @return the virtual page number of this page
     */
    public int getPageNum() {
        return this.pageNum;
    }

    private class PageBuffer extends AbstractBuffer {
        private int offset;
        private BaseTransaction transaction;

        public PageBuffer(BaseTransaction transaction) {
            this(transaction, 0, 0);
        }

        public PageBuffer(BaseTransaction transaction, int offset, int position) {
            super(position);
            this.offset = offset;
            this.transaction = transaction;
        }

        public Buffer get(byte[] dst, int offset, int length) {
            LockContext table = lockContext.parentContext();
            if (!LockType.substitutable(lockContext.getGlobalLockType(transaction), LockType.S)) {
                if ( table.capacity() >= 10 && table.saturation(this.transaction) >= 0.2) {
                    table.escalate(this.transaction);
                }
                LockUtil.requestLocks(this.transaction, lockContext, LockType.S);
            }


            // TODO(hw5): locking code here
            Page.this.readBytes(this.offset + offset, length, dst);
            return this;
        }

        public Buffer put(byte[] src, int offset, int length) {
            if (!LockType.substitutable(lockContext.getGlobalLockType(transaction), LockType.X)) {
                LockUtil.requestLocks(transaction, lockContext, LockType.X);
            }

            // TODO(hw5): locking code here
            Page.this.writeBytes(this.offset + offset, length, src);
            return this;
        }

        public Buffer slice() {
            return new PageBuffer(transaction, offset + position(), 0);
        }

        public Buffer duplicate() {
            PageBuffer pb = new PageBuffer(transaction, offset, position());
            return pb;
        }
    }
}
