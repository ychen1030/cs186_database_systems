package edu.berkeley.cs186.database.io;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.lang.IllegalArgumentException;
import java.util.Arrays;
import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.Closeable;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;

/**
 * A PageAllocation system for an OS paging system. Provides memory-mapped paging from the OS, an
 * interface to individual pages with the Page objects, an LRU cache for pages, 16GB worth of paging,
 * and virtual page translation.
 *
 */
public class PageAllocator implements Closeable {
    private static final int numHeaderPages = 1024;
    private static final int cacheSize = 1024;

    private static AtomicInteger pACounter = new AtomicInteger(0);
    private static LRUCache<Long, Page> pageLRU = new LRUCache<>(cacheSize);
    private static AtomicLong numIOs = new AtomicLong(0);
    private static AtomicLong cacheMisses = new AtomicLong(0);

    private LockContext lockContext;
    private Page masterPage;
    private FileChannel fc;
    private int numPages;
    private int numUsedHeaderPages;
    private int allocID;
    private boolean durable;
    /**
     * Create a new PageAllocator that writes its bytes into a file named fName. If wipe is true, the
     * data in the page is completely removed.
     *
     * @param lockContext the lock context
     * @param fName the name of the file for this PageAllocator
     * @param wipe a boolean specifying whether to wipe the file
     */
    public PageAllocator(LockContext lockContext, String fName, boolean wipe,
                         BaseTransaction transaction) {
        this(lockContext, fName, wipe, true, transaction);
    }

    public PageAllocator(String fName, boolean wipe, BaseTransaction transaction) {
        this(fName, wipe, true, transaction);
    }

    public PageAllocator(String fName, boolean wipe, boolean durable, BaseTransaction transaction) {
        this(new DummyLockContext(), fName, wipe, durable, transaction);
    }

    public PageAllocator(LockContext lockContext, String fName, boolean wipe, boolean durable,
                         BaseTransaction transaction) {
        this.lockContext = lockContext;
        this.durable = durable;
        try {
            this.fc = new RandomAccessFile(fName, "rw").getChannel();
        } catch (IOException e) {
            throw new PageException("Could not open File: " + e.getMessage());
        }

        this.masterPage = new Page(this.lockContext.childContext(-1), this.fc, 0, -1);
        this.allocID = pACounter.getAndIncrement();

        if (wipe) {
            // Nukes masterPage and headerPages
            byte[] masterBytes = this.masterPage.readBytes(transaction);
            IntBuffer ib = ByteBuffer.wrap(masterBytes).asIntBuffer();

            int[] pageCounts = new int[ib.capacity()];
            ib.get(pageCounts);

            this.numPages = 0;

            for (int i = 0; i < numHeaderPages; i++) {
                if (pageCounts[i] > 0) {
                    getHeadPage(i).wipe(transaction);
                }
            }

            this.masterPage.wipe(transaction);
        }

        byte[] masterBytes = masterPage.readBytes(transaction);
        IntBuffer ib = ByteBuffer.wrap(masterBytes).asIntBuffer();
        int[] pageCounts = new int[ib.capacity()];
        ib.get(pageCounts);

        this.numPages = 0;
        this.numUsedHeaderPages = 0;
        for (int i = 0; i < numHeaderPages; i++) {
            this.numPages += pageCounts[i];
            if (pageCounts[i] != 0) {
                this.numUsedHeaderPages += 1;
            }
        }

        // TODO(hw5): any initialization of lock context (or none)
        this.lockContext.capacity(this.numPages + this.numUsedHeaderPages);
    }

    /**
     * Allocates a new page in the file.
     *
     * @return the virtual page number of the page
     */
    public synchronized int allocPage(BaseTransaction transaction) {
        byte[] masterBytes = this.masterPage.readBytes(transaction);
        IntBuffer ib = ByteBuffer.wrap(masterBytes).asIntBuffer();
        int[] pageCounts = new int[ib.capacity()];
        ib.get(pageCounts);

        Page headerPage = null;
        int headerIndex = -1;
        for (int i = 0; i < numHeaderPages; i++) {
            if (pageCounts[i] < Page.pageSize) {
                // Found header page with space
                headerPage = getHeadPage(i);
                headerIndex = i;
                break;
            }
        }

        if (headerPage == null) {
            throw new PageException("No free Pages Available");
        }

        byte[] headerBytes = headerPage.readBytes(transaction);
        int pageIndex = -1;

        for (int i = 0; i < Page.pageSize; i++) {
            if (headerBytes[i] == 0) {
                pageIndex = i;
                break;
            }
        }

        if (pageIndex == -1) {
            throw new PageException("Header page should have free page but doesnt");
        }

        int newCount = pageCounts[headerIndex] + 1;
        this.masterPage.getBuffer(transaction).putInt(headerIndex * 4, newCount);
        headerPage.getBuffer(transaction).put(pageIndex, (byte) 1);

        if (this.durable) {
            this.masterPage.flush();
            headerPage.flush();
        }

        int pageNum = headerIndex * Page.pageSize + pageIndex;
        fetchPage(transaction, pageNum).wipe(transaction);
        this.numPages += 1;
        if (pageCounts[headerIndex] == 0) {
            this.numUsedHeaderPages += 1;
        }

        // TODO(hw5): any lock context changes needed
        this.lockContext.capacity(this.numPages + this.numUsedHeaderPages);

        return pageNum;
    }

    /**
     * Fetches the page corresponding to virtual page number pageNum.
     *
     * @param pageNum the virtual page number
     * @return a Page object wrapping the page corresponding to pageNum
     */
    public synchronized Page fetchPage(BaseTransaction transaction, int pageNum) {
        if (pageNum < 0) {
            throw new PageException("invalid page number -- out of bounds");
        }

        numIOs.getAndIncrement();

        synchronized(PageAllocator.class) {
            if (pageLRU.containsKey(translatePageNum(pageNum))) {
                return pageLRU.get(translatePageNum(pageNum));
            }
        }

        int headPageIndex = pageNum / Page.pageSize;

        if (headPageIndex >= numHeaderPages) {
            throw new PageException("invalid page number -- out of bounds");
        }

        int headCount = this.masterPage.getBuffer(transaction).getInt(headPageIndex * 4);

        if (headCount < 1) {
            throw new PageException("invalid page number -- page not allocated");
        }

        Page headPage = getHeadPage(headPageIndex);

        int dataPageIndex = pageNum % Page.pageSize;

        byte validByte = headPage.getBuffer(transaction).get(dataPageIndex);

        if (validByte == 0) {
            throw new PageException("invalid page number -- page not allocated");
        }

        int dataBlockID = 2 + headPageIndex * (Page.pageSize + 1) + dataPageIndex;
        Page dataPage = new Page(this.lockContext.childContext(pageNum), this.fc, dataBlockID, pageNum,
                                 this.durable);

        synchronized(PageAllocator.class) {
            pageLRU.put(translatePageNum(pageNum), dataPage);
        }

        return dataPage;
    }

    /**
     * Frees the page to be returned back to the system. The page is no longer valid and can be re-used
     * the next time the user called allocPage.
     *
     * @param p the page to free
     * @return whether or not the page was freed
     */
    public synchronized boolean freePage(BaseTransaction transaction, Page p) {
        if (this.durable) {
            p.flush();
        }
        int pageNum = p.getPageNum();
        int headPageIndex = pageNum / Page.pageSize;
        int dataPageIndex = pageNum % Page.pageSize;

        Page headPage = getHeadPage(headPageIndex);
        Buffer headPageBuffer = headPage.getBuffer(transaction);

        if (headPageBuffer.get(dataPageIndex) == 0) {
            return false;
        }

        headPageBuffer.put(dataPageIndex, (byte) 0);
        if (this.durable) {
            headPage.flush();
        }

        Buffer masterPageBuffer = masterPage.getBuffer(transaction);
        int count = masterPageBuffer.getInt(4 * headPageIndex);
        masterPageBuffer.putInt(4 * headPageIndex, count - 1);
        if (this.durable) {
            masterPage.flush();
        }

        synchronized(PageAllocator.class) {
            if (pageLRU.containsKey(translatePageNum(pageNum))) {
                pageLRU.remove(translatePageNum(pageNum));
            }
        }

        this.numPages -= 1;
        if (count == 1) {
            this.numUsedHeaderPages -= 1;
        }

        // TODO(hw5): any lock context changes needed
        this.lockContext.capacity(this.numPages + this.numUsedHeaderPages);

        return true;
    }

    /**
     * Frees the page to be returned back to the system. The page is no longer valid and can be re-used
     * the next time the user called allocPage.
     *
     * @param pageNum the virtual page number to be flushed
     * @return whether or not the page was freed
     */
    public synchronized boolean freePage(BaseTransaction transaction, int pageNum) {
        Page p;
        try {
            p = fetchPage(transaction, pageNum);
        } catch (PageException e) {
            return false;
        }
        return freePage(transaction, p);
    }

    /**
     * Close this PageAllocator.
     */
    public synchronized void close() {
        if (this.masterPage == null) {
            return;
        }
        if (this.durable) {
            this.masterPage.flush();
        }
        List<Long> toRemove = new ArrayList<Long>();
        Set<Long> vPageNums = null;
        List<Page> toFlush = new ArrayList<Page>();

        synchronized(PageAllocator.class) {
            vPageNums = pageLRU.keySet();
        }

        for (Long l : vPageNums) {
            if (translateAllocator(l) == this.allocID) {
                toRemove.add(l);
            }
        }

        synchronized(PageAllocator.class) {
            for (Long vPageNum : toRemove) {
                if (pageLRU.containsKey(vPageNum)) {
                    toFlush.add(pageLRU.get(vPageNum));
                    pageLRU.remove(vPageNum);
                }
            }
        }
        if (this.durable) {
            for (Page p : toFlush) {
                p.flush();
            }
        }
        this.masterPage = null;
        try {
            this.fc.close();
        } catch (IOException e) {
            throw new PageException("Could not close Page Alloc " + e.getMessage());
        }
    }

    private synchronized Page getHeadPage(int headIndex) {
        int headBlockID = 1 + headIndex * (Page.pageSize + 1);
        return new Page(this.lockContext.childContext(-1), this.fc, headBlockID, -1);
    }

    public synchronized int getNumPages() {
        return this.numPages;
    }

    public synchronized static long getNumIOs() {
        return PageAllocator.numIOs.get();
    }

    static synchronized void incrementNumIOs() {
        PageAllocator.numIOs.getAndIncrement();
    }

    static synchronized void incrementCacheMisses() {
        PageAllocator.cacheMisses.getAndIncrement();
    }

    public synchronized static long getNumCacheMisses() {
        return PageAllocator.cacheMisses.get();
    }

    private synchronized long translatePageNum(int pageNum) {
        return (((long) this.allocID) << 32) | (((long) pageNum) & 0xFFFFFFFFL);
    }

    static synchronized private int translateAllocator(long vPageNum) {
        return (int) ((vPageNum & 0xFFFFFFFF00000000L) >> 32);
    }

    /**
     * @return an iterator of the valid pages managed by this PageAllocator.
     */
    public PageIterator iterator(BaseTransaction transaction) {
        return new PageIterator(transaction);
    }

    public class PageIterator implements BacktrackingIterator<Page> {
        private int pageNum;
        private int cursor;
        private int markedPageNum;
        private int markedCursor;
        private BaseTransaction transaction;

        PageIterator(BaseTransaction transaction) {
            this.pageNum = 0;
            this.cursor = 0;
            this.markedPageNum = 0;
            this.markedCursor = 0;
            this.transaction = transaction;
        }

        public boolean hasNext() {
            return this.pageNum < PageAllocator.this.numPages;
        }

        public Page next() {
            if (this.hasNext()) {
                while (true) {
                    Page p;
                    try {
                        p = PageAllocator.this.fetchPage(transaction, cursor);
                        cursor++;
                        pageNum++;
                        return p;
                    } catch (PageException e) {
                        cursor++;
                    }
                }
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Marks a page to come back to later.
         *
         * See comments on PageIterator#reset for usage. Marking twice overrides
         * the initial mark. This may only be called after next().
         */
        public void mark() {
            if (this.cursor == 0) {
                throw new UnsupportedOperationException("cannot mark() before next()");
            }
            this.markedPageNum = this.pageNum;
            this.markedCursor = this.cursor;
        }

        /**
         * Reset the PageIterator to the last marked page.
         *
         * Call PageIterator#mark after PageIterator#next to tell PageIterator
         * to keep track of the page that PageIterator#next returned. When you later
         * call PageIterator#reset, the PageIterator will jump back to that page,
         * and return it the next time you call PageIterator#next.
         *
         * Only one page can be marked at a time, and marking a second page
         * will override the original mark and cause all resets to jump to
         * that second page instead.
         */
        public void reset() {
            this.pageNum = this.markedPageNum - 1;
            this.cursor = this.markedCursor - 1;
        }
    }
}
