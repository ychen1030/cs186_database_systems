package edu.berkeley.cs186.database.table;

import java.io.Closeable;
import java.util.*;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.common.Bits;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.stats.TableStats;

/**
 * # Overview
 * A Table represents a database table with which users can insert, get,
 * update, and delete records:
 *
 *   // Create a brand new table t(x: int, y: int) which is persisted in the
 *   // file "t.table".
 *   List<String> fieldNames = Arrays.asList("x", "y");
 *   List<String> fieldTypes = Arrays.asList(Type.intType(), Type.intType());
 *   Schema schema = new Schema(fieldNames, fieldTypes);
 *   Table t = new Table(schema, "t", "t.table");
 *
 *   // Insert, get, update, and delete records.
 *   List<DataBox> a = Arrays.asList(new IntDataBox(1), new IntDataBox(2));
 *   List<DataBox> b = Arrays.asList(new IntDataBox(3), new IntDataBox(4));
 *   RecordId rid = t.addRecord(a);
 *   Record ra = t.getRecord(rid);
 *   t.updateRecord(b, rid);
 *   Record rb = t.getRecord(rid);
 *   t.deleteRecord(rid);
 *
 *   // Close the table. All tables must be closed.
 *   t.close();
 *
 * # Persistence
 * Every table constructs a new PageAllocator which it uses to persist its data
 * into a file. For example, the table above persists itself into a file
 * "t.table". We can later run the following code to reload the table:
 *
 *   // Load the table t from the file "t.table". Unlike above, we do not have
 *   // to specify the schema of t because it will be parsed from "t.table".
 *   Table t = new Table("t", "t.table");
 *   // Don't forget to close the table.
 *   t.close();
 *
 * # Storage Format
 * Now, we discuss how tables serialize their data into files.
 *
 *   1. Each file begins with a header page into which tables serialize their
 *      schema.
 *   2. All remaining pages are data pages. Every data page begins with an
 *      n-byte bitmap followed by m records. The bitmap indicates which records
 *      in the page are valid. The values of n and m are set to maximize the
 *      number of records per page (see computeDataPageNumbers for details).
 *
 * For example, here is a cartoon of what a table's file would look like if we
 * had 5-byte pages and 1-byte records:
 *
 *            Serialized Schema___________________________________
 *           /                                                    \
 *          +----------+----------+----------+----------+----------+ \
 *   Page 0 | 00000001 | 00000001 | 01111111 | 00000001 | 00000100 |  |- header
 *          +----------+----------+----------+----------+----------+ /
 *          +----------+----------+----------+----------+----------+ \
 *   Page 1 | 1001xxxx | 01111010 | xxxxxxxx | xxxxxxxx | 01100001 |  |
 *          +----------+----------+----------+----------+----------+  |
 *   Page 2 | 1101xxxx | 01110010 | 01100100 | xxxxxxxx | 01101111 |  |- data
 *          +----------+----------+----------+----------+----------+  |
 *   Page 3 | 0011xxxx | xxxxxxxx | xxxxxxxx | 01111010 | 00100001 |  |
 *          +----------+----------+----------+----------+----------+ /
 *           \________/ \________/ \________/ \________/ \________/
 *            bitmap     record 0   record 1   record 2   record 3
 *
 *  - The first page (Page 0) is the header page and contains the serialized
 *    schema.
 *  - The second page (Page 1) is a data page. The first byte of this data page
 *    is a bitmap, and the next four bytes are each records. The first and
 *    fourth bit are set indicating that record 0 and record 3 are valid.
 *    Record 1 and record 2 are invalid, so we ignore their contents.
 *    Similarly, the last four bits of the bitmap are unused, so we ignore
 *    their contents.
 *  - The third and fourth page (Page 2 and 3) are also data pages and are
 *    formatted similar to Page 1.
 *
 *  When we add a record to a table, we add it to the very first free slot in
 *  the table. See addRecord for more information.
 */
public class Table implements Closeable {
    public static final String FILENAME_PREFIX = "db";
    public static final String FILENAME_EXTENSION = ".table";

    // The name of the database.
    private String name;

    // The filename of the file in which this table is persisted.
    private String filename;

    // The schema of the database.
    private Schema schema;

    // The allocator used to persist the database.
    private PageAllocator allocator;

    // The size (in bytes) of the bitmap found at the beginning of each data page.
    private int bitmapSizeInBytes;

    // The number of records on each data page.
    private int numRecordsPerPage;

    // Statistics about the contents of the database.
    private TableStats stats;

    // The page numbers of all allocated pages which have room for more records.
    private TreeSet<Integer> freePageNums;

    // The number of records in the table.
    private long numRecords;

    // The lock context.
    private LockContext lockContext;

    // Constructors //////////////////////////////////////////////////////////////
    /**
     * Construct a brand new table named `name` with schema `schema` persisted in
     * file `filename`.
     */
    public Table(String name, Schema schema, String filename, LockContext lockContext,
                 BaseTransaction transaction) {
        this.name = name;
        this.filename = filename;
        this.schema = schema;
        this.allocator = new PageAllocator(lockContext, filename, true, transaction);
        this.bitmapSizeInBytes = computeBitmapSizeInBytes(Page.pageSize, schema);
        numRecordsPerPage = computeNumRecordsPerPage(Page.pageSize, schema);
        this.stats = new TableStats(this.schema);
        this.freePageNums = new TreeSet<Integer>();
        this.numRecords = 0;
        this.lockContext = lockContext;

        // TODO(hw5): any initialization of lock context (or none)

        writeSchemaToHeaderPage(transaction, allocator, schema);
    }

    /**
     * Load a table named `name` from the file `filename`. The schema of the
     * table will be read from the header page of the file.
     */
    public Table(String name, String filename, LockContext lockContext,
                 BaseTransaction transaction) throws DatabaseException {
        this.name = name;
        this.filename = filename;
        this.allocator = new PageAllocator(lockContext, filename, false, transaction);
        this.schema = readSchemaFromHeaderPage(transaction, this.allocator);
        this.bitmapSizeInBytes = computeBitmapSizeInBytes(Page.pageSize, this.schema);
        this.numRecordsPerPage = computeNumRecordsPerPage(Page.pageSize, this.schema);

        // We compute the stats, free pages, and number of records naively. We
        // iterate through every single data page of the file, and for each data
        // data page, we use the bitmap to read every single record.
        this.stats = new TableStats(this.schema);
        this.freePageNums = new TreeSet<Integer>();
        this.numRecords = 0;

        Iterator<Page> iter = this.allocator.iterator(transaction);
        iter.next(); // Skip the header page.
        while(iter.hasNext()) {
            Page page = iter.next();
            byte[] bitmap = getBitMap(transaction, page);

            for (short i = 0; i < numRecordsPerPage; ++i) {
                if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                    Record r = getRecord(transaction, new RecordId(page.getPageNum(), i));
                    stats.addRecord(r);
                    numRecords++;
                }
            }

            if (numRecordsOnPage(transaction, page) != numRecordsPerPage) {
                freePageNums.add(page.getPageNum());
            }
        }

        this.lockContext = lockContext;

        // TODO(hw5): any initialization of lock context (or none)
    }

    // Accessors /////////////////////////////////////////////////////////////////
    public String getName() {
        return name;
    }

    public String getFilename() {
        return filename;
    }

    public Schema getSchema() {
        return schema;
    }

    public PageAllocator getAllocator() {
        return allocator;
    }

    public int getBitmapSizeInBytes() {
        return bitmapSizeInBytes;
    }

    public int getNumRecordsPerPage() {
        return numRecordsPerPage;
    }

    public TableStats getStats() {
        return stats;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public int getNumDataPages() {
        // All pages but the first are data pages.
        return allocator.getNumPages() - 1;
    }

    // TODO(mwhittaker): This should not be public. Right now, other code
    // elsewhere reads the bitmap of tables, so we're forced to make it public.
    // We should refactor to avoid this.
    public byte[] getBitMap(BaseTransaction transaction, Page page) {
        byte[] bytes = new byte[bitmapSizeInBytes];
        page.getBuffer(transaction).get(bytes);
        return bytes;
    }

    public static int computeBitmapSizeInBytes(int pageSize, Schema schema) {
        // Dividing by 8 simultaneously (a) rounds down the number of records to a
        // multiple of 8 and (b) converts bits to bytes.
        return computeUnroundedNumRecordsPerPage(pageSize, schema) / 8;
    }

    public static int computeNumRecordsPerPage(int pageSize, Schema schema) {
        // Dividing by 8 and then multiplying by 8 rounds down to the nearest
        // multiple of 8.
        return computeUnroundedNumRecordsPerPage(pageSize, schema) / 8 * 8;
    }

    // Modifiers /////////////////////////////////////////////////////////////////
    /**
     * buildStatistics builds histograms on each of the columns of a table. Running
     * it multiple times refreshes the statistics
     */
    public TableStats buildStatistics(BaseTransaction transaction, int buckets) {
        this.stats.refreshHistograms(transaction, buckets, this);
        return this.stats;
    }

    // Modifiers /////////////////////////////////////////////////////////////////
    private synchronized void insertRecord(BaseTransaction transaction, Page page, int entryNum,
                                           Record record) {
        int offset = bitmapSizeInBytes + (entryNum * schema.getSizeInBytes());
        page.getBuffer(transaction).position(offset).put(record.toBytes(schema));
    }

    private void requestXLock(BaseTransaction transaction, Page page) {
        LockContext pageContext = lockContext.childContext(page.getPageNum());
        if (!LockType.substitutable(pageContext.getGlobalLockType(transaction), LockType.X)) {
            if (lockContext.saturation(transaction) >= 0.2 && lockContext.capacity() >= 10) {
                lockContext.escalate(transaction);
            }
            LockUtil.requestLocks(transaction, pageContext, LockType.X);
        }
    }

    /**
     * addRecord adds a record to this table and returns the record id of the
     * newly added record. stats, freePageNums, and numRecords are updated
     * accordingly. The record is added to the first free slot of the first free
     * page (if one exists, otherwise one is allocated). For example, if the
     * first free page has bitmap 0b11101000, then the record is inserted into
     * the page with index 3 and the bitmap is updated to 0b11111000.
     */
    public synchronized RecordId addRecord(BaseTransaction transaction,
                                           List<DataBox> values) throws DatabaseException {
        // TODO(hw5): modify for smarter locking
        Record record = schema.verify(values);

        // Get a free page, allocating a new one if necessary.
        if (freePageNums.isEmpty()) {
            freePageNums.add(allocator.allocPage(transaction));
        }
        Page page = allocator.fetchPage(transaction, freePageNums.first());
        requestXLock(transaction, page);

        // Find the first empty slot in the bitmap.
        // entry number of the first free slot and store it in entryNum; and (2) we
        // count the total number of entries on this page.
        byte[] bitmap = getBitMap(transaction, page);
        int entryNum = 0;
        for (; entryNum < numRecordsPerPage; ++entryNum) {
            if (Bits.getBit(bitmap, entryNum) == Bits.Bit.ZERO) {
                break;
            }
        }
        assert(entryNum < numRecordsPerPage);

        // Insert the record and update the bitmap.
        insertRecord(transaction, page, entryNum, record);
        Bits.setBit(page.getBuffer(transaction), entryNum, Bits.Bit.ONE);

        // Update the metadata.
        stats.addRecord(record);
        if (numRecordsOnPage(transaction, page) == numRecordsPerPage) {
            freePageNums.pollFirst();
        }
        numRecords++;

        return new RecordId(page.getPageNum(), (short) entryNum);
    }

    /**
     * Retrieves a record from the table, throwing an exception if no such record
     * exists.
     */
    public synchronized Record getRecord(BaseTransaction transaction,
                                         RecordId rid) throws DatabaseException {
        validateRecordId(rid);
        Page page = allocator.fetchPage(transaction, rid.getPageNum());
        byte[] bitmap = getBitMap(transaction, page);
        if (Bits.getBit(bitmap, rid.getEntryNum()) == Bits.Bit.ZERO) {
            String msg = String.format("Record %s does not exist.", rid);
            throw new DatabaseException(msg);
        }

        int offset = bitmapSizeInBytes + (rid.getEntryNum() * schema.getSizeInBytes());
        Buffer buf = page.getBuffer(transaction);
        buf.position(offset);
        return Record.fromBytes(buf, schema);
    }

    /**
     * Overwrites an existing record with new values and returns the existing
     * record. stats is updated accordingly. An exception is thrown if rid does
     * not correspond to an existing record in the table.
     */
    public synchronized Record updateRecord(BaseTransaction transaction, List<DataBox> values,
                                            RecordId rid) throws DatabaseException {
        // TODO(hw5): modify for smarter locking
        validateRecordId(rid);
        Record newRecord = schema.verify(values);

        Page page = allocator.fetchPage(transaction, rid.getPageNum());
        requestXLock(transaction, page);

        Record oldRecord = getRecord(transaction, rid);
        insertRecord(transaction, page, rid.getEntryNum(), newRecord);
        this.stats.removeRecord(oldRecord);
        this.stats.addRecord(newRecord);
        return oldRecord;
    }

    /**
     * Deletes and returns the record specified by rid from the table and updates
     * stats, freePageNums, and numRecords as necessary. An exception is thrown
     * if rid does not correspond to an existing record in the table.
     */
    public synchronized Record deleteRecord(BaseTransaction transaction,
                                            RecordId rid) throws DatabaseException {
        // TODO(hw5): modify for smarter locking
        validateRecordId(rid);
        Page page = allocator.fetchPage(transaction, rid.getPageNum());
        requestXLock(transaction, page);

        Record record = getRecord(transaction, rid);
        Bits.setBit(page.getBuffer(transaction), rid.getEntryNum(), Bits.Bit.ZERO);

        stats.removeRecord(record);
        if(numRecordsOnPage(transaction, page) == numRecordsPerPage - 1) {
            freePageNums.add(page.getPageNum());
        }
        numRecords--;

        return record;
    }

    /**
     * Frees all empty pages used by the table.
     */
    public synchronized void cleanup(BaseTransaction transaction) throws DatabaseException {
        // TODO(hw5): modify for smarter locking
        if (!LockType.substitutable(lockContext.getGlobalLockType(transaction), LockType.X)) {
            if (lockContext.saturation(transaction) >= 0.2 && lockContext.capacity() >= 10) {
                lockContext.escalate(transaction);
            }
            LockUtil.requestLocks(transaction, lockContext, LockType.X);
        }

        for (Integer pageNum : freePageNums) {
            allocator.freePage(transaction, pageNum);
        }
        freePageNums.clear();
    }

    public void close() {
        allocator.close();
    }

    // Helpers ///////////////////////////////////////////////////////////////////
    private static Schema readSchemaFromHeaderPage(BaseTransaction transaction,
            PageAllocator allocator) {
        Page headerPage = allocator.fetchPage(transaction, 0);
        Buffer buf = headerPage.getBuffer(transaction);
        return Schema.fromBytes(buf);
    }

    private static void writeSchemaToHeaderPage(BaseTransaction transaction, PageAllocator allocator,
            Schema schema) {
        Page headerPage = allocator.fetchPage(transaction, allocator.allocPage(transaction));
        assert(0 == headerPage.getPageNum());
        headerPage.getBuffer(transaction).put(schema.toBytes());
    }

    /**
     * Recall that every data page contains an m-byte bitmap followed by n
     * records. The following three functions computes m and n such that n is
     * maximized. To simplify things, we round n down to the nearest multiple of
     * 8 if necessary. m and n are stored in bitmapSizeInBytes and
     * numRecordsPerPage respectively.
     *
     * Some examples:
     *
     *   | Page Size | Record Size | bitmapSizeInBytes | numRecordsPerPage |
     *   | --------- | ----------- | ----------------- | ----------------- |
     *   | 9 bytes   | 1 byte      | 1                 | 8                 |
     *   | 10 bytes  | 1 byte      | 1                 | 8                 |
     *   ...
     *   | 17 bytes  | 1 byte      | 1                 | 8                 |
     *   | 18 bytes  | 2 byte      | 2                 | 16                |
     *   | 19 bytes  | 2 byte      | 2                 | 16                |
     */
    private static int computeUnroundedNumRecordsPerPage(int pageSize, Schema schema) {
        // Storing each record requires 1 bit for the bitmap and 8 *
        // schema.getSizeInBytes() bits for the record.
        int recordOverheadInBits = 1 + 8 * schema.getSizeInBytes();
        int pageSizeInBits = pageSize * 8;
        return pageSizeInBits / recordOverheadInBits;
    }

    private int numRecordsOnPage(BaseTransaction transaction, Page page) {
        byte[] bitmap = getBitMap(transaction, page);
        int numRecords = 0;
        for (int i = 0; i < numRecordsPerPage; ++i) {
            if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                numRecords++;
            }
        }
        return numRecords;
    }

    private void validateRecordId(RecordId rid) throws DatabaseException {
        int p = rid.getPageNum();
        int e = rid.getEntryNum();

        if (p == 0) {
            throw new DatabaseException("Page 0 is a header page, not a data page.");
        }

        if (e < 0) {
            String msg = String.format("Invalid negative entry number %d.", e);
            throw new DatabaseException(msg);
        }

        if (e >= numRecordsPerPage) {
            String msg = String.format(
                             "There are only %d records per page, but record %d was requested.",
                             numRecordsPerPage, e);
            throw new DatabaseException(msg);
        }
    }

    // Iterators /////////////////////////////////////////////////////////////////
    public TableIterator ridIterator(BaseTransaction transaction) {
        // TODO(hw5): reduce locking overhead for table scans
        if (!LockType.substitutable(lockContext.getGlobalLockType(transaction), LockType.S)) {
            LockUtil.requestLocks(transaction, lockContext, LockType.S);
        }

        return new TableIterator(transaction);
    }

    public RecordIterator iterator(BaseTransaction transaction) {
        List<RecordId> rids = getAllRecordIds(transaction);
        BacktrackingIterator<RecordId> iter = new ArrayBacktrackingIterator<RecordId>(rids.toArray(
                    new RecordId[rids.size()]));
        // Placeholder to call ridIterator anyways (since the normal version includes a call to ridIterator)
        try {
            ridIterator(transaction);
        } catch (UnsupportedOperationException e) {}
        return new RecordIterator(transaction, this, iter);
        /*
        return new RecordIterator(transaction, this, ridIterator(transaction));
        */
    }

    public BacktrackingIterator<Record> blockIterator(BaseTransaction transaction, Page[] block) {
        return new RecordIterator(transaction, this, new RIDBlockIterator(transaction, block));
    }

    public BacktrackingIterator<Record> blockIterator(BaseTransaction transaction,
            BacktrackingIterator<Page> block) {
        return new RecordIterator(transaction, this, new RIDBlockIterator(transaction, block));
    }

    public BacktrackingIterator<Record> blockIterator(BaseTransaction transaction, Iterator<Page> block,
            int maxRecords) {
        return new RecordIterator(transaction, this, new RIDBlockIterator(transaction, block, maxRecords));
    }

    /**
     * RIDPageIterator is a BacktrackingIterator over the RecordIds of a single
     * page of the table.
     *
     * See comments on the BacktrackingIterator interface for how mark and reset
     * should function.
     */
    public class RIDPageIterator implements BacktrackingIterator<RecordId> {
        //member variables go here

        /**
        * The following method signature is provided for guidance, but not necessary. Feel free to
        * implement your own solution using whatever helper methods you would like.
        */

        public RIDPageIterator(BaseTransaction transaction, Page page) {
            throw new UnsupportedOperationException("TODO(hw3): implement");
        }

        public boolean hasNext() {
            throw new UnsupportedOperationException("TODO(hw3): implement");
        }

        public RecordId next() {
            throw new UnsupportedOperationException("TODO(hw3): implement");
        }

        public void mark() {
            throw new UnsupportedOperationException("TODO(hw3): implement");
        }

        public void reset() {
            throw new UnsupportedOperationException("TODO(hw3): implement");
        }
    }

    /**
     * Helper function to create a BacktrackingIterator from an Iterator of
     * Pages, and a maximum number of pages.
     *
     * At most maxPages pages will be loaded into the iterator; if there are
     * not enough pages available, then fewer pages will be used.
     */
    private static BacktrackingIterator<Page> getBlockFromIterator(Iterator<Page> pageIter,
            int maxPages) {
        Page[] block = new Page[maxPages];
        int numPages;
        for (numPages = 0; numPages < maxPages && pageIter.hasNext(); ++numPages) {
            block[numPages] = pageIter.next();
        }
        if (numPages < maxPages) {
            Page[] temp = new Page[numPages];
            System.arraycopy(block, 0, temp, 0, numPages);
            block = temp;
        }
        return new ArrayBacktrackingIterator<>(block);
    }

    /**
     * RIDBlockIterator is a BacktrackingIterator yielding RecordIds of a block
     * of pages.
     *
     * A "block" is specified by a BacktrackingIterator of Pages: every single
     * Page returned by the iterator is part of the block. Your code should only
     * utilize this iterator's functionality for fetching pages, i.e. you should
     * *not* fetch every Page from the block iterator into an array or collection.
     *
     * The mark and reset methods have been provided for you already, and work by
     * saving a BacktrackingIterator of RecordIds over the appropriate page.
     *
     * The iterator maintains a few pieces of state:
     * - block is simply the BacktrackingIterator<Page> specifying the pages in
     *   the block.
     * - blockIter is a BacktrackingIterator over RecordIds of the current page we
     *   are iterating over.
     * - prevRecordId is the last RecordId that next() returned.
     * - nextRecordId is the next RecordId that next() will return.
     *
     * In addition to these, we maintain some state to help with the
     * implementation of mark() and reset(); you should not need to use these
     * for implementing next() and hasNext().
     */
    public class RIDBlockIterator implements BacktrackingIterator<RecordId> {
        private BacktrackingIterator<Page> block = null;
        private BacktrackingIterator<RecordId> blockIter = null;

        private BacktrackingIterator<RecordId> markedBlockIter = null;
        private RecordId markedPrevRecordId = null;

        private RecordId prevRecordId = null;
        private RecordId nextRecordId = null;

        private BaseTransaction transaction = null;

        RIDBlockIterator(BaseTransaction transaction, BacktrackingIterator<Page> block) {
            this.transaction = transaction;
            this.block = block;
            throw new UnsupportedOperationException("TODO(hw3): implement");
            //if you want to add anything to this constructor, feel free to
        }

        /**
         * This is an extra constructor that allows one to create an
         * RIDBlockIterator by taking the first maxPages of an iterator of Pages.
         *
         * If there are fewer than maxPages number of Pages available in pageIter,
         * then all remaining pages shall be used in the "block"; otherwise,
         * only the first maxPages number of pages shall be used.
         *
         * Note that this also advances pageIter by maxPages, so you can do the
         * following:
         *
         * Iterator<Page> pageIter = // ...
         * RIDBlockIterator firstBlock = new RIDBlockIterator(pageIter, 100);
         * RIDBlockIterator secondBlock = new RIDBlockIterator(pageIter, 100);
         * RIDBlockIterator thirdBlock = new RIDBlockIterator(pageIter, 100);
         *
         * to get iterators over the first 100 pages, second 100 pages, and third
         * 100 pages.
         */
        RIDBlockIterator(BaseTransaction transaction, Iterator<Page> pageIter, int maxPages) {
            this(transaction, Table.getBlockFromIterator(pageIter, maxPages));
        }

        /**
         * This is an extra constructor that allows one to create an
         * RIDBlockIterator over an array of Pages.
         *
         * Every page in the pages array will be used in the block of pages.
         */
        RIDBlockIterator(BaseTransaction transaction, Page[] pages) {
            this(transaction, new ArrayBacktrackingIterator<>(pages));
        }

        public boolean hasNext() {
            throw new UnsupportedOperationException("TODO(hw3): implement");
        }

        public RecordId next() {
            throw new UnsupportedOperationException("TODO(hw3): implement");
        }

        /**
         * Marks the last recordId returned by next().
         *
         * This implementation of mark simply marks and saves the current page's
         * iterator of RecordIds.
         */
        public void mark() {
            if (this.prevRecordId == null) {
                return;
            }

            this.block.mark();
            this.blockIter.mark();
            this.markedBlockIter = this.blockIter;
            this.markedPrevRecordId = this.prevRecordId;
        }

        /**
         * Resets to the marked recordId.
         *
         * This implementation of reset restores the marked page's iterator,
         * and calls reset() on it to move it to the correct record. Some extra
         * care is taken to ensure that we properly reset the block page iterator.
         */
        public void reset() {
            if (this.markedPrevRecordId == null) {
                return;
            }
            this.block.reset();
            // We don't want to get the current page again
            this.block.next();
            this.blockIter = this.markedBlockIter;
            this.blockIter.reset();
            // If we're at the end of the block, we don't want to repeat the record
            if (!this.block.hasNext()) {
                this.blockIter.next();
                if (this.blockIter.hasNext()) {
                    this.blockIter.reset();
                }
            }

            this.prevRecordId = null;
            this.nextRecordId = this.markedPrevRecordId;
        }
    }

    /**
     * A helper function that returns the same iterator passed in, but with
     * a single page skipped.
     */
    private static Iterator<Page> iteratorSkipPage(Iterator<Page> iter) {
        iter.next();
        return iter;
    }

    /**
     * TableIterator is an Iterator over the record IDs of a table.
     *
     * This is just a very thin wrapper around RIDBlockIterator, where the "block"
     * is an iterator of all the pages of the table (minus the header page). Once
     * RIDBlockIterator is filled in, all tests on TableIterator should
     * automatically pass.
     */
    public class TableIterator extends RIDBlockIterator {
        TableIterator(BaseTransaction transaction) {
            super(transaction, (BacktrackingIterator<Page>) Table.iteratorSkipPage(
                      Table.this.allocator.iterator(transaction)));
        }
    }

    /**
     * A helper method that returns every record id (and assumes no deletes have happened).
     */
    private List<RecordId> getAllRecordIds(BaseTransaction transaction) {
        Iterator<Page> pageIterator = Table.this.allocator.iterator(transaction);
        List<RecordId> res = new ArrayList<>();

        pageIterator.next();
        while (pageIterator.hasNext()) {
            Page page = pageIterator.next();
            for (short entryNum = 0; entryNum < Table.this.numRecordsPerPage &&
                    res.size() < numRecords; ++entryNum) {
                res.add(new RecordId(page.getPageNum(), entryNum));
            }
        }

        return res;
    }
}

