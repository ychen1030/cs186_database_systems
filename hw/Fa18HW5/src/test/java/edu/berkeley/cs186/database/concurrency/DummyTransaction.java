package edu.berkeley.cs186.database.concurrency;

import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.LoggingLockManager;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator.PageIterator;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

/**
 * A dummy transaction class that only supports checking/setting active/blocked
 * status. Used for testing locking code without requiring an instance
 * of the database.
 */
public class DummyTransaction implements BaseTransaction {
    private long tNum;
    private boolean active;
    private boolean blocked;
    private LoggingLockManager lockManager;

    public DummyTransaction(LoggingLockManager lockManager, long tNum) {
        this.lockManager = lockManager;
        this.tNum = tNum;
        this.active = true;
        this.blocked = false;
    }

    public long getTransNum() {
        return this.tNum;
    }

    public boolean isActive() {
        return this.active;
    }

    public void end() {
        this.active = false;
    }

    public void createTable(Schema s, String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public void createTableWithIndices(Schema s, String tableName,
                                       List<String> indexColumns) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public boolean deleteTable(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public void deleteAllTables() {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public QueryPlan query(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public void queryAs(String tableName, String alias) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public String createTempTable(Schema schema) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public void createTempTable(Schema schema, String tempTableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public boolean indexExists(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public Iterator<Record> sortedScan(String tableName, String columnName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public Iterator<Record> sortedScanFrom(String tableName, String columnName,
                                           DataBox startValue) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public Iterator<Record> lookupKey(String tableName, String columnName,
                                      DataBox key) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public boolean contains(String tableName, String columnName, DataBox key) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public RecordId addRecord(String tableName, List<DataBox> values) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public int getNumMemoryPages() throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public RecordId deleteRecord(String tableName, RecordId rid)  throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public Record getRecord(String tableName, RecordId rid) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public RecordIterator getRecordIterator(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public RecordId updateRecord(String tableName, List<DataBox> values,
                                 RecordId rid)  throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public PageIterator getPageIterator(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public BacktrackingIterator<Record> getBlockIterator(String tableName,
            Page[] block) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public BacktrackingIterator<Record> getBlockIterator(String tableName,
            BacktrackingIterator<Page> block) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block,
            int maxPages) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public RecordId runUpdateRecordWhere(String tableName, String targetColumnName, DataBox targetVaue,
                                         String predColumnName, DataBox predValue)  throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public TableStats getStats(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public int getNumDataPages(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public int getNumEntriesPerPage(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public byte[] readPageHeader(String tableName, Page p) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public int getPageHeaderSize(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public int getEntrySize(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public long getNumRecords(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public int getNumIndexPages(String tableName, String columnName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public Schema getSchema(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public Schema getFullyQualifiedSchema(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public void deleteTempTable(String tempTableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public void block() {
        this.blocked = true;
        lockManager.emit("block " + tNum);
    }

    public void unblock() {
        this.blocked = false;
        lockManager.emit("unblock " + tNum);
    }

    public boolean getBlocked() {
        return this.blocked;
    }

    @Override
    public String toString() {
        return "Dummy Transaction #" + tNum;
    }
}

