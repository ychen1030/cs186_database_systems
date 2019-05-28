package edu.berkeley.cs186.database;

import java.util.Iterator;
import java.util.List;

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

public interface BaseTransaction {
    long getTransNum();
    boolean isActive();
    void end();

    /**
     * Create a new table in this database.
     *
     * @param s the table schema
     * @param tableName the name of the table
     * @throws DatabaseException
     */
    void createTable(Schema s, String tableName) throws DatabaseException;

    /**
     * Create a new table in this database with an index on each of the given column names.
     * @param s the table schema
     * @param tableName the name of the table
     * @param indexColumns the list of unique columnNames on the maintain an index on
     * @throws DatabaseException
     */
    void createTableWithIndices(Schema s, String tableName,
                                List<String> indexColumns) throws DatabaseException;

    /**
     * Delete a table in this database.
     *
     * @param tableName the name of the table
     * @return true if the database was successfully deleted
     */
    boolean deleteTable(String tableName);

    /**
     * Delete all tables from this database.
     */
    void deleteAllTables();

    /**
     * Allows the user to query a table. See query#QueryPlan
     *
     * @param tableName The name/alias of the table wished to be queried.
     * @throws DatabaseException if table does not exist
     */
    QueryPlan query(String tableName) throws DatabaseException;

    /**
     * Allows the user to provide an alias for a particular table. That alias is valid for the
     * remainder of the transaction. For a particular QueryPlan, once you specify an alias, you
     * must use that alias for the rest of the query.
     *
     * @param tableName The original name of the table.
     * @param alias The new Aliased name.
     * @throws DatabaseException if the alias already exists or the table does not.
     */
    void queryAs(String tableName, String alias) throws DatabaseException;

    /**
     * Create a temporary table within this transaction.
     *
     * @param schema the table schema
     * @throws DatabaseException
     * @return name of the tempTable
     */
    String createTempTable(Schema schema) throws DatabaseException;

    /**
     * Create a temporary table within this transaction.
     *
     * @param schema the table schema
     * @param tempTableName the name of the table
     * @throws DatabaseException
     */
    void createTempTable(Schema schema, String tempTableName) throws DatabaseException;

    /**
     * Perform a check to see if the database has an index on this (table,column).
     *
     * @param tableName the name of the table
     * @param columnName the name of the column
     * @return boolean if the index exists
     */
    boolean indexExists(String tableName, String columnName);

    Iterator<Record> sortedScan(String tableName, String columnName) throws DatabaseException;

    Iterator<Record> sortedScanFrom(String tableName, String columnName,
                                    DataBox startValue) throws DatabaseException;

    Iterator<Record> lookupKey(String tableName, String columnName,
                               DataBox key) throws DatabaseException;

    boolean contains(String tableName, String columnName, DataBox key) throws DatabaseException;

    RecordId addRecord(String tableName, List<DataBox> values) throws DatabaseException;

    int getNumMemoryPages() throws DatabaseException;

    RecordId deleteRecord(String tableName, RecordId rid)  throws DatabaseException;

    Record getRecord(String tableName, RecordId rid) throws DatabaseException;

    RecordIterator getRecordIterator(String tableName) throws DatabaseException;

    RecordId updateRecord(String tableName, List<DataBox> values,
                          RecordId rid)  throws DatabaseException;

    PageIterator getPageIterator(String tableName) throws DatabaseException;

    BacktrackingIterator<Record> getBlockIterator(String tableName,
            Page[] block) throws DatabaseException;

    BacktrackingIterator<Record> getBlockIterator(String tableName,
            BacktrackingIterator<Page> block) throws DatabaseException;

    BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block,
            int maxPages) throws DatabaseException;

    RecordId runUpdateRecordWhere(String tableName, String targetColumnName, DataBox targetVaue,
                                  String predColumnName, DataBox predValue)  throws DatabaseException;

    TableStats getStats(String tableName) throws DatabaseException;

    int getNumDataPages(String tableName) throws DatabaseException;

    int getNumEntriesPerPage(String tableName) throws DatabaseException;

    byte[] readPageHeader(String tableName, Page p) throws DatabaseException;

    int getPageHeaderSize(String tableName) throws DatabaseException;

    int getEntrySize(String tableName) throws DatabaseException;

    long getNumRecords(String tableName) throws DatabaseException;

    int getNumIndexPages(String tableName, String columnName) throws DatabaseException;

    Schema getSchema(String tableName) throws DatabaseException;

    Schema getFullyQualifiedSchema(String tableName) throws DatabaseException;

    void deleteTempTable(String tempTableName);

    void block();

    void unblock();

    boolean getBlocked();
}

