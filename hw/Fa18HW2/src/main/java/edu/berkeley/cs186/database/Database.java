package edu.berkeley.cs186.database;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;

import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.index.BPlusTree;
import edu.berkeley.cs186.database.index.BPlusTreeException;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;
import edu.berkeley.cs186.database.io.PageAllocator.PageIterator;


public class Database {
  private Map<String, Table> tableLookup;
  private Map<String, BPlusTree> indexLookup;
  private long numTransactions;
  private String fileDir;
  private LockManager lockMan;
  private int numMemoryPages;

  /**
   * Creates a new database.
   *
   * @param fileDir the directory to put the table files in
   * @throws DatabaseException
   */
  public Database(String fileDir) throws DatabaseException {
    this (fileDir, 5);
  }



  /**
   * Creates a new database.
   *
   * @param fileDir the directory to put the table files in
   * @param numMemoryPages the number of pages of memory Database Operations should use when executing Queries
   * @throws DatabaseException
   */
  public Database(String fileDir, int numMemoryPages) throws DatabaseException {
    this.numMemoryPages = numMemoryPages;
    this.fileDir = fileDir;
    numTransactions = 0;
    tableLookup = new ConcurrentHashMap<String, Table>();
    indexLookup = new ConcurrentHashMap<String, BPlusTree>();

    File dir = new File(fileDir);
    lockMan = new LockManager();

    if (!dir.exists()) {
      dir.mkdirs();
    }

    File[] files = dir.listFiles();

    for (File f : files) {
      String fName = f.getName();
      if (fName.endsWith(Table.FILENAME_EXTENSION)) {
        int lastIndex = fName.lastIndexOf(Table.FILENAME_EXTENSION);
        String tableName = fName.substring(0, lastIndex);
        tableLookup.put(tableName, new Table(tableName, f.toPath().toString()));
      } else if (fName.endsWith(BPlusTree.FILENAME_EXTENSION)) {
        int lastIndex = fName.lastIndexOf(BPlusTree.FILENAME_EXTENSION);
        String indexName = fName.substring(0, lastIndex);
        Path path = Paths.get(f.toPath().toString(), indexName + BPlusTree.FILENAME_EXTENSION);
        indexLookup.put(indexName, new BPlusTree(path.toString()));
      }
    }
  }


  /**
   * Create a new table in this database.
   *
   * @param s the table schema
   * @param tableName the name of the table
   * @throws DatabaseException
   */
  public synchronized void createTable(Schema s, String tableName) throws DatabaseException {
    if (this.tableLookup.containsKey(tableName)) {
      throw new DatabaseException("Table name already exists");
    }

    Path path = Paths.get(fileDir, tableName + Table.FILENAME_EXTENSION);
    this.tableLookup.put(tableName, new Table(tableName, s, path.toString()));
  }

  /**
   * Create a new table in this database with an index on each of the given column names.
   * NOTE: YOU CAN NOT DELETE/UPDATE FROM THIS TABLE IF YOU CHOOSE TO BUILD INDICES!!
   * @param s the table schema
   * @param tableName the name of the table
   * @param indexColumns the list of unique columnNames on the maintain an index on
   * @throws DatabaseException
   */
  public synchronized void createTableWithIndices(Schema s, String tableName, List<String> indexColumns) throws DatabaseException {
    if (this.tableLookup.containsKey(tableName)) {
      throw new DatabaseException("Table name already exists");
    }

    List<String> schemaColNames = s.getFieldNames();
    List<Type> schemaColType = s.getFieldTypes();

    HashSet<String> seenColNames = new HashSet<String>();
    List<Integer> schemaColIndex = new ArrayList<Integer>();
    for (int i = 0; i < indexColumns.size(); i++) {
      String col = indexColumns.get(i);
      if (!schemaColNames.contains(col)) {
        throw new DatabaseException("Column desired for index does not exist");
      }
      if (seenColNames.contains(col)) {
        throw new DatabaseException("Column desired for index has been duplicated");
      }
      seenColNames.add(col);
      schemaColIndex.add(schemaColNames.indexOf(col));
    }

    Path path = Paths.get(fileDir, tableName + Table.FILENAME_EXTENSION);
    this.tableLookup.put(tableName, new Table(tableName, s, path.toString()));
    for (int i : schemaColIndex) {
      String colName = schemaColNames.get(i);
      Type colType = schemaColType.get(i);
      String indexName = tableName + "," + colName;
      Path p = Paths.get(this.fileDir, indexName + BPlusTree.FILENAME_EXTENSION);
      try {
      this.indexLookup.put(indexName, new BPlusTree(p.toString(), colType,
                           BPlusTree.maxOrder(Page.pageSize, colType)));
      } catch (BPlusTreeException e) {
        throw new DatabaseException(e.getMessage());
      }
    }
  }

  /**
   * Delete a table in this database.
   *
   * @param tableName the name of the table
   * @return true if the database was successfully deleted
   */
  public synchronized boolean deleteTable(String tableName) {
    if (!this.tableLookup.containsKey(tableName)) {
      return false;
    }

    this.tableLookup.get(tableName).close();
    this.tableLookup.remove(tableName);

    File f = new File(fileDir + tableName + Table.FILENAME_EXTENSION);
    f.delete();

    return true;
  }

  /**
   * Delete all tables from this database.
   */
  public synchronized void deleteAllTables() {
    List<String> tableNames = new ArrayList<String>(tableLookup.keySet());

    for (String s : tableNames) {
      deleteTable(s);
    }
  }

  /**
   * Close this database.
   */
  public synchronized void close() {
    for (Table t : this.tableLookup.values()) {
      t.close();
    }

    this.tableLookup.clear();
  }

  public Table getTable(String tableName) {
    return tableLookup.get(tableName);
  }

  /**
   * Start a new transaction.
   *
   * @return the new Transaction
   */
  public synchronized Transaction beginTransaction() {
    Transaction t = new Transaction(this.numTransactions);

    this.numTransactions++;
    return t;
  }

  /**
   * Start a new transaction.
   *
   * @return the new Transaction
   */
  public synchronized AtomicTransaction createAtomicTransaction() {
    AtomicTransaction t = new AtomicTransaction(this.numTransactions);
    return t;
  }


  public class Transaction {
    long transNum;
    boolean active;
    HashMap<String, Table> tempTables;
    HashMap<String, String> aliasMaps;
    long tempTableCounter;

    private Transaction(long tNum) {
      this.transNum = tNum;
      this.active = true;
      this.tempTables = new HashMap<String, Table>();
      this.aliasMaps = new HashMap<String, String>();
      this.tempTableCounter = 0;
    }

    public boolean isActive() {
      return this.active;
    }

    public void end(){
      assert(this.active);

      deleteAllTempTables();
      this.active = false;

    }

    /**
     * Allows the user to query a table. See query#QueryPlan
     *
     * @param tableName The name/alias of the table wished to be queried.
     * @throws DatabaseException if table does not exist
     */
    public QueryPlan query(String tableName) throws DatabaseException {
      assert(this.active);
      return new QueryPlan(this, tableName);
    }

    /**
     * Allows the user to provide an alias for a particular table. That alias is valid for the
     * remainder of the transaction. For a particular QueryPlan, once you specify an alias, you
     * must use that alias for the rest of the query.
     *
     * @param tableName The original name of the table.
     * @param alias The new Aliased name.
     * @throws DatabaseException if the alias already exists or the table does not.
     */
    public void queryAs(String tableName, String alias) throws DatabaseException {
      assert(this.active);

      if (Database.this.tableLookup.containsKey(alias)
              || this.tempTables.containsKey(alias)
              || this.aliasMaps.containsKey(alias)) {
        throw new DatabaseException("Table name already exists");
      }

      if (Database.this.tableLookup.containsKey(tableName)) {
        this.aliasMaps.put(alias, tableName);
      } else if (tempTables.containsKey(tableName)) {
        this.aliasMaps.put(alias, tableName);
      } else {
        throw new DatabaseException("Table name not found");
      }
    }

    /**
     * Create a temporary table within this transaction.
     *
     * @param schema the table schema
     * @throws DatabaseException
     * @return name of the tempTable
     */
    public String createTempTable(Schema schema) throws DatabaseException {
      assert(this.active);

      String tempTableName = "tempTable" + tempTableCounter;
      tempTableCounter++;

      Path dir = Paths.get(Database.this.fileDir, "temp");
      File f = new File(dir.toAbsolutePath().toString());
      if (!f.exists()) {
        f.mkdirs();
      }

      Path path = Paths.get(Database.this.fileDir, "temp", tempTableName + Table.FILENAME_EXTENSION);
      this.tempTables.put(tempTableName, new Table(tempTableName, schema, path.toString()));
      return tempTableName;
    }


    /**
     * Create a temporary table within this transaction.
     *
     * @param schema the table schema
     * @param tempTableName the name of the table
     * @throws DatabaseException
     */
    public void createTempTable(Schema schema, String tempTableName) throws DatabaseException {
      assert(this.active);

      if (Database.this.tableLookup.containsKey(tempTableName)
              || this.tempTables.containsKey(tempTableName))  {
        throw new DatabaseException("Table name already exists");
      }

      Path dir = Paths.get(Database.this.fileDir, "temp");
      File f = new File(dir.toAbsolutePath().toString());
      if (!f.exists()) {
        f.mkdirs();
      }

      Path path = Paths.get(Database.this.fileDir, "temp", tempTableName + Table.FILENAME_EXTENSION);
      this.tempTables.put(tempTableName, new Table(tempTableName, schema, path.toString()));
    }


    /**
     * Perform a check to see if the database has an index on this (table,column).
     *
     * @param tableName the name of the table
     * @param columnName the name of the column
     * @return boolean if the index exists
     */
    public boolean indexExists(String tableName, String columnName) {
      try {
        resolveIndexFromName(tableName, columnName);
      } catch (DatabaseException e) {
        return false;
      }
      return true;
    }

    public Iterator<Record> sortedScan(String tableName, String columnName) throws DatabaseException {
      Table tab = getTable(tableName);
      BPlusTree index = resolveIndexFromName(tableName, columnName);
      return new RecordIterator(tab, index.scanAll());
    }

    public Iterator<Record> sortedScanFrom(String tableName, String columnName, DataBox startValue) throws DatabaseException {
      Table tab = getTable(tableName);
      BPlusTree index = resolveIndexFromName(tableName, columnName);
      return new RecordIterator(tab, index.scanGreaterEqual(startValue));
    }

    public Iterator<Record> lookupKey(String tableName, String columnName, DataBox key) throws DatabaseException {
      Table tab = getTable(tableName);
      BPlusTree index = resolveIndexFromName(tableName, columnName);
      return new RecordIterator(tab, index.scanEqual(key));
    }

    public boolean contains(String tableName, String columnName, DataBox key) throws DatabaseException {
      BPlusTree index = resolveIndexFromName(tableName, columnName);
      return index.get(key).isPresent();
    }

    public RecordId addRecord(String tableName, List<DataBox> values) throws DatabaseException {
      assert(this.active);
        return runAddRecord(tableName, values);
    }

    private RecordId runAddRecord(String tableName, List<DataBox> values) throws DatabaseException {
      assert(this.active);
      Table tab = getTable(tableName);
      RecordId rid = tab.addRecord(values);
      Schema s = tab.getSchema();
      List<String> colNames = s.getFieldNames();

      for (int i = 0; i < colNames.size(); i++) {
        String col = colNames.get(i);
        if (indexExists(tableName, col)) {
          try {
            resolveIndexFromName(tableName, col).put(values.get(i), rid);
          } catch (BPlusTreeException e) {
            throw new DatabaseException(e.getMessage());
          }
        }
      }

      //find(tableName, "string");

      return rid;
    }


    public int getNumMemoryPages() throws DatabaseException {
      assert(this.active);

      return Database.this.numMemoryPages;

    }


    public RecordId deleteRecord(String tableName, RecordId rid)  throws DatabaseException {
        return runDeleteRecord(tableName, rid);
    }

    private RecordId runDeleteRecord(String tableName, RecordId rid) throws DatabaseException {
      assert(active);

      Table tab = getTable(tableName);
      Schema s = tab.getSchema();

      Record rec = tab.deleteRecord(rid);
      List<DataBox> values = rec.getValues();
      List<String> colNames = s.getFieldNames();
      for (int i = 0; i < colNames.size(); i++) {
        String col = colNames.get(i);
        if (indexExists(tableName, col)) {
          resolveIndexFromName(tableName, col).remove(values.get(i));
        }
      }

      return rid;
    }

    public Record getRecord(String tableName, RecordId rid) throws DatabaseException {
      assert(active);
      return getTable(tableName).getRecord(rid);
    }

    public RecordIterator getRecordIterator(String tableName) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).iterator();
    }

    public RecordId updateRecord(String tableName, List<DataBox> values, RecordId rid)  throws DatabaseException {
        return runUpdateRecord(tableName, values, rid);
    }

    public PageIterator getPageIterator(String tableName) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).getAllocator().iterator();
    }
    

    public BacktrackingIterator<Record> getBlockIterator(String tableName, Page[] block) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).blockIterator(block);
    }

    public BacktrackingIterator<Record> getBlockIterator(String tableName, BacktrackingIterator<Page> block) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).blockIterator(block);
    }

    public BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block, int maxPages) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).blockIterator(block, maxPages);
    }


    public RecordId runUpdateRecordWhere(String tableName, String targetColumnName, DataBox targetVaue, String predColumnName, DataBox predValue)  throws DatabaseException {

        Table tab = getTable(tableName);
        Iterator<RecordId> recordIds = tab.ridIterator();

        Schema s = tab.getSchema();
        int uindex = s.getFieldNames().indexOf(targetColumnName);
        int pindex = s.getFieldNames().indexOf(predColumnName);

        while(recordIds.hasNext()) {
          RecordId curRID = recordIds.next();
          Record cur = getRecord(tableName, curRID);
          List<DataBox> record_copy = new ArrayList<DataBox>(cur.getValues());

          if (record_copy.get(pindex).equals(predValue)){
              record_copy.set(uindex, targetVaue);
              runUpdateRecord(tableName, record_copy, curRID);
          }
        }

        return null;
    }


    private RecordId runUpdateRecord(String tableName, List<DataBox> values, RecordId rid) throws DatabaseException {
      assert(this.active);
      Table tab = getTable(tableName);
      Schema s = tab.getSchema();

      Record rec = tab.updateRecord(values, rid);

      List<DataBox> oldValues = rec.getValues();
      List<String> colNames = s.getFieldNames();

      for (int i = 0; i < colNames.size(); i++) {
        String col = colNames.get(i);
        if (indexExists(tableName, col)) {
          BPlusTree tree = resolveIndexFromName(tableName, col);
          tree.remove(oldValues.get(i));
          try {
            tree.put(values.get(i), rid);
          } catch (BPlusTreeException e) {
            throw new DatabaseException(e.getMessage());
          }
        }
      }

      return rid;
    }


    public TableStats getStats(String tableName) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).getStats();
    }

    public int getNumDataPages(String tableName) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).getNumDataPages();
    }

    public int getNumEntriesPerPage(String tableName) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).getNumRecordsPerPage();
    }

    public byte[] readPageHeader(String tableName, Page p) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).getBitMap(p);
    }

    public int getPageHeaderSize(String tableName) throws DatabaseException{
      assert(this.active);
      return getTable(tableName).getBitmapSizeInBytes ();
    }

    public int getEntrySize(String tableName) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).getSchema().getSizeInBytes();
    }

    public long getNumRecords(String tableName) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).getNumRecords();
    }

    public int getNumIndexPages(String tableName, String columnName) throws DatabaseException {
      assert(this.active);
      return this.resolveIndexFromName(tableName, columnName).getNumPages();
    }

    public Schema getSchema(String tableName) throws DatabaseException {
      assert(this.active);
      return getTable(tableName).getSchema();
    }

    public Schema getFullyQualifiedSchema(String tableName) throws DatabaseException {
      assert(this.active);

      Schema schema = getTable(tableName).getSchema();

      List<String> newColumnNames = new ArrayList<String>();

      for (String oldName : schema.getFieldNames()) {
        newColumnNames.add(tableName + "." + oldName);
      }

      return new Schema(newColumnNames, schema.getFieldTypes());
    }

    private BPlusTree resolveIndexFromName(String tableName, String columnName) throws DatabaseException {
      while (aliasMaps.containsKey(tableName)) {
        tableName = aliasMaps.get(tableName);
      }
      if (columnName.contains(".")) {
        String columnPrefix = columnName.split("\\.")[0];
        while (aliasMaps.containsKey(columnPrefix)) {
          columnPrefix = aliasMaps.get(columnPrefix);
        }
        if (!tableName.equals(columnPrefix)) {
          throw new DatabaseException("Column: " + columnName + " is not a column of " + tableName);
        }
        columnName = columnName.split("\\.")[1];
      }
      String indexName = tableName + "," + columnName;
      if (Database.this.indexLookup.containsKey(indexName)) {
        return Database.this.indexLookup.get(indexName);
      }
      throw new DatabaseException("Index does not exist");
    }

    private Table getTable(String tableName) throws DatabaseException {
      if (this.tempTables.containsKey(tableName)) {
        return this.tempTables.get(tableName);
      }

      while (aliasMaps.containsKey(tableName)) {
        tableName = aliasMaps.get(tableName);
      }

      if (!Database.this.tableLookup.containsKey(tableName)) {
        throw new DatabaseException("Table: " + tableName + "does not exist");
      }

      return Database.this.tableLookup.get(tableName);
    }


    public void deleteTempTable(String tempTableName) {
      assert(this.active);

      if (!this.tempTables.containsKey(tempTableName)) {
        return;
      }

      this.tempTables.get(tempTableName).close();
      Database.this.tableLookup.remove(tempTableName);

      File f = new File(Database.this.fileDir + "temp/" + tempTableName + Table.FILENAME_EXTENSION);
      f.delete();
    }

    private void deleteAllTempTables() {
      Set<String> keys = tempTables.keySet();

      for (String tableName : keys) {
        deleteTempTable(tableName);
      }
    }
  }


  public class AtomicTransaction extends Transaction implements Runnable {

    LinkedList<Operation> operationList;

    private class Operation {
        public RecordId rid;
        public String tableName;
        public List<DataBox> values;
        public int type;

        public String targetColumnName;
        public DataBox targetVaue;
        public String predColumnName;
        public DataBox predValue;

        public static final int ADD = 0;
        public static final int DELETE = 1;
        public static final int UPDATE = 2;
        public static final int UPDATE_WHERE = 3;
    }



    private AtomicTransaction(long tNum) {
      super(tNum);
      this.operationList = new LinkedList<Operation>();
    }


    public void run(){
      assert(this.active);

      LinkedList<RecordId> newRecords =  new LinkedList<RecordId>();

      for (Operation op : this.operationList){



          try{
              switch (op.type) {
                case Operation.ADD: newRecords.add(super.runAddRecord(op.tableName, op.values)); break;
                case Operation.DELETE: newRecords.add(super.runDeleteRecord(op.tableName, op.rid)); break;
                case Operation.UPDATE: newRecords.add(super.runUpdateRecord(op.tableName, op.values, op.rid)); break;
                case Operation.UPDATE_WHERE: newRecords.add(super.runUpdateRecordWhere(op.tableName, op.targetColumnName, op.targetVaue, op.predColumnName, op.predValue)); break;
                default: continue;
              }
          }
          catch(DatabaseException d)
          {
            //do something here
          }

      }

      super.deleteAllTempTables();
      this.active = false;
    }

    //stores the operation for execution in the future
    public RecordId addRecord(String tableName, List<DataBox> values)  throws DatabaseException {
        Operation op = new Operation();
        op.tableName = tableName;
        op.values = values;
        op.type = Operation.ADD;
        this.operationList.add(op);
        return null;
    }


    //stores the operation for execution in the future
    public RecordId deleteRecord(String tableName, RecordId rid)  throws DatabaseException {
        Operation op = new Operation();
        op.tableName = tableName;
        op.rid = rid;
        op.type = Operation.DELETE;
        this.operationList.add(op);
        return null;
    }

    //stores the operation for execution in the future
    public RecordId updateRecord(String tableName, List<DataBox> values, RecordId rid)  throws DatabaseException {
        Operation op = new Operation();
        op.tableName = tableName;
        op.rid = rid;
        op.values = values;
        op.type = Operation.UPDATE;
        this.operationList.add(op);
        return null;
    }

        //stores the operation for execution in the future
    public RecordId updateRecordWhere(String tableName, String targetColumnName, DataBox targetVaue, String predColumnName, DataBox predValue)  throws DatabaseException {
        Operation op = new Operation();
        op.tableName = tableName;
        op.targetColumnName = targetColumnName;
        op.targetVaue = targetVaue;
        op.predColumnName = predColumnName;
        op.predValue = predValue;
        op.type = Operation.UPDATE_WHERE;
        this.operationList.add(op);
        return null;
    }

  }

}
