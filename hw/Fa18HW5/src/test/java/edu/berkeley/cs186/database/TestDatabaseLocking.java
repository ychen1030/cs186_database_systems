package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.index.BPlusTree;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.QueryPlanException;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.*;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import javax.xml.crypto.Data;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.*;

public class TestDatabaseLocking {
    public static final String TestDir = "testDatabaseLocking";
    private Database db;
    private LoggingLockManager lockManager;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.seconds(10));

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.lockManager = new LoggingLockManager();
        this.db = new Database(filename, 5, lockManager);
        BaseTransaction t = this.db.beginTransaction();
        t.deleteAllTables();
        t.end();
    }

    @After
    public void afterEach() {
        this.lockManager.endLog();
        BaseTransaction t = this.db.beginTransaction();
        t.deleteAllTables();
        t.end();
        this.db.close();
    }

    private List<RecordId> createTable(String tableName, int pages) throws DatabaseException {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();
        List<DataBox> values = input.getValues();

        BaseTransaction t1 = db.beginTransaction();

        t1.createTable(s, tableName);
        int numRecords = pages * db.getTable(tableName).getNumRecordsPerPage();
        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < numRecords; ++i) {
            rids.add(t1.addRecord(tableName, values));
        }

        t1.end();

        return rids;
    }

    private List<RecordId> createTableWithIndices(String tableName, int pages,
            List<String> indexColumns) throws DatabaseException {
        return createTableWithIndices(tableName, pages, indexColumns, true);
    }

    private List<RecordId> createTableWithIndices(String tableName, int pages,
            List<String> indexColumns,
            boolean disableChildLocks) throws DatabaseException {
        if (!disableChildLocks) {
            for (String indexCol : indexColumns) {
                toggleIndexDisableChildLocking(tableName + "," + indexCol, false);
            }
        }

        Schema s = TestUtils.createSchemaWithTwoInts();
        BaseTransaction t1 = db.beginTransaction();

        t1.createTableWithIndices(s, tableName, indexColumns);
        int numRecords = pages * db.getTable(tableName).getNumRecordsPerPage();
        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < numRecords; ++i) {
            rids.add(t1.addRecord(tableName, Arrays.asList(new IntDataBox(i), new IntDataBox(i))));
        }

        t1.end();

        if (!disableChildLocks) {
            for (String indexCol : indexColumns) {
                toggleIndexDisableChildLocking(tableName + "," + indexCol, true);
            }
            if (lockManager.isLogging()) {
                List<String> oldLog = lockManager.log;
                lockManager = new LoggingLockManager();
                lockManager.log = oldLog;
                lockManager.startLog();
            } else {
                lockManager = new LoggingLockManager();
            }
            db.close();
            db = new Database(filename, 5, lockManager);
        }

        return rids;
    }

    // Turns on/off disableChildLocking to allow some tests
    // to set things up and run before all locking integration has been completed.
    private void toggleIndexDisableChildLocking(String indexName, boolean enable) {
        try {
            Method getIndexContext = Database.class.getDeclaredMethod("getIndexContext", String.class);
            getIndexContext.setAccessible(true);
            LoggingLockContext indexContext = (LoggingLockContext) getIndexContext.invoke(db, indexName);
            getIndexContext.setAccessible(false);
            indexContext.allowDisableChildLocks(enable);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRecordRead() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        t1.getRecord(tableName, rids.get(0));
        t1.getRecord(tableName, rids.get(3 * rids.size() / 4 - 1));
        t1.getRecord(tableName, rids.get(rids.size() - 1));

        assertEquals(Arrays.asList(
                         "acquire 2 database IS",
                         "acquire 2 database/table-testTable1 IS",
                         "acquire 2 database/table-testTable1/1 S",
                         "acquire 2 database/table-testTable1/3 S",
                         "acquire 2 database/table-testTable1/4 S"
                     ), lockManager.log);
    }

    @Test
    public void testTableCleanup() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        db.getTable(tableName).cleanup(t1);

        assertEquals(Arrays.asList(
                         "acquire 2 database IX",
                         "acquire 2 database/table-testTable1 X"
                     ), lockManager.log.subList(0, 2));
    }

    @Test
    public void testSimpleTransactionCleanup() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        BaseTransaction t1 = db.beginTransaction();
        t1.getRecord(tableName, rids.get(0));
        t1.getRecord(tableName, rids.get(3 * rids.size() / 4 - 1));
        t1.getRecord(tableName, rids.get(rids.size() - 1));

        assertEquals("did not acquire all required locks", 5, lockManager.getLocks(t1).size());
        t1.end();
        assertTrue("did not free all required locks", lockManager.getLocks(t1).isEmpty());
    }

    @Test
    public void testRecordWrite() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        Record input = TestUtils.createRecordWithAllTypes();
        List<DataBox> values = input.getValues();

        BaseTransaction t0 = db.beginTransaction();
        t0.deleteRecord(tableName, rids.get(rids.size() - 1));
        t0.end();

        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        t1.addRecord(tableName, values);

        assertEquals(Arrays.asList(
                         "acquire 3 database IX",
                         "acquire 3 database/table-testTable1 IX",
                         "acquire 3 database/table-testTable1/4 X"
                     ), lockManager.log);
    }

    @Test
    public void testRecordUpdate() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        Record input = TestUtils.createRecordWithAllTypes();
        List<DataBox> values = input.getValues();

        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        t1.updateRecord(tableName, values, rids.get(rids.size() - 1));

        assertEquals(Arrays.asList(
                         "acquire 2 database IX",
                         "acquire 2 database/table-testTable1 IX",
                         "acquire 2 database/table-testTable1/4 X"
                     ), lockManager.log);
    }

    @Test
    public void testRecordDelete() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        t1.deleteRecord(tableName, rids.get(rids.size() - 1));

        assertEquals(Arrays.asList(
                         "acquire 2 database IX",
                         "acquire 2 database/table-testTable1 IX",
                         "acquire 2 database/table-testTable1/4 X"
                     ), lockManager.log);
    }

    @Test
    public void testTableScan() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        Iterator<Record> r = t1.getRecordIterator(tableName);
        while (r.hasNext()) {
            r.next();
        }

        assertEquals(Arrays.asList(
                         "acquire 2 database IS",
                         "acquire 2 database/table-testTable1 S"
                     ), lockManager.log);
    }

    @Test
    public void testSortedScanNoIndexLocking() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 1);

        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        Iterator<Record> r = t1.sortedScan(tableName, "int");
        while (r.hasNext()) {
            r.next();
        }

        assertEquals(Arrays.asList(
                         "acquire 2 database IS",
                         "acquire 2 database/table-testTable1 S"
                     ), lockManager.log.subList(0, 2));
    }

    @Test
    public void testBPlusTreeRestrict() throws DatabaseException {
        String tableName = "testTable1";
        lockManager.startLog();

        createTableWithIndices(tableName, 0, Arrays.asList("int1"), false);
        assertTrue(lockManager.log.contains("disable-children database/index-testTable1,int1"));
    }

    @Test
    public void testSortedScanLocking() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTableWithIndices(tableName, 1, Arrays.asList("int1", "int2"), false);
        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        Iterator<Record> r = t1.sortedScan(tableName, "int1");
        while (r.hasNext()) {
            r.next();
        }
        BaseTransaction t2 = db.beginTransaction();
        r = t2.sortedScanFrom(tableName, "int2", new IntDataBox(rids.size() / 2));
        while (r.hasNext()) {
            r.next();
        }

        try {
            assertEquals(Arrays.asList(
                             "acquire 0 database IS",
                             "acquire 0 database/table-testTable1 S",
                             "acquire 0 database/index-testTable1,int1 S",
                             "acquire 1 database IS",
                             "acquire 1 database/table-testTable1 S",
                             "acquire 1 database/index-testTable1,int2 S"
                         ), lockManager.log);
        } catch (AssertionError e) {
            assertEquals(Arrays.asList(
                             "acquire 0 database IS",
                             "acquire 0 database/index-testTable1,int1 S",
                             "acquire 0 database/table-testTable1 S",
                             "acquire 1 database IS",
                             "acquire 1 database/index-testTable1,int2 S",
                             "acquire 1 database/table-testTable1 S"
                         ), lockManager.log);
        }
    }

    @Test
    public void testSearchOperationLocking() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTableWithIndices(tableName, 1, Arrays.asList("int1", "int2"), false);
        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        t1.lookupKey(tableName, "int1", new IntDataBox(rids.size() / 2));
        BaseTransaction t2 = db.beginTransaction();
        t2.contains(tableName, "int2", new IntDataBox(rids.size() / 2 - 1));

        assertEquals(Arrays.asList(
                         "acquire 0 database IS",
                         "acquire 0 database/index-testTable1,int1 S",
                         "acquire 1 database IS",
                         "acquire 1 database/index-testTable1,int2 S"
                     ), lockManager.log);
    }

    @Test
    public void testQueryWithIndex() throws DatabaseException, QueryPlanException {
        String tableName = "testTable1";
        createTableWithIndices(tableName, 2, Arrays.asList("int1", "int2"), false);

        BaseTransaction t0 = db.beginTransaction();
        /*
        // This line only needs to be called if you have implemented Histogram and uncommented the
        // calls to estimateStats/estimateIOCost in the various operators.
        db.getTable("testTable1").buildStatistics(t0, 10);
        */
        t0.end();

        lockManager.startLog();

        BaseTransaction t1 = db.beginTransaction();
        QueryPlan q = t1.query(tableName);
        q.select("int1", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(2));
        q.project(Arrays.asList("int2"));
        Iterator<Record> iter = q.execute();

        assertEquals(Arrays.asList(
                         "acquire 1 database IS",
                         "acquire 1 database/index-testTable1,int1 S"
                     ), lockManager.log);

        while(iter.hasNext()) {
            iter.next();
        }

        assertEquals(Arrays.asList(
                         "acquire 1 database IS",
                         "acquire 1 database/index-testTable1,int1 S",
                         "acquire 1 database/table-testTable1 IS",
                         "acquire 1 database/table-testTable1/1 S"
                     ), lockManager.log);
    }

    @Test
    public void testTempTable() throws DatabaseException {
        lockManager.startLog();

        BaseTransaction t0 = db.beginTransaction();
        String tableName = t0.createTempTable(TestUtils.createSchemaWithAllTypes());

        assertTrue(lockManager.log.contains("disable-children temp-" + tableName));
        assertTrue(LockType.substitutable(lockManager.orphanContext("temp-" + tableName).getLocalLockType(
                                              t0), LockType.S));
    }

    @Test
    public void testPageAllocatorInitCapacity() throws DatabaseException {
        String tableName = "testTable1";
        createTable(tableName, 0);
        db.close();
        this.lockManager = new LoggingLockManager();
        lockManager.startLog();

        this.db = new Database(filename, 5, lockManager);
        assertTrue(lockManager.log.contains("set-capacity database/table-testTable1 2"));
    }

    @Test
    public void testAutoEscalateS() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 18);

        lockManager.startLog();

        BaseTransaction t0 = db.beginTransaction();
        t0.getRecord(tableName, rids.get(0));
        t0.getRecord(tableName, rids.get(rids.size() / 5));
        t0.getRecord(tableName, rids.get(rids.size() / 5 * 2));
        t0.getRecord(tableName, rids.get(rids.size() / 5 * 3));
        t0.getRecord(tableName, rids.get(rids.size() - 1));

        assertEquals(Arrays.asList(
                         "acquire 2 database IS",
                         "acquire 2 database/table-testTable1 IS",
                         "acquire 2 database/table-testTable1/1 S",
                         "acquire 2 database/table-testTable1/4 S",
                         "acquire 2 database/table-testTable1/8 S",
                         "acquire 2 database/table-testTable1/11 S",
                         "acquire/t 2 database/table-testTable1 S",
                         "release/t 2 database/table-testTable1",
                         "release/t 2 database/table-testTable1/1",
                         "release/t 2 database/table-testTable1/11",
                         "release/t 2 database/table-testTable1/4",
                         "release/t 2 database/table-testTable1/8"
                     ), lockManager.log);
    }

    @Test
    public void testAutoEscalateX() throws DatabaseException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 18);
        List<DataBox> values = TestUtils.createRecordWithAllTypes().getValues();

        lockManager.startLog();

        BaseTransaction t0 = db.beginTransaction();
        t0.updateRecord(tableName, values, rids.get(0));
        t0.deleteRecord(tableName, rids.get(rids.size() / 5));
        t0.updateRecord(tableName, values, rids.get(rids.size() / 5 * 2));
        t0.deleteRecord(tableName, rids.get(rids.size() / 5 * 3));
        t0.updateRecord(tableName, values, rids.get(rids.size() - 1));

        assertEquals(Arrays.asList(
                         "acquire 2 database IX",
                         "acquire 2 database/table-testTable1 IX",
                         "acquire 2 database/table-testTable1/1 X",
                         "acquire 2 database/table-testTable1/4 X",
                         "acquire 2 database/table-testTable1/8 X",
                         "acquire 2 database/table-testTable1/11 X",
                         "acquire/t 2 database/table-testTable1 X",
                         "release/t 2 database/table-testTable1",
                         "release/t 2 database/table-testTable1/1",
                         "release/t 2 database/table-testTable1/11",
                         "release/t 2 database/table-testTable1/4",
                         "release/t 2 database/table-testTable1/8"
                     ), lockManager.log);
    }

    @Test
    public void testCreateTableSimple() throws DatabaseException {
        lockManager.startLog();
        createTable("testTable1", 4);
        assertEquals(Arrays.asList(
                         "acquire 1 database IX",
                         "acquire 1 database/table-testTable1 X"
                     ), lockManager.log.subList(0, 2));
    }

    @Test
    public void testDeleteTableSimple() throws DatabaseException {
        String tableName = "testTable1";
        createTable(tableName, 0);

        lockManager.startLog();

        BaseTransaction t0 = db.beginTransaction();
        t0.deleteTable(tableName);

        assertEquals(Arrays.asList(
                         "acquire 2 database IX",
                         "acquire 2 database/table-testTable1 X"
                     ), lockManager.log);
    }

    @Test
    public void testDeleteAllTables() {
        lockManager.startLog();

        BaseTransaction t0 = db.beginTransaction();
        t0.deleteAllTables();

        assertEquals(Arrays.asList(
                         "acquire 1 database X"
                     ), lockManager.log);
    }
}
