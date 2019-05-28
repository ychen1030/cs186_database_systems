package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.table.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import java.io.File;

public class TestDatabase {
    public static final String TestDir = "testDatabase";
    private Database db;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.db = new Database(filename);
        BaseTransaction t = this.db.beginTransaction();
        t.deleteAllTables();
        t.end();
    }

    @After
    public void afterEach() {
        BaseTransaction t = this.db.beginTransaction();
        t.deleteAllTables();
        t.end();
        this.db.close();
    }

    @Test
    public void testTableCreate() throws DatabaseException {
        Schema s = TestUtils.createSchemaWithAllTypes();

        BaseTransaction t = this.db.beginTransaction();
        t.createTable(s, "testTable1");
        t.end();
    }

    @Test
    public void testTransactionBegin() throws DatabaseException {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        Database.Transaction t1 = db.beginTransaction();
        t1.createTable(s, tableName);
        RecordId rid = t1.addRecord(tableName, input.getValues());
        t1.getRecord(tableName, rid);
        t1.end();
    }

    @Test
    public void testTransactionTempTable() throws DatabaseException {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        Database.Transaction t1 = db.beginTransaction();
        t1.createTable(s, tableName);
        RecordId rid = t1.addRecord(tableName, input.getValues());
        Record rec = t1.getRecord(tableName, rid);
        assertEquals(input, rec);

        t1.createTempTable(s, "temp1");
        rid = t1.addRecord("temp1", input.getValues());
        rec = t1.getRecord("temp1", rid);
        assertEquals(input, rec);
        t1.end();
    }

    @Test(expected = DatabaseException.class)
    public void testTransactionTempTable2() throws DatabaseException {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        Database.Transaction t1 = db.beginTransaction();
        t1.createTable(s, tableName);
        RecordId rid = t1.addRecord(tableName, input.getValues());
        Record rec = t1.getRecord(tableName, rid);
        assertEquals(input, rec);

        t1.createTempTable(s, "temp1");
        rid = t1.addRecord("temp1", input.getValues());
        rec = t1.getRecord("temp1", rid);
        assertEquals(input, rec);
        t1.end();
        Database.Transaction t2 = db.beginTransaction();
        rid = t2.addRecord("temp1", input.getValues());
        rec = t1.getRecord("temp1", rid);
        assertEquals(input, rec);
        t2.end();
    }

    @Test
    public void testDatabaseDurablity() throws DatabaseException {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        Database.Transaction t1 = db.beginTransaction();
        t1.createTable(s, tableName);
        RecordId rid = t1.addRecord(tableName, input.getValues());
        Record rec = t1.getRecord(tableName, rid);

        assertEquals(input, rec);

        t1.end();

        db.close();

        db = new Database(this.filename);
        t1 = db.beginTransaction();
        rec = t1.getRecord(tableName, rid);
        assertEquals(input, rec);
        t1.end();
    }

}
