package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.table.*;
import edu.berkeley.cs186.database.databox.*;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

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
    this.db.deleteAllTables();
  }

  @After
  public void afterEach() {
    this.db.deleteAllTables();
    this.db.close();
  }

  @Test
  public void testTableCreate() throws DatabaseException {
    Schema s = TestUtils.createSchemaWithAllTypes();

    db.createTable(s, "testTable1");
  }

  @Test
  public void testTransactionBegin() throws DatabaseException {
    Schema s = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    String tableName = "testTable1";
    db.createTable(s, tableName);

    Database.Transaction t1 = db.beginTransaction();
    RecordId rid = t1.addRecord(tableName, input.getValues());
    Record rec = t1.getRecord(tableName, rid);
    assertEquals(input, rec);
    t1.end();
  }

  @Test
  public void testTransactionTempTable() throws DatabaseException {
    Schema s = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    String tableName = "testTable1";
    db.createTable(s, tableName);

    Database.Transaction t1 = db.beginTransaction();
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
    db.createTable(s, tableName);

    Database.Transaction t1 = db.beginTransaction();
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
    db.createTable(s, tableName);


    Database.Transaction t1 = db.beginTransaction();
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

  @Test
  public void testAtomicTransactions1() throws DatabaseException {
    Schema s = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    //create a test table
    String tableName = "testTable1";
    db.createTable(s, tableName);


    Database.AtomicTransaction transaction1 = db.createAtomicTransaction();
    for (int i = 0; i < 100; i++){
      transaction1.addRecord(tableName, input.getValues());
    }
    //add a 100 random records


    Thread t1_thread = new Thread(transaction1, "x1");
    t1_thread.start();
    //launch


    Database.AtomicTransaction transaction2 = db.createAtomicTransaction();
    for (int i = 0; i < 100; i++){
      transaction2.addRecord(tableName, input.getValues());
    }
    //add a 100 random records

    Thread t2_thread = new Thread(transaction2, "x2");
    t2_thread.start();


    Table table = db.getTable(tableName);

    System.out.println(table);

    try {
            t1_thread.join();
            t2_thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    assertEquals(table.getNumRecords(),200);
  }
}