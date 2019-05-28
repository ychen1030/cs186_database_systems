package edu.berkeley.cs186.database.optimization;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.Ignore;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Arrays;

import edu.berkeley.cs186.database.table.stats.Histogram;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.QueryPlanException;
import edu.berkeley.cs186.database.Database;

import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.BoolDataBox;

import static org.junit.Assert.*;
import org.junit.After;

@Ignore
public class TestOptimizationJoins {

  private Table table;
  private Schema schema;
  public static final String TABLENAME = "T";

  public static final String TestDir = "testDatabase";
  private Database db;
  private String filename;


  //Before every test you create a temporary table, after every test you close it
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void beforeEach() throws Exception {
    
    File testDir = tempFolder.newFolder(TestDir);
    this.filename = testDir.getAbsolutePath();
    this.db = new Database(filename);
    this.db.deleteAllTables();

    this.schema = TestUtils.createSchemaWithAllTypes();

    db.createTable(this.schema, TABLENAME);

    db.createTableWithIndices(this.schema, TABLENAME+"I", Arrays.asList("int"));

    db.createTable(TestUtils.createSchemaWithAllTypes("one_"), TABLENAME+"o1");
    db.createTable(TestUtils.createSchemaWithAllTypes("two_"), TABLENAME+"o2");
    db.createTable(TestUtils.createSchemaWithAllTypes("three_"), TABLENAME+"o3");
    db.createTable(TestUtils.createSchemaWithAllTypes("four_"), TABLENAME+"o4");
  }

  @After
  public void afterEach() {
    this.db.deleteAllTables();
    this.db.close();
  }

  //creates a record with all specified types
  private static Record createRecordWithAllTypes(boolean a1, int a2, String a3, float a4) {
    Record r = TestUtils.createRecordWithAllTypes();
    r.getValues().set(0, new BoolDataBox(a1));
    r.getValues().set(1, new IntDataBox(a2));
    r.getValues().set(2, new StringDataBox(a3,5));
    r.getValues().set(3, new FloatDataBox(a4));
    return r;
  }

  @Test
  public void testJoinTypeA() throws DatabaseException, QueryPlanException{

    Table table = db.getTable(TABLENAME);

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table.buildStatistics(10);


    // create a new transaction
    Database.Transaction transaction = this.db.beginTransaction();

    transaction.queryAs("T", "t1");
    transaction.queryAs("T", "t2");

    // add a join and a select to the QueryPlan
    QueryPlan query = transaction.query("t1");
    query.join("t2", "t1.int", "t2.int");
    //query.select("int", PredicateOperator.EQUALS, new IntDataBox(10));

    // execute the query and get the output
    Iterator<Record> queryOutput = query.executeOptimal();

    QueryOperator finalOperator = query.getFinalOperator();
    assert(finalOperator.toString().contains("BNLJ"));

  }

  @Test
  public void testJoinTypeB() throws DatabaseException, QueryPlanException{

    Table table = db.getTable(TABLENAME+"I");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 10; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table.buildStatistics(10);


    // create a new transaction
    Database.Transaction transaction = this.db.beginTransaction();

    transaction.queryAs("TI", "t1");
    transaction.queryAs("TI", "t2");

    // add a join and a select to the QueryPlan
    QueryPlan query = transaction.query("t1");
    query.join("t2", "t1.int", "t2.int");
    query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));

    // execute the query and get the output
    Iterator<Record> queryOutput = query.executeOptimal();

    QueryOperator finalOperator = query.getFinalOperator();

    assert(finalOperator.toString().contains("\tvalue: 9"));
  }

  @Test
  public void testJoinTypeC() throws DatabaseException, QueryPlanException{

    Table table = db.getTable(TABLENAME+"I");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table.buildStatistics(10);


    // create a new transaction
    Database.Transaction transaction = this.db.beginTransaction();

    transaction.queryAs("TI", "t1");
    transaction.queryAs("TI", "t2");

    // add a join and a select to the QueryPlan
    QueryPlan query = transaction.query("t1");
    query.join("t2", "t1.int", "t2.int");
    query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));

    // execute the query and get the output
    Iterator<Record> queryOutput = query.executeOptimal();

    QueryOperator finalOperator = query.getFinalOperator();

    assert(finalOperator.toString().contains("INDEXSCAN"));

    assert(finalOperator.toString().contains("SNLJ"));
  }


  @Test
  public void testJoinOrderA() throws DatabaseException, QueryPlanException{

    Table table1 = db.getTable(TABLENAME+"o1");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 10; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table1.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table1.buildStatistics(10);


    Table table2 = db.getTable(TABLENAME+"o2");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 100; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table2.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table2.buildStatistics(10);

    Table table3 = db.getTable(TABLENAME+"o3");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table3.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table3.buildStatistics(10);


    // create a new transaction
    Database.Transaction transaction = this.db.beginTransaction();

    // add a join and a select to the QueryPlan
    QueryPlan query = transaction.query("To1");
    query.join("To2", "To1.one_int", "To2.two_int");
    query.join("To3", "To2.two_int", "To3.three_int");

    // execute the query and get the output
    Iterator<Record> queryOutput = query.executeOptimal();

    QueryOperator finalOperator = query.getFinalOperator();
    //inner most joins are the largest tables
    assert(finalOperator.toString().contains("\t\ttable: To2"));
    assert(finalOperator.toString().contains("\t\ttable: To3"));
  }


  @Test
  public void testJoinOrderB() throws DatabaseException, QueryPlanException{

    Table table1 = db.getTable(TABLENAME+"o1");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 10; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table1.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table1.buildStatistics(10);


    Table table2 = db.getTable(TABLENAME+"o2");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 100; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table2.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table2.buildStatistics(10);

    Table table3 = db.getTable(TABLENAME+"o3");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table3.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table3.buildStatistics(10);


    Table table4 = db.getTable(TABLENAME+"o4");

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table4.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //build the statistics on the table
    table4.buildStatistics(10);


    // create a new transaction
    Database.Transaction transaction = this.db.beginTransaction();

    // add a join and a select to the QueryPlan
    QueryPlan query = transaction.query("To1");
    query.join("To2", "To1.one_int", "To2.two_int");
    query.join("To3", "To2.two_int", "To3.three_int");
    query.join("To4", "To1.one_string", "To4.four_string");

    // execute the query and get the output
    Iterator<Record> queryOutput = query.executeOptimal();

    QueryOperator finalOperator = query.getFinalOperator();

    //smallest to largest order
    assert(finalOperator.toString().contains("\t\t\ttable: To1"));
    assert(finalOperator.toString().contains("\t\t\ttable: To4"));
    assert(finalOperator.toString().contains("\t\ttable: To2"));
    assert(finalOperator.toString().contains("\ttable: To3"));
  }


}
