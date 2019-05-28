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
public class TestOptimization2 {

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

    //db.createTableWithIndices(this.schema, TABLENAME, Arrays.asList("int"));
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
  public void test() throws DatabaseException, QueryPlanException{

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
    System.out.println(finalOperator.toString());

    assert(true);

  }




}
