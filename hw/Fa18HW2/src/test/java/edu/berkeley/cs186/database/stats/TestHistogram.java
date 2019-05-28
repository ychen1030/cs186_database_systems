package edu.berkeley.cs186.database.stats;

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
import java.util.Random;

import edu.berkeley.cs186.database.table.stats.Histogram;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;
import edu.berkeley.cs186.database.query.QueryPlan;

import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.BoolDataBox;

import static org.junit.Assert.*;
import org.junit.After;

@Ignore
public class TestHistogram {

  private Table table;
  private Schema schema;
  public static final String TABLENAME = "testtable";


  //Before every test you create a temporary table, after every test you close it
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void beforeEach() throws Exception {
    this.schema = TestUtils.createSchemaWithAllTypes();
    this.table = createTestTable(this.schema, TABLENAME);
  }

  @After
  public void afterEach() {
    this.table.close();
  }

  private Table createTestTable(Schema schema, String tableName) throws DatabaseException {
    try {
      File file = tempFolder.newFile(tableName + Table.FILENAME_EXTENSION);
      return new Table(tableName, schema, file.getAbsolutePath());
    } catch (IOException e) {
      throw new DatabaseException(e.getMessage());
    }
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

  private String getSaltString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }

  @Test
  public void testBuildHistogramBasic() {

    //creates a 101 records int 0 to 100
    try{
      for (int i = 0; i < 101; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //creates a histogram of 10 buckets
    Histogram h = new Histogram(10);
    h.buildHistogram(table, 1); //build on the integer col

    assertEquals(101, h.getCount());
    
    assertEquals(101, h.getNumDistinct());

    for (int i=0; i<9; i++)
      assertEquals(10, h.get(i).getCount());


    assertEquals(11, h.get(9).getCount());
  }

  @Test
  public void testBuildHistogramString() {

    //creates a 101 records int 0 to 100
    try{
      for (int i = 0; i < 101; ++i) {
        Record r = createRecordWithAllTypes(false, 0, getSaltString(), 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //creates a histogram of 10 buckets
    Histogram h = new Histogram(10);
    h.buildHistogram(table, 2); //build on the integer col

    assertEquals(101, h.getCount()); //count updated properly

    assertEquals(101, h.getNumDistinct()); //distinct count updated properly
  }

  @Test
  public void testBuildHistogramEdge() {

    //creates a 101 records int 0 to 100
    try{
      for (int i = 0; i < 101; ++i) {
        Record r = createRecordWithAllTypes(false, 0, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //creates a histogram of 10 buckets
    Histogram h = new Histogram(10);
    h.buildHistogram(table, 1); //build on the integer col

    assertEquals(101, h.getCount()); //count updated properly
    
    assertEquals(1, h.getNumDistinct()); //distinct count updated properly

    for (int i=0; i<9; i++)
      assertEquals(0, h.get(i).getCount());

    assertEquals(101, h.get(9).getCount());
  }


  @Test
  public void testEquality() {

    //creates a 100 records int 0 to 99
    try{
      for (int i = 0; i < 100; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //creates a histogram of 10 buckets
    Histogram h = new Histogram(10);
    h.buildHistogram(table, 1); //build on the integer col

    //Should return [0.1,0,0,0,0,0,0,0,0,0,0]
    float [] result = h.filter(PredicateOperator.EQUALS, new IntDataBox(5));
    assert(Math.abs(result[0]-0.1) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-0.0) < 0.00001);


    //Should return [0.9,1,1,1,1,1,1,1,1,1,1]
    result = h.filter(PredicateOperator.NOT_EQUALS, new IntDataBox(5));
    assert(Math.abs(result[0]-0.9) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-1.0) < 0.00001);


    //Should return [0,0,0,0,0,0,0,0,0,0,0.1]
    result = h.filter(PredicateOperator.EQUALS, new IntDataBox(99));
    assert(Math.abs(result[9]-0.1) < 0.00001);
    for (int i=0; i<9; i++)
      assert(Math.abs(result[i]-0.0) < 0.00001);


    //Should return [0,0,0,0,0,0,0,0,0,0,0.0]
    result = h.filter(PredicateOperator.EQUALS, new IntDataBox(100));
    for (int i=0; i<10; i++)
      assert(Math.abs(result[i]-0.0) < 0.00001);


    //Should return [0,0,0,0,0,0,0,0,0,0,0.0]
    result = h.filter(PredicateOperator.EQUALS, new IntDataBox(-1));
    for (int i=0; i<10; i++)
      assert(Math.abs(result[i]-0.0) < 0.00001);

    //Should return [1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]
    result = h.filter(PredicateOperator.NOT_EQUALS, new IntDataBox(-1));
    for (int i=0; i<10; i++)
      assert(Math.abs(result[i]-1.0) < 0.00001);

  }


  @Test
  public void testGreaterThan() {

    //creates a 101 records int 0 to 100
    try{
      for (int i = 0; i <= 100; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //creates a histogram of 10 buckets
    Histogram h = new Histogram(10);
    h.buildHistogram(table, 1); //build on the integery col

    //Should return [0.1,1,1,1,1,1,1,1,1,1,1]
    float [] result = h.filter(PredicateOperator.GREATER_THAN, new IntDataBox(9));
    assert(Math.abs(result[0]-0.1) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-1.0) < 0.00001);

    //Should return [0.0,1,1,1,1,1,1,1,1,1,1]
    result = h.filter(PredicateOperator.GREATER_THAN, new IntDataBox(10));
    assert(Math.abs(result[0]-0.0) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-1.0) < 0.00001);

    //Should return [1,1,1,1,1,1,1,1,1,1,1]
    result = h.filter(PredicateOperator.GREATER_THAN, new IntDataBox(-1));
    assert(Math.abs(result[0]-1.0) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-1.0) < 0.00001);


    //Should return [0,0,0,0,0,0,0,0,0,0,0.0]
    result = h.filter(PredicateOperator.GREATER_THAN, new IntDataBox(101));
    assert(Math.abs(result[0]-0.0) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-0.0) < 0.00001);

  }


  @Test
  public void testLessThan() {

    //creates a 101 records int 0 to 100
    try{
      for (int i = 0; i <= 100; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    //creates a histogram of 10 buckets
    Histogram h = new Histogram(10);
    h.buildHistogram(table, 1); //build on the integery col

    //Should return [0.9,0,0,0,0,0,0,0,0,0,0] 
    float [] result = h.filter(PredicateOperator.LESS_THAN, new IntDataBox(9));
    assert(Math.abs(result[0]-0.9) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-0.0) < 0.00001);

    //Should return [1.0,0,0,0,0,0,0,0,0,0,0]
    result = h.filter(PredicateOperator.LESS_THAN, new IntDataBox(10));
    assert(Math.abs(result[0]-1.0) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-0.0) < 0.00001);


    //Should return [1,1,1,1,1,1,1,1,1,1,1]
    result = h.filter(PredicateOperator.LESS_THAN, new IntDataBox(101));
    assert(Math.abs(result[0]-1.0) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-1.0) < 0.00001);


    //Should return [0,0,0,0,0,0,0,0,0,0,0.0]
    result = h.filter(PredicateOperator.LESS_THAN, new IntDataBox(-1));
    assert(Math.abs(result[0]-0.0) < 0.00001);
    for (int i=1; i<10; i++)
      assert(Math.abs(result[i]-0.0) < 0.00001);
  }
  
}
