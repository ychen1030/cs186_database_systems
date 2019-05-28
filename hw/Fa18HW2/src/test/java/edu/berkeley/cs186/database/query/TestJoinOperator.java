package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;
import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class TestJoinOperator {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test(timeout=5000)
  @Ignore
  public void testOperatorSchema() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    List<String> expectedSchemaNames = new ArrayList<String>();
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");

    List<Type> expectedSchemaTypes = new ArrayList<Type>();
    expectedSchemaTypes.add(Type.boolType());
    expectedSchemaTypes.add(Type.intType());
    expectedSchemaTypes.add(Type.stringType(5));
    expectedSchemaTypes.add(Type.floatType());
    expectedSchemaTypes.add(Type.boolType());
    expectedSchemaTypes.add(Type.intType());
    expectedSchemaTypes.add(Type.stringType(5));
    expectedSchemaTypes.add(Type.floatType());

    Schema expectedSchema = new Schema(expectedSchemaNames, expectedSchemaTypes);

    assertEquals(expectedSchema, joinOperator.getOutputSchema());
  }

  @Test(timeout=5000)
  @Ignore
  public void testSimpleJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  @Ignore
  public void testEmptyJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator leftSourceOperator = new TestSourceOperator();

    List<Integer> values = new ArrayList<Integer>();
    TestSourceOperator rightSourceOperator = TestUtils.createTestSourceOperatorWithInts(values);
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();

    assertFalse(outputIterator.hasNext());
  }

  @Test(timeout=5000)
  @Ignore
  public void testSimpleJoinPNLJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new PNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  @Ignore
  public void testSimpleJoinBNLJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new BNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }




  @Test(timeout=10000)
  @Ignore
  public void testSimplePNLJOutputOrder() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath());
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();

    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
    }

    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

    for (int i = 0; i < 288; i++) {
      List<DataBox> vals;
      if (i < 144) {
        vals = r1Vals;
      } else {
        vals = r2Vals;
      }
      transaction.addRecord("leftTable", vals);
      transaction.addRecord("rightTable", vals);
    }

    for (int i = 0; i < 288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new PNLJOperator(s1, s2, "int", "int", transaction);

    int count = 0;
    Iterator<Record> outputIterator = joinOperator.iterator();

    while (outputIterator.hasNext()) {
      if (count < 20736) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*2) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*3) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*4) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*5) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*6) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*7) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else {
        assertEquals(expectedRecord1, outputIterator.next());
      }
      count++;
    }

    assertTrue(count == 165888);
  }

  @Test(timeout=5000)
  @Ignore
  public void testSimpleSortMergeJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SortMergeOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }


  @Test(timeout=10000)
  @Ignore
  public void testSortMergeJoinUnsortedInputs() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataBox> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataBox> r4Vals = r4.getValues();
    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();

    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataBox val: r3Vals) {
        expectedRecordValues3.add(val);
      }
      for (DataBox val: r4Vals) {
        expectedRecordValues4.add(val);
      }
    }
    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    Record expectedRecord4 = new Record(expectedRecordValues4);
    List<Record> leftTableRecords = new ArrayList<>();
    List<Record> rightTableRecords = new ArrayList<>();
    for (int i = 0; i < 288*2; i++) {
      Record r;
      if (i % 4 == 0) {
        r = r1;
      } else if (i % 4  == 1) {
        r = r2;
      } else if (i % 4  == 2) {
        r = r3;
      } else {
        r = r4;
      }
      leftTableRecords.add(r);
      rightTableRecords.add(r);
    }
    Collections.shuffle(leftTableRecords, new Random(10));
    Collections.shuffle(rightTableRecords, new Random(20));
    for (int i = 0; i < 288*2; i++) {
      transaction.addRecord("leftTable", leftTableRecords.get(i).getValues());
      transaction.addRecord("rightTable", rightTableRecords.get(i).getValues());
    }


    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");

    JoinOperator joinOperator = new SortMergeOperator(s1, s2, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;
    Record expectedRecord;


    while (outputIterator.hasNext()) {
      if (numRecords < (288*288/4)) {
        expectedRecord = expectedRecord1;
      } else if (numRecords < (288*288/2)) {
        expectedRecord = expectedRecord2;
      } else if (numRecords < 288*288 - (288*288/4)) {
        expectedRecord = expectedRecord3;
      } else {
        expectedRecord = expectedRecord4;
      }
      Record r = outputIterator.next();
      assertEquals(r, expectedRecord);
      numRecords++;
    }

    assertEquals(288*288, numRecords);
  }




  @Test(timeout=10000)
  @Ignore
  public void testBNLJDiffOutPutThanPNLJ() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataBox> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataBox> r4Vals = r4.getValues();
    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();

    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataBox val: r3Vals) {
        expectedRecordValues3.add(val);
      }
      for (DataBox val: r4Vals) {
        expectedRecordValues4.add(val);
      }
    }
    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    Record expectedRecord4 = new Record(expectedRecordValues4);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
    for (int i = 0; i < 2*288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r3Vals);
      } else if (i < 288) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r4Vals);
      } else if (i < 432) {
        transaction.addRecord("leftTable", r3Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r4Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }
    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new BNLJOperator(s1, s2, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    int count = 0;
    while (outputIterator.hasNext()) {
      Record r = outputIterator.next();
      if (count < 144 * 144) {
        assertEquals(expectedRecord3, r);
      } else if (count < 2 * 144 * 144) {
        assertEquals(expectedRecord4, r);
      } else if (count < 3 * 144 * 144) {
        assertEquals(expectedRecord1, r);
      } else {
        assertEquals(expectedRecord2, r);
      }
      count++;
    }
    assertTrue(count == 82944);

  }

}
