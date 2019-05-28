package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

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

public class TestSortOperator {


  @Ignore
  public static class SortRecordComparator implements Comparator<Record> {
    private int columnIndex;

    public SortRecordComparator(int columnIndex) {
      this.columnIndex = columnIndex;

    }
    public int compare(Record o1, Record o2) {
      return o1.getValues().get(this.columnIndex).compareTo(
              o2.getValues().get(this.columnIndex));
    }
  }


  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();



  @Test(timeout=5000)
  @Ignore
  public void testSortRun() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("sortTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    d.createTable(TestUtils.createSchemaWithAllTypes(), "table");
    List<Record> records = new ArrayList<>();
    List<Record> recordsToShuffle = new ArrayList<>();
    for (int i = 0; i < 288*3; i++) {
      Record r = TestUtils.createRecordWithAllTypesWithValue(i);
      records.add(r);
      recordsToShuffle.add(r);
    }
    Collections.shuffle(recordsToShuffle, new Random(42));
    SortOperator s = new SortOperator(transaction,"table", new SortRecordComparator(1));
    SortOperator.Run r = s.createRun();
    r.addRecords(recordsToShuffle);
    SortOperator.Run sortedRun = s.sortRun(r);
    Iterator<Record> iter = sortedRun.iterator();
    int i = 0;
    while (iter.hasNext()) {
      assertEquals(records.get(i), iter.next());
      i++;
    }
    assertTrue(i == 288*3);

  }




  @Test(timeout=5000)
  @Ignore
  public void testMergeSortedRuns() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("sortTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    d.createTable(TestUtils.createSchemaWithAllTypes(), "table");
    List<Record> records = new ArrayList<>();
    SortOperator s = new SortOperator(transaction,"table", new SortRecordComparator(1));
    SortOperator.Run r1 = s.createRun();
    SortOperator.Run r2 = s.createRun();
    for (int i = 0; i < 288*3; i++) {
      Record r = TestUtils.createRecordWithAllTypesWithValue(i);
      records.add(r);
      if (i % 2 == 0) {
        r1.addRecord(r.getValues());
      } else {
        r2.addRecord(r.getValues());
      }

    }
    List<SortOperator.Run> runs = new ArrayList<>();
    runs.add(r1);
    runs.add(r2);
    SortOperator.Run mergedSortedRuns = s.mergeSortedRuns(runs);
    Iterator<Record> iter = mergedSortedRuns.iterator();
    int i = 0;
    while (iter.hasNext()) {
      assertEquals(records.get(i), iter.next());
      i++;
    }
    assertTrue(i == 288*3);
  }

  @Test(timeout=10000)
  @Ignore
  public void testMergePass() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("sortTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    d.createTable(TestUtils.createSchemaWithAllTypes(), "table");
    List<Record> records1 = new ArrayList<>();
    List<Record> records2 = new ArrayList<>();
    SortOperator s = new SortOperator(transaction,"table", new SortRecordComparator(1));
    SortOperator.Run r1 = s.createRun();
    SortOperator.Run r2 = s.createRun();
    SortOperator.Run r3 = s.createRun();
    SortOperator.Run r4 = s.createRun();


    for (int i = 0; i < 288*4; i++) {
      Record r = TestUtils.createRecordWithAllTypesWithValue(i);
      if (i % 4 == 0) {
        r1.addRecord(r.getValues());
        records2.add(r);
      } else if (i % 4  == 1) {
        r2.addRecord(r.getValues());
        records1.add(r);
      } else if (i % 4  == 2) {
        r3.addRecord(r.getValues());
        records1.add(r);
      } else {
        r4.addRecord(r.getValues());
        records2.add(r);
      }

    }
    List<SortOperator.Run> runs = new ArrayList<>();
    runs.add(r3);
    runs.add(r2);
    runs.add(r1);
    runs.add(r4);
    List<SortOperator.Run> result = s.mergePass(runs);
    assertTrue(result.size() == 2);
    Iterator<Record> iter1 = result.get(0).iterator();
    Iterator<Record> iter2 = result.get(1).iterator();
    int i = 0;
    while (iter1.hasNext()) {
      assertEquals(records1.get(i), iter1.next());
      i++;
    }
    assertTrue(i == 288*2);
    i = 0;
    while (iter2.hasNext()) {
      assertEquals(records2.get(i), iter2.next());
      i++;
    }
    assertTrue(i == 288*2);

  }



  @Test(timeout=5000)
  @Ignore
  public void testSortNoChange() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("sortTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    d.createTable(TestUtils.createSchemaWithAllTypes(), "table");
    Record[] records = new Record[288*3];
    for (int i = 0; i < 288*3; i++) {
      Record r = TestUtils.createRecordWithAllTypesWithValue(i);
      records[i] = r;
      transaction.addRecord("table", r.getValues());
    }
    SortOperator s = new SortOperator(transaction,"table", new SortRecordComparator(1));
    String sortedTableName = s.sort();
    Iterator<Record> iter = transaction.getRecordIterator(sortedTableName);
    int i = 0;
    while (iter.hasNext()) {
      assertEquals(records[i], iter.next());
      i++;
    }
    assertTrue(i == 288*3);

  }

  @Test(timeout=5000)
  @Ignore
  public void testSortBackwards() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("sortTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    d.createTable(TestUtils.createSchemaWithAllTypes(), "table");
    Record[] records = new Record[288*3];
    for (int i = 288*3; i > 0; i--) {
      Record r = TestUtils.createRecordWithAllTypesWithValue(i);
      records[i-1] = r;
      transaction.addRecord("table", r.getValues());
    }
    SortOperator s = new SortOperator(transaction,"table", new SortRecordComparator(1));
    String sortedTableName = s.sort();
    Iterator<Record> iter = transaction.getRecordIterator(sortedTableName);
    int i = 0;
    while (iter.hasNext()) {
      assertEquals(records[i], iter.next());
      i++;
    }
    assertTrue(i == 288*3);

  }


  @Test(timeout=5000)
  @Ignore
  public void testSortRandomOrder() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("sortTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    d.createTable(TestUtils.createSchemaWithAllTypes(), "table");
    List<Record> records = new ArrayList<>();
    List<Record> recordsToShuffle = new ArrayList<>();
    for (int i = 0; i < 288*3; i++) {
      Record r = TestUtils.createRecordWithAllTypesWithValue(i);
      records.add(r);
      recordsToShuffle.add(r);
    }
    Collections.shuffle(recordsToShuffle, new Random(42));
    for (Record r: recordsToShuffle) {
      transaction.addRecord("table", r.getValues());
    }
    SortOperator s = new SortOperator(transaction,"table", new SortRecordComparator(1));
    String sortedTableName = s.sort();
    Iterator<Record> iter = transaction.getRecordIterator(sortedTableName);
    int i = 0;
    while (iter.hasNext()) {
      assertEquals(records.get(i), iter.next());
      i++;
    }
    assertTrue(i == 288*3);

  }

}
