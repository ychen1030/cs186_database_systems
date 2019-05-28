package edu.berkeley.cs186.database.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.Page;

@FixMethodOrder(MethodSorters.DEFAULT)
@Ignore
public class TestTable {
  public static final String TABLENAME = "testtable";
  private Table table;
  private Schema schema;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void beforeEach() throws Exception {
    this.schema = TestUtils.createSchemaWithAllTypes();
    this.table = createTestTable(this.schema, TABLENAME);
  }

  @After
  public void afterEach() {
    table.close();
  }

  private Table createTestTable(Schema schema, String tableName) throws DatabaseException {
    try {
      File file = tempFolder.newFile(tableName + Table.FILENAME_EXTENSION);
      return new Table(tableName, schema, file.getAbsolutePath());
    } catch (IOException e) {
      throw new DatabaseException(e.getMessage());
    }
  }

  private static Record createRecordWithAllTypes(int i) {
    Record r = TestUtils.createRecordWithAllTypes();
    r.getValues().set(1, new IntDataBox(i));
    return r;
  }

  @Test
  @Ignore
  public void testComputeBitMapSizeInBytesAndComputeNumRecordsPerPage() {
    Schema oneByte = new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(1)));
    Schema tenBytes = new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(10)));

    for (int i = 9; i <= 17; ++i) {
      assertEquals(8, Table.computeNumRecordsPerPage(i, oneByte));
      assertEquals(1, Table.computeBitmapSizeInBytes(i, oneByte));
    }
    for (int i = 18; i <= 26; ++i) {
      assertEquals(16, Table.computeNumRecordsPerPage(i, oneByte));
      assertEquals(2, Table.computeBitmapSizeInBytes(i, oneByte));
    }
    for (int i = 27; i <= 35; ++i) {
      assertEquals(24, Table.computeNumRecordsPerPage(i, oneByte));
      assertEquals(3, Table.computeBitmapSizeInBytes(i, oneByte));
    }

    for (int i = 81; i <= 161; ++i) {
      assertEquals(8, Table.computeNumRecordsPerPage(i, tenBytes));
      assertEquals(1, Table.computeBitmapSizeInBytes(i, tenBytes));
    }
    for (int i = 162; i <= 242; ++i) {
      assertEquals(16, Table.computeNumRecordsPerPage(i, tenBytes));
      assertEquals(2, Table.computeBitmapSizeInBytes(i, tenBytes));
    }
    for (int i = 243; i <= 323; ++i) {
      assertEquals(24, Table.computeNumRecordsPerPage(i, tenBytes));
      assertEquals(3, Table.computeBitmapSizeInBytes(i, tenBytes));
    }
  }

  @Test
  @Ignore
  public void testGetNumRecordsPerPage() throws DatabaseException {
    assertEquals(14, schema.getSizeInBytes());
    assertEquals(4096, Page.pageSize);
    // 36 + (288 * 14) = 4068
    // 37 + (296 * 14) = 4181
    assertEquals(288, table.getNumRecordsPerPage());
  }

  @Test
  @Ignore
  public void testGetBitmapSizeInBytes() throws DatabaseException {
    assertEquals(14, schema.getSizeInBytes());
    assertEquals(4096, Page.pageSize);
    // 36 + (288 * 14) = 4068
    // 37 + (296 * 14) = 4181
    assertEquals(36, table.getBitmapSizeInBytes());
  }

  @Test
  @Ignore
  public void testSingleInsertAndGet() throws DatabaseException {
    Record r = createRecordWithAllTypes(0);
    RecordId rid = table.addRecord(r.getValues());
    assertEquals(r, table.getRecord(rid));
  }

  @Test
  @Ignore
  public void testThreePagesOfInserts() throws DatabaseException {
    List<RecordId> rids = new ArrayList<>();
    for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
      Record r = createRecordWithAllTypes(i);
      rids.add(table.addRecord(r.getValues()));
    }

    for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
      Record r = createRecordWithAllTypes(i);
      assertEquals(r, table.getRecord(rids.get(i)));
    }
  }

  @Test
  @Ignore
  public void testSingleDelete() throws DatabaseException {
    Record r = createRecordWithAllTypes(0);
    RecordId rid = table.addRecord(r.getValues());
    assertEquals(r, table.deleteRecord(rid));
  }

  @Test
  @Ignore
  public void testThreePagesOfDeletes() throws DatabaseException {
    List<RecordId> rids = new ArrayList<>();
    for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
      Record r = createRecordWithAllTypes(i);
      rids.add(table.addRecord(r.getValues()));
    }

    for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
      Record r = createRecordWithAllTypes(i);
      assertEquals(r, table.deleteRecord(rids.get(i)));
    }
  }

  @Test(expected = DatabaseException.class)
  @Ignore
  public void testGetDeletedRecord() throws DatabaseException {
    Record r = createRecordWithAllTypes(0);
    RecordId rid = table.addRecord(r.getValues());
    table.deleteRecord(rid);
    table.getRecord(rid);
  }

  @Test
  @Ignore
  public void testUpdateSingleRecord() throws DatabaseException {
    Record rOld = createRecordWithAllTypes(0);
    Record rNew = createRecordWithAllTypes(42);

    RecordId rid = table.addRecord(rOld.getValues());
    assertEquals(rOld, table.updateRecord(rNew.getValues(), rid));
    assertEquals(rNew, table.getRecord(rid));
  }

  @Test
  @Ignore
  public void testThreePagesOfUpdates() throws DatabaseException {
    List<RecordId> rids = new ArrayList<>();
    for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
      Record r = createRecordWithAllTypes(i);
      rids.add(table.addRecord(r.getValues()));
    }

    for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
      Record rOld = createRecordWithAllTypes(i);
      Record rNew = createRecordWithAllTypes(i * 10000);
      assertEquals(rOld, table.updateRecord(rNew.getValues(), rids.get(i)));
      assertEquals(rNew, table.getRecord(rids.get(i)));
    }
  }

  @Test
  @Ignore
  public void testLoadTableFromDisk() throws Exception {
    // We add 42 to make sure we have some incomplete pages.
    int numRecords = table.getNumRecordsPerPage() * 2 + 42;

    List<RecordId> rids = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      Record r = createRecordWithAllTypes(i);
      rids.add(table.addRecord(r.getValues()));
    }
    table.close();

    table = new Table(table.getName(), table.getFilename());
    for (int i = 0; i < numRecords; ++i) {
      Record r = createRecordWithAllTypes(i);
      assertEquals(r, table.getRecord(rids.get(i)));
    }
  }

  @Test
  @Ignore
  public void testLoadTableFromDiskThenWriteMoreRecords() throws Exception {
    // We add 42 to make sure we have some incomplete pages.
    int numRecords = table.getNumRecordsPerPage() * 2 + 42;

    List<RecordId> rids = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      Record r = createRecordWithAllTypes(i);
      rids.add(table.addRecord(r.getValues()));
    }
    table.close();

    table = new Table(table.getName(), table.getFilename());
    for (int i = numRecords; i < 2 * numRecords; ++i) {
      Record r = createRecordWithAllTypes(i);
      rids.add(table.addRecord(r.getValues()));
    }

    for (int i = 0; i < 2 * numRecords; ++i) {
      Record r = createRecordWithAllTypes(i);
      assertEquals(r, table.getRecord(rids.get(i)));
    }
  }

  /**
   * Loads some number of pages of records. rids will be loaded with all the record IDs
   * of the new records, and the number of records will be returned.
   */
  private int setupIteratorTest(List<RecordId> rids, int pages) throws DatabaseException {
    int numRecords = table.getNumRecordsPerPage() * pages;

    // Write the records.
    for (int i = 0; i < numRecords; ++i) {
      Record r = createRecordWithAllTypes(i);
      RecordId rid = table.addRecord(r.getValues());
      rids.add(rid);
    }

    return numRecords;
  }

  /**
   * See above; this overload should be used when the list of record IDs is not
   * needed.
   */
  private int setupIteratorTest(int pages) throws DatabaseException {
    List<RecordId> rids = new ArrayList();
    return setupIteratorTest(rids, pages);
  }

  /**
   * Performs a simple loop checking (end - start)/incr records from iter, and
   * assuming values of recordWithAllTypes(i), where start <= i < end and
   * i increments by incr.
   */
  private void checkSequentialRecords(int start, int end, int incr, BacktrackingIterator<Record> iter) {
    for (int i = start; i < end; i += incr) {
      assertTrue(iter.hasNext());
      assertEquals(createRecordWithAllTypes(i), iter.next());
    }
  }

  /**
   * Basic test over a full page of records to check that next/hasNext work.
   */
  @Test
  public void testRIDPageIterator() throws DatabaseException {
    int numRecords = setupIteratorTest(1);
    Iterator<Page> pages = table.getAllocator().iterator();
    // Header page.
    pages.next();
    Page page = pages.next();

    BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));
    checkSequentialRecords(0, numRecords, 1, iter);
    assertFalse(iter.hasNext());
  }

  /**
   * Basic test over a half-full page of records, with a missing first/last
   * record and gaps between every record, to check that next/hasNext work.
   */
  @Test
  public void testRIDPageIteratorWithGaps() throws DatabaseException {
    List<RecordId> rids = new ArrayList();
    int numRecords = setupIteratorTest(rids, 1);

    // Delete every other record and the last record.
    for (int i = 0; i < numRecords - 1; i += 2) {
      table.deleteRecord(rids.get(i));
    }
    table.deleteRecord(rids.get(numRecords - 1));

    Iterator<Page> pages = table.getAllocator().iterator();
    // Header page.
    pages.next();
    Page page = pages.next();

    BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));
    checkSequentialRecords(1, numRecords - 1, 2, iter);
    assertFalse(iter.hasNext());
  }

  /**
   * Basic test making sure that RIDPageIterator handles mark/reset properly.
   */
  @Test
  public void testRIDPageIteratorMarkReset() throws DatabaseException {
    int numRecords = setupIteratorTest(1);
    Iterator<Page> pages = table.getAllocator().iterator();
    // Header page.
    pages.next();
    Page page = pages.next();

    BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));
    checkSequentialRecords(0, numRecords / 2, 1, iter);
    iter.mark();
    checkSequentialRecords(numRecords / 2, numRecords, 1, iter);
    assertFalse(iter.hasNext());
    iter.reset();
    // -1 because the record before the mark must also be returned
    checkSequentialRecords(numRecords / 2 - 1, numRecords, 1, iter);
    assertFalse(iter.hasNext());

    // resetting twice to the same mark should be fine.
    iter.reset();
    checkSequentialRecords(numRecords / 2 - 1, numRecords, 1, iter);
    assertFalse(iter.hasNext());
  }

  /**
   * Tests RIDBlockIterator over three full pages of records.
   */
  @Test
  public void testRIDBlockIterator() throws DatabaseException {
    int numRecords = setupIteratorTest(3);
    Iterator<Page> pages = table.getAllocator().iterator();
    // Header page.
    pages.next();

    BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDBlockIterator(pages, 3));
    checkSequentialRecords(0, numRecords, 1, iter);
    assertFalse(iter.hasNext());
  }

  /**
   * Basic test making sure that RIDBlockIterator handles mark/reset properly.
   */
  @Test
  public void testRIDBlockIteratorMarkReset() throws DatabaseException {
    int numRecords = setupIteratorTest(3);
    Iterator<Page> pages = table.getAllocator().iterator();
    // Header page.
    pages.next();

    BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDBlockIterator(pages, 3));
    checkSequentialRecords(0, numRecords / 2, 1, iter);
    iter.mark();
    checkSequentialRecords(numRecords / 2, numRecords, 1, iter);
    assertFalse(iter.hasNext());
    iter.reset();
    // -1 because the record before the mark must also be returned
    checkSequentialRecords(numRecords / 2 - 1, numRecords, 1, iter);
    assertFalse(iter.hasNext());

    // resetting twice to the same mark should be fine.
    iter.reset();
    checkSequentialRecords(numRecords / 2 - 1, numRecords, 1, iter);
    assertFalse(iter.hasNext());

    // page boundary test
    pages = table.getAllocator().iterator();
    pages.next();
    iter = new RecordIterator(table, table.new RIDBlockIterator(pages, 3));
    // test second to last record of second to last page
    checkSequentialRecords(0, 2 * numRecords / 3 - 1, 1, iter);
    iter.mark();
    checkSequentialRecords(2 * numRecords / 3 - 1, numRecords, 1, iter);
    assertFalse(iter.hasNext());
    iter.reset();
    checkSequentialRecords(2 * numRecords / 3 - 2, numRecords, 1, iter);
    assertFalse(iter.hasNext());
    iter.reset();
    iter.next();
    iter.next();
    // test last record of second to last page
    iter.mark();
    checkSequentialRecords(2 * numRecords / 3, numRecords, 1, iter);
    assertFalse(iter.hasNext());
    iter.reset();
    checkSequentialRecords(2 * numRecords / 3 - 1, numRecords, 1, iter);
    assertFalse(iter.hasNext());
  }

  /**
   * Simple test of TableIterator over three pages of records with no gaps.
   */
  @Test
  public void testTableIterator() throws DatabaseException {
    // We add 42 to make sure we have some incomplete pages.
    int numRecords = table.getNumRecordsPerPage() * 2 + 42;

    // Write the records.
    for (int i = 0; i < numRecords; ++i) {
      Record r = createRecordWithAllTypes(i);
      table.addRecord(r.getValues());
    }

    // Iterate once.
    BacktrackingIterator<Record> iter = table.iterator();
    checkSequentialRecords(0, numRecords, 1, iter);
    assertFalse(iter.hasNext());

    // Iterate twice for good measure.
    iter = table.iterator();
    checkSequentialRecords(0, numRecords, 1, iter);
    assertFalse(iter.hasNext());

    // Reload the database and iterate again.
    table.close();
    table = new Table(table.getName(), table.getFilename());
    iter = table.iterator();
    checkSequentialRecords(0, numRecords, 1, iter);
    assertFalse(iter.hasNext());
  }

  /**
   * Simple test of TableIterator over three pages of records with every other
   * record missing.
   */
  @Test
  public void testTableIteratorWithGaps() throws DatabaseException {
    // We add 42 to make sure we have some incomplete pages.
    int numRecords = table.getNumRecordsPerPage() * 2 + 42;

    // Write the records.
    List<RecordId> rids = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      Record r = createRecordWithAllTypes(i);
      rids.add(table.addRecord(r.getValues()));
    }

    // Delete every other record.
    for (int i = 0; i < numRecords; i += 2) {
      table.deleteRecord(rids.get(i));
    }

    // Iterate.
    BacktrackingIterator<Record> iter = table.iterator();
    checkSequentialRecords(1, numRecords, 2, iter);
    assertFalse(iter.hasNext());

    // Reload the database and iterate again.
    table.close();
    table = new Table(table.getName(), table.getFilename());
    iter = table.iterator();
    checkSequentialRecords(1, numRecords, 2, iter);
    assertFalse(iter.hasNext());
  }
}
