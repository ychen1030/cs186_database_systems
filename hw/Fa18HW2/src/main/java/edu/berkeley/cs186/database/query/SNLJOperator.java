package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;

public class SNLJOperator extends JoinOperator {
  private QueryOperator leftSource;
  private QueryOperator rightSource;
  private int leftColumnIndex;
  private int rightColumnIndex;
  private String leftColumnName;
  private String rightColumnName;
  private Database.Transaction transaction;

  public SNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.SNLJ);

    this.leftColumnName = getLeftColumnName();
    this.rightColumnName = getRightColumnName();

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    //does nothing
    return 0;
  }   

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   * Note that the left table is the "outer" loop and the right table is the "inner" loop.
   */
  private class SNLJIterator extends JoinIterator {
    private RecordIterator leftIterator;
    private RecordIterator rightIterator;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;

    public SNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.rightIterator = SNLJOperator.this.getRecordIterator(this.getRightTableName());
      this.leftIterator = SNLJOperator.this.getRecordIterator(this.getLeftTableName());

      this.nextRecord = null;

      this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
      this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

      // We mark the first record so we can reset to it when we advance the left record.
      if (rightRecord != null) {
        rightIterator.mark();
      }
      else return;

      try {
        fetchNextRecord();
      } catch (DatabaseException e) {
        this.nextRecord = null;
      }
    }

    /**
     * After this method is called, rightRecord will contain the first record in the rightSource.
     * There is always a first record. If there were no first records (empty rightSource)
     * then the code would not have made it this far. See line 69.
     */
    private void resetRightRecord(){
      this.rightIterator.reset();
      assert(rightIterator.hasNext());
      rightRecord = rightIterator.next();
      rightIterator.mark();
    }

    /**
     * Advances the left record
     *
     * The thrown exception means we're done: there is no next record
     * It causes this.fetchNextRecord (the caller) to hand control to its caller.
     * Exceptions can be a way to force the parent to "return" (with simple logic).
     * @throws DatabaseException
     */
    private void nextLeftRecord() throws DatabaseException {
      if (!leftIterator.hasNext()) throw new DatabaseException("All Done!");
      leftRecord = leftIterator.next();
    }

    /**
     * Pre-fetches what will be the next record, and puts it in this.nextRecord.
     * Pre-fetching simplifies the logic of this.hasNext() and this.next()
     * @throws DatabaseException
     */
    private void fetchNextRecord() throws DatabaseException {
      if (this.leftRecord == null) throw new DatabaseException("No new record to fetch");
      this.nextRecord = null;
      do {
        if (this.rightRecord != null) {
          DataBox leftJoinValue = this.leftRecord.getValues().get(SNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = rightRecord.getValues().get(SNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
          }
          this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
        }
        else {
          nextLeftRecord();
          resetRightRecord();
        }
      } while (!hasNext());
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      return this.nextRecord != null;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (!this.hasNext()) {
        throw new NoSuchElementException();
      }

      Record nextRecord = this.nextRecord;
      try {
        this.fetchNextRecord();
      } catch (DatabaseException e) {
        this.nextRecord = null;
      }
      return nextRecord;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

}
