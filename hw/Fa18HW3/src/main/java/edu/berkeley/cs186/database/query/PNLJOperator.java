package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  public int estimateIOCost() throws QueryPlanException {
    int numRightPages = getRightSource().getStats().getNumPages();
    int numLeftPages = getLeftSource().getStats().getNumPages();

    return numLeftPages * numRightPages + numLeftPages;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private Iterator<Page> leftIterator;
    private BacktrackingIterator<Page> rightIterator;
    private BacktrackingIterator<Record> leftRecordIterator;
    private BacktrackingIterator<Record> rightRecordIterator;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord = null;

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftIterator = PNLJOperator.this.getPageIterator(this.getLeftTableName());
      this.rightIterator = (BacktrackingIterator<Page>) PNLJOperator.this.getPageIterator(this.getRightTableName());
      leftIterator.next();
      rightIterator.next();

      rightIterator.next();
      rightIterator.mark();
      rightIterator.reset();

      this.leftRecordIterator = PNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, 1);
      this.rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
      this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
      this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;

      if (leftRecord != null) {
        leftRecordIterator.mark();
      } else return;
      if (rightRecord != null) {
        rightRecordIterator.mark();
      } else return;

      try {
        fetchNextRecord();
      } catch (DatabaseException e) {
        this.nextRecord = null;
      }
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
          DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = this.rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
          }
          this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;

        } else if (leftRecordIterator.hasNext()) {
          this.leftRecord = leftRecordIterator.next();
          this.rightRecordIterator.reset();
          this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
          this.rightRecordIterator.mark();

        } else if (rightIterator.hasNext()) {
          leftRecordIterator.reset();
          this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;

          this.rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
          this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
          this.rightRecordIterator.mark();

        } else if (leftIterator.hasNext()) {
          this.rightIterator.reset();
          this.rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
          this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
          this.rightRecordIterator.mark();

          this.leftRecordIterator = PNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, 1);
          this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
          this.leftRecordIterator.mark();

        } else {
          throw new DatabaseException("All Done!");
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

      Record currRecord = this.nextRecord;
      try {
        this.fetchNextRecord();
      } catch (DatabaseException e) {
        this.nextRecord = null;
      }
      return currRecord;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
