package edu.berkeley.cs186.database.query; //hw4

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
import edu.berkeley.cs186.database.table.stats.TableStats;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    //This method implements the the IO cost estimation of the Block Nested Loop Join

    int usableBuffers = numBuffers - 2; //Common mistake have to first calculate the number of usable buffers

    int numLeftPages = getLeftSource().getStats().getNumPages();

    int numRightPages = getRightSource().getStats().getNumPages();

    return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages + numLeftPages;

  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  
  private class BNLJIterator extends JoinIterator {
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

      public BNLJIterator() throws QueryPlanException, DatabaseException {
          super();
          this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
          this.rightIterator = (BacktrackingIterator<Page>) BNLJOperator.this.getPageIterator(this.getRightTableName());
          leftIterator.next();
          rightIterator.next();

          rightIterator.next();
          rightIterator.mark();
          rightIterator.reset();

          this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, numBuffers - 2);
          this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
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
                  DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                  DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
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

                  this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
                  this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                  this.rightRecordIterator.mark();

              } else if (leftIterator.hasNext()) {
                  this.rightIterator.reset();
                  this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
                  this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                  this.rightRecordIterator.mark();

                  this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, numBuffers - 2);
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
