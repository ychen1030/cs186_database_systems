package edu.berkeley.cs186.database.table;

import java.util.Iterator;

import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.DatabaseException;

/**
 * A RecordIterator wraps an Iterator<RecordId> to form an Iterator<Record>.
 * For example,
 *
 *   Iterator<RecordId> ridIterator = getRecordIdIterator();
 *   RecordIterator recordIterator = new RecordIterator(t, ridIterator);
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 */
public class RecordIterator implements BacktrackingIterator<Record> {
  private Iterator<RecordId> ridIter;
  private Table table;

  public RecordIterator(Table table, Iterator<RecordId> ridIter) {
    this.ridIter = ridIter;
    this.table = table;
  }


  public boolean hasNext() {
    return ridIter.hasNext();
  }

  public Record next() {
    try {
      return table.getRecord(ridIter.next());
    } catch (DatabaseException e) {
      throw new IllegalStateException(e);
    }
  }

  public void mark() {
    if (ridIter instanceof BacktrackingIterator) {
      ((BacktrackingIterator) ridIter).mark();
    } else {
      throw new UnsupportedOperationException("Cannot mark using underlying iterator");
    }
  }

  public void reset() {
    if (ridIter instanceof BacktrackingIterator) {
      ((BacktrackingIterator) ridIter).reset();
    } else {
      throw new UnsupportedOperationException("Cannot reset using underlying iterator");
    }
  }
}

