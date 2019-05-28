package edu.berkeley.cs186.database.common;

import java.util.Iterator;

public interface BacktrackingIterator<T> extends Iterator<T> {
  /**
   * mark() marks the last returned value of the iterator, which is the last
   * returned value of next().
   *
   * Calling mark() on an iterator that has not yielded a record yet,
   * or that has not yielded a record since the last reset() call does nothing.
   */
  void mark();

  /**
   * reset() resets the iterator to the last marked location.
   *
   * The next next() call should return the value that was returned right before
   * the last time mark() was called. If mark() was never called, reset()
   * does nothing. You may reset() to the same point as many times as desired,
   * as long as mark() is not called again.
   */
  void reset();
}

