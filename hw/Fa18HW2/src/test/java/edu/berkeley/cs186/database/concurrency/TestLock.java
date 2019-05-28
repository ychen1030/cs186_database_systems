package edu.berkeley.cs186.database.concurrency;

import static org.junit.Assert.assertFalse;

import java.util.Set;
import java.util.function.Consumer;

import org.junit.Test;

public class TestLock {
  private Consumer<Set<Long>> ignoreConflicts = (Set<Long> conflicts) -> {};

  @Test
  public void TestSingleSLock() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.S, ignoreConflicts));
  }

  @Test
  public void TestSingleXLock() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.X, ignoreConflicts));
  }

  @Test
  public void TestSingleISLock() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.IS, ignoreConflicts));
  }

  @Test
  public void TestSingleIXLock() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.IX, ignoreConflicts));
  }

  @Test
  public void TestMultipleSLocks() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.S, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.S, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.S, ignoreConflicts));
  }

  @Test
  public void TestMultipleISLocks() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.IS, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IS, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IS, ignoreConflicts));
  }

  @Test
  public void TestMultipleIXLocks() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.IX, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IX, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IX, ignoreConflicts));
  }

  @Test
  public void TestMultipleSISLocks() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.S, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IS, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.S, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IS, ignoreConflicts));
  }

  @Test
  public void TestMultipleISIXLocks() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.IS, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IX, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IS, ignoreConflicts));
    assertFalse(lock.lock(0l, LockType.IX, ignoreConflicts));
  }

  @Test
  public void TestSXPromote() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.S, ignoreConflicts));
    assertFalse(lock.promote(0l, LockType.S, LockType.X, ignoreConflicts));
  }

  @Test
  public void TestXSPromote() {
    Lock lock = new Lock();
    assertFalse(lock.lock(0l, LockType.X, ignoreConflicts));
    assertFalse(lock.promote(0l, LockType.X, LockType.S, ignoreConflicts));
  }

}
