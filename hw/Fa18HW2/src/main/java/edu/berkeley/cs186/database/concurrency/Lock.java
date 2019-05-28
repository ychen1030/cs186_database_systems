package edu.berkeley.cs186.database.concurrency;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class Lock {
  private Map<LockType, Set<Long>> lockers;

  public Lock() {
    lockers = new HashMap<LockType, Set<Long>>();
    for (LockType type : LockType.values()) {
      lockers.put(type, new HashSet<Long>());
    }
  }

  public synchronized boolean lock(Long tid, LockType type,
                                   Consumer<Set<Long>> onBlock) {
    boolean blocked = false;
    Set<Long> conflicts = getConflictingTransactions(type);
    if (conflicts.size() > 0) {
      blocked = true;
      onBlock.accept(conflicts);

      while (getConflictingTransactions(type).size() > 0) {
        try {
          wait();
        } catch (InterruptedException e) {
          // TODO(mwhittaker): Handle exception.
        }
      }
    }
    lockers.get(type).add(tid);
    return blocked;
  }

  public synchronized boolean promote(Long tid, LockType from, LockType to,
                                      Consumer<Set<Long>> onBlock) {
    boolean blocked = false;
    Set<Long> conflicts = getConflictingTransactions(to);
    conflicts.remove(tid);
    if (conflicts.size() > 0) {
      blocked = true;
      onBlock.accept(conflicts);

      while (conflicts.size() > 0) {
        try {
          wait();
        } catch (InterruptedException e) {
        }
        conflicts = getConflictingTransactions(to);
        conflicts.remove(tid);
      }
    }
    lockers.get(from).remove(tid);
    lockers.get(to).add(tid);
    return blocked;
  }

  public synchronized void unlock(Long tid, LockType type) {
    lockers.get(type).remove(tid);
    notifyAll();
  }

  private synchronized Set<Long> getConflictingTransactions(LockType type) {
      Set<Long> conflicts = new HashSet<Long>();
      for (LockType t : LockType.conflictingLockTypes(type)) {
          conflicts.addAll(lockers.get(t));
      }
      return conflicts;
  }
}
