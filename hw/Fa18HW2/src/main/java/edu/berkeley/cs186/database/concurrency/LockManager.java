package edu.berkeley.cs186.database.concurrency;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.table.RecordId;


// 1. Make queries multi-threaded in a way that is easy for students.
// 2. Build the LockManager.
// 3. Make database, table, and pages use the lock manager and don't permit
//    unlocked access.
// 4. Write a deadlock detector in a separate thread.


public class LockManager {
  // table name -> lock
  private Map<String, Lock> tableLocks = new HashMap<>();

  // (filename, page number)-> lock
  private Map<Pair<String, Integer>, Lock> pageLocks = new HashMap<>();

  // (table name, record id)-> lock
  private Map<Pair<String, RecordId>, Lock> recordLocks = new HashMap<>();

  // tid -> tids
  private Map<Long, Set<Long>> waitsForGraph = new HashMap<>();

  public void lockTable(Long tid, String table, edu.berkeley.cs186.database.concurrency.LockType type) {
    lock(tableLocks, tid, table, type);
  }

  public void lockPage(Long tid, String filename, int pageNum, edu.berkeley.cs186.database.concurrency.LockType type) {
    lock(pageLocks, tid, new Pair<String, Integer>(filename, pageNum), type);
  }

  public void lockRecord(Long tid, String table, RecordId rid, edu.berkeley.cs186.database.concurrency.LockType type) {
    lock(recordLocks, tid, new Pair<String, RecordId>(table, rid), type);
  }

  public void unlockTable(Long tid, String table, edu.berkeley.cs186.database.concurrency.LockType type) {
    unlock(tableLocks, tid, table, type);
  }

  public void unlockPage(Long tid, String filename, int pageNum, edu.berkeley.cs186.database.concurrency.LockType type) {
    unlock(pageLocks, tid, new Pair<String, Integer>(filename, pageNum), type);
  }

  public void unlockRecord(Long tid, String table, RecordId rid, edu.berkeley.cs186.database.concurrency.LockType type) {
    unlock(recordLocks, tid, new Pair<String, RecordId>(table, rid), type);
  }

  public Map<Long, Set<Long>> getWaitsForGraph() {
    synchronized(waitsForGraph) {
      return waitsForGraph;
    }
  }

  private <K> void lock(Map<K, Lock> lockTable, Long tid, K k, edu.berkeley.cs186.database.concurrency.LockType type) {
    Lock lock;
    synchronized(lockTable) {
      lock = getOrDefault(lockTable, k, new Lock());
    }

    boolean blocked = lock.lock(tid, type, (Set<Long> conflicts) -> {
      synchronized(waitsForGraph) {
        waitsForGraph.put(tid, conflicts);
      }
    });

    if (blocked) {
      synchronized(waitsForGraph) {
        waitsForGraph.get(tid).clear();
      }
    }
  }

  private <K> void unlock(Map<K, Lock> lockTable, Long tid, K k, edu.berkeley.cs186.database.concurrency.LockType type) {
    Lock lock;
    synchronized(lockTable) {
      lock = getOrDefault(lockTable, k, new Lock());
    }
    lock.unlock(tid, type);
    synchronized(waitsForGraph) {
      for (Map.Entry<Long, Set<Long>> entry : waitsForGraph.entrySet()) {
        entry.getValue().remove(tid);
      }
    }
  }

  private static <K, V> V getOrDefault(Map<K, V> m, K k, V defaultValue) {
    if (m.containsKey(k)) {
      return m.get(k);
    } else {
      m.put(k, defaultValue);
      return defaultValue;
    }
  }

  // Old stuff.
  public enum LockType {SHARED, EXCLUSIVE};
  public synchronized void acquireLock(String tableName, long transNum, LockType lockType) { }
  public synchronized void releaseLock(String tableName, long transNum) { }
  public synchronized boolean holdsLock(String tableName, long transNum, LockType lockType) { return false; }
}
