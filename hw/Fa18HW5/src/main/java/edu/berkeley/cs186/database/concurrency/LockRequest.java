package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

import java.util.Arrays;
import java.util.List;

/**
 * Represents a lock request on the queue, for
 * TRANSACTION requesting LOCK, with all locks in releasedLocks
 * to be released **after** granting the lock, but **before**
 * unblocking the transaction.
 */
class LockRequest {
    public BaseTransaction transaction;
    public Lock lock;
    public List<Lock> releasedLocks;

    public LockRequest(BaseTransaction transaction, Lock lock) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = Arrays.asList();
    }

    public LockRequest(BaseTransaction transaction, Lock lock, List<Lock> releasedLocks) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = releasedLocks;
    }

    @Override
    public String toString() {
        return "Request for " + lock.toString() + " (releasing " + releasedLocks.toString() + ")";
    }
}