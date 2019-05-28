package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 */
public class LockManager {
    // These members are given as a suggestion. You are not required to use them, and may
    // delete them and add members as you see fit.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    private Map<ResourceName, List<Pair<Long, Lock>>> resourceLocks = new HashMap<>();
    private Deque<LockRequest> waitingQueue = new ArrayDeque<>();

    // You should not modify this.
    protected Map<Object, LockContext> contexts = new HashMap<>();

    public LockManager() {}

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public LockContext databaseContext() {
        if (!contexts.containsKey("database")) {
            contexts.put("database", new LockContext(this, null, "database"));
        }
        return contexts.get("database");
    }

    /**
     * Create a lock context with no parent. Cannot be called "database".
     */
    public LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock. No error checking is performed for holding
     * requisite parent locks or freeing dependent child locks. Blocks the transaction and
     * places it in queue if the requested lock is not compatible with another transaction's
     * lock on the resource. Unblocks and unqueues all transactions that can be unblocked
     * after releasing locks in RELEASELOCKS, in order of lock request.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        boolean downgrade = false;
        try {
            acquire(transaction, name, lockType);
        } catch (DuplicateLockRequestException e) {
            int index = releaseLocks.indexOf(name);
            if (index == -1) throw e;
            else downgrade = true;
        }

        if (!transaction.getBlocked()) {
            for (ResourceName rn : releaseLocks) release(transaction, rn);
            if (downgrade) acquire(transaction, name, lockType);
        } else {
            List<Lock> list = new ArrayList<>();
            for (ResourceName r: releaseLocks) list.add(new Lock(r, null));
            LockRequest request = new LockRequest(transaction, new Lock(name, lockType), list);
            waitingQueue.add(request);
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION. No error
     * checking is performed for holding requisite parent locks. Blocks the
     * transaction and places it in queue if the requested lock is not compatible
     * with another transaction's lock on the resource.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        List<Pair<Long, Lock>> locks = new ArrayList<>();
        Long transNum = transaction.getTransNum();
        Lock newLock = new Lock(name, lockType);
        if (resourceLocks.containsKey(name)) {
            locks = resourceLocks.get(name);
            for (Pair<Long, Lock> lock : locks) {
                if (lock.getFirst().equals(transNum)) {
                    throw new DuplicateLockRequestException("");
//                if (lock.getFirst().equals(transNum) && LockType.substitutable(lockType, lock.getSecond().lockType)) {
//                    throw new DuplicateLockRequestException("Duplicate");
//                } else if (lock.getFirst().equals(transNum)) {
//                    lock.getSecond().lockType = lockType;
//                    resourceLocks.put(name, locks);
//                    List<Lock> list = transactionLocks.get(transNum);
//                    for (Lock lk : list) if (lk.name == name) lk.lockType = lockType;
//                    transactionLocks.put(transNum, list);
//                    break;
                } else if (!LockType.compatible(lock.getSecond().lockType, lockType)) {
                    transaction.block();
                    waitingQueue.add(new LockRequest(transaction, newLock));
                    return;
                }
            }
        }
        List<Lock> tranLocks = transactionLocks.getOrDefault(transNum, new ArrayList<>());
        tranLocks.add(newLock);
        transactionLocks.put(transNum, tranLocks);
        locks.add(new Pair<>(transNum, newLock));
        resourceLocks.put(name, locks);
    }

    /**
     * Release TRANSACTION's lock on NAME. No error checking is performed for
     * freeing dependent child locks. Unblocks and unqueues all transactions
     * that can be unblocked, in order of lock request.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException {
        Long transNum = transaction.getTransNum();
        if (!transactionLocks.containsKey(transNum)) throw new NoLockHeldException("NoLock");

        boolean found = false;
        List<Lock> tranlocks = transactionLocks.get(transNum);
        for (Lock lock : tranlocks) {
            if (lock.name == name) {
                tranlocks.remove(lock);
                transactionLocks.put(transNum, tranlocks);
                found = true;
                break;
            }
        }
        if (!found) throw new NoLockHeldException("NoLock Held");

        List<Pair<Long, Lock>> locks = resourceLocks.get(name);
        for (Pair<Long, Lock> lock : locks) {
            if (lock.getFirst().equals(transNum)) {
                locks.remove(lock);
                resourceLocks.put(name, locks);
                break;
            }
        }

        for (LockRequest req : waitingQueue) {
            if (req.lock.name == name) {
                boolean unblock = true;
                boolean promote = false;
                for (Pair<Long, Lock> lock: resourceLocks.get(name)) {
                    if (lock.getFirst() == req.transaction.getTransNum() ||
                            !LockType.compatible(lock.getSecond().lockType, req.lock.lockType)) unblock = false;
                    if (lock.getFirst() == req.transaction.getTransNum() &&
                            LockType.substitutable(req.lock.lockType, lock.getSecond().lockType)) promote = true;
                }
                if (unblock || promote) {
                    waitingQueue.remove(req);
                    req.transaction.unblock();
                    if (unblock) acquire(req.transaction, name, req.lock.lockType);
                    else promote(req.transaction, name, req.lock.lockType);
                    if (!req.releasedLocks.isEmpty()) {
                        for (Lock lk : req.releasedLocks) release(transaction, lk.name);
                    }
                }
            }
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE. No error checking is
     * performed for holding requisite locks. Blocks the transaction and places
     * TRANSACTION in the **front** of the queue if the request cannot be
     * immediately granted (i.e. another transaction holds a conflicting lock). A
     * lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (!resourceLocks.containsKey(name)) throw new NoLockHeldException("");

        Lock oldLock = null;
        Long transNum = transaction.getTransNum();
        for (Pair<Long, Lock> lock : resourceLocks.get(name)) {
            if (lock.getFirst().equals(transNum)) {
                oldLock = lock.getSecond();
            } else if (!LockType.compatible(lock.getSecond().lockType, newLockType)) {
                waitingQueue.addFirst(new LockRequest(transaction, new Lock(name, newLockType)));
                return;
            }
        }

        if (oldLock == null) throw new NoLockHeldException("");
        if (oldLock.lockType == newLockType) throw new DuplicateLockRequestException("");
        if (!LockType.substitutable(newLockType, oldLock.lockType)) throw new InvalidLockException("");

        List<Lock> tranLocks = transactionLocks.get(transNum);
        for (Lock lock : tranLocks) {
            if (lock.name == name) {
                lock.lockType = newLockType;
                transactionLocks.put(transNum, tranLocks);
                break;
            }
        }

        List<Pair<Long, Lock>> locks = resourceLocks.get(name);
        for (Pair<Long, Lock> lock : locks) {
            if (lock.getFirst().equals(transNum)) {
                lock.getSecond().lockType = newLockType;
                resourceLocks.put(name, locks);
                break;
            }
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME, or null if no lock is
     * held.
     */
    public LockType getLockType(BaseTransaction transaction, ResourceName name) {
        if (resourceLocks.containsKey(name)) {
            List<Pair<Long, Lock>> locks = resourceLocks.get(name);
            for (Pair<Long, Lock> lock : locks) {
                if (lock.getFirst() == transaction.getTransNum()) {
                    return lock.getSecond().lockType;
                }
            }
        }
        return null;
    }

    /**
     * Returns the list of transactions ids and lock types for locks held on
     * NAME, in order of acquisition. A promotion should count as acquired
     * at the original time.
     */
    public List<Pair<Long, LockType>> getLocks(ResourceName name) {
        List<Pair<Long, LockType>> list = new ArrayList<>();
        if (resourceLocks.containsKey(name)) {
            List<Pair<Long, Lock>> locks = resourceLocks.get(name);
            for (Pair<Long, Lock> lock : locks) {
                list.add(new Pair<>(lock.getFirst(), lock.getSecond().lockType));
            }
        }
        return list;
    }

    /**
     * Returns the list of resource names and lock types for locks held by
     * TRANSACTION, in order of acquisition. A promotion should count as acquired
     * at the original time.
     */
    public List<Pair<ResourceName, LockType>> getLocks(BaseTransaction transaction) {
        List<Pair<ResourceName, LockType>> list = new ArrayList<>();
        if (transactionLocks.containsKey(transaction.getTransNum())) {
            List<Lock> locks = transactionLocks.get(transaction.getTransNum());
            for (Lock lock : locks) {
                list.add(new Pair<>(lock.name, lock.lockType));
            }
        }
        return list;
    }
}
