package edu.berkeley.cs186.database.concurrency;

import java.util.Arrays;
import java.util.List;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.common.Pair;

/**
 * Dummy lock manager that does no locking or error checking.
 *
 * Used for non-locking-related tests to disable locking.
 */
public class DummyLockManager extends LockManager {
    public DummyLockManager() { }

    @Override
    public LockContext databaseContext() {
        return new DummyLockContext(null);
    }

    @Override
    public LockContext orphanContext(Object name) {
        return new DummyLockContext(null);
    }

    @Override
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException { }

    @Override
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException { }

    @Override
    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException { }

    @Override
    public LockType getLockType(BaseTransaction transaction, ResourceName name) {
        return null;
    }

    @Override
    public List<Pair<Long, LockType>> getLocks(ResourceName name) {
        return Arrays.asList();
    }

    @Override
    public List<Pair<ResourceName, LockType>> getLocks(BaseTransaction transaction) {
        return Arrays.asList();
    }
}

