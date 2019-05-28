package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

public class DummyLockContext extends LockContext {
    public DummyLockContext() {
        this(null);
    }

    public DummyLockContext(LockContext parent) {
        super(new DummyLockManager(), parent, null);
    }

    @Override
    public void acquire(BaseTransaction transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException { }

    @Override
    public void release(BaseTransaction transaction)
    throws NoLockHeldException, InvalidLockException { }

    @Override
    public void promote(BaseTransaction transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException { }

    @Override
    public void escalate(BaseTransaction transaction) throws NoLockHeldException { }

    @Override
    public void disableChildLocks() { }

    @Override
    public LockContext childContext(Object name) { return new DummyLockContext(this); }

    @Override
    public int capacity() {
        return 0;
    }

    @Override
    public void capacity(int capacity) {
    }

    @Override
    public double saturation(BaseTransaction transaction) {
        return 0.0;
    }
}

