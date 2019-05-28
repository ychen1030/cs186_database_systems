package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.LoggingLockManager;
import edu.berkeley.cs186.database.common.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import sun.rmi.runtime.Log;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestLockManager {
    LoggingLockManager lockman;
    BaseTransaction[] transactions;
    ResourceName dbResource;
    ResourceName[] tables;

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.seconds(1));

    public static boolean holds(LockManager lockman, BaseTransaction transaction, ResourceName name,
                                LockType type) {
        List<Pair<ResourceName, LockType>> locks = lockman.getLocks(transaction);
        if (locks == null) {
            return false;
        }
        for (Pair<ResourceName, LockType> lock : locks) {
            if (lock.getFirst() == name && lock.getSecond() == type) {
                return true;
            }
        }
        return false;
    }

    @Before
    public void setUp() {
        lockman = new LoggingLockManager();
        transactions = new BaseTransaction[8];
        dbResource = new ResourceName("database");
        tables = new ResourceName[transactions.length];
        for (int i = 0; i < transactions.length; ++i) {
            transactions[i] = new DummyTransaction(lockman, i);
            tables[i] = new ResourceName(Arrays.asList("database"), "table" + i);
        }
    }

    @Test
    public void testSimpleAcquireRelease() {
        lockman.acquireAndRelease(transactions[0], tables[0], LockType.S, Arrays.asList());
        lockman.acquireAndRelease(transactions[0], tables[1], LockType.S, Arrays.asList(tables[0]));
        assertEquals(null, lockman.getLockType(transactions[0], tables[0]));
        assertEquals(Arrays.asList(), lockman.getLocks(tables[0]));
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[1]));
        assertEquals(Arrays.asList(new Pair<>(0L, LockType.S)), lockman.getLocks(tables[1]));
    }

    @Test
    public void testAcquireReleaseQueue() {
        lockman.acquireAndRelease(transactions[0], tables[0], LockType.X, Arrays.asList());
        lockman.acquireAndRelease(transactions[1], tables[1], LockType.X, Arrays.asList());
        lockman.acquireAndRelease(transactions[0], tables[1], LockType.X, Arrays.asList(tables[0]));
        assertEquals(LockType.X, lockman.getLockType(transactions[0], tables[0]));
        assertEquals(Arrays.asList(new Pair<>(0L, LockType.X)), lockman.getLocks(tables[0]));
        assertEquals(null, lockman.getLockType(transactions[0], tables[1]));
        assertEquals(Arrays.asList(new Pair<>(1L, LockType.X)), lockman.getLocks(tables[1]));
        assertTrue(transactions[0].getBlocked());
    }

    @Test
    public void testAcquireReleaseDuplicateLock() {
        lockman.acquireAndRelease(transactions[0], tables[0], LockType.X, Arrays.asList());
        try {
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.X, Arrays.asList());
            fail();
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }
    }

    @Test
    public void testAcquireReleaseNotHeld() {
        lockman.acquireAndRelease(transactions[0], tables[0], LockType.X, Arrays.asList());
        try {
            lockman.acquireAndRelease(transactions[0], tables[2], LockType.X, Arrays.asList(tables[0],
                                      tables[1]));
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }
    }

    @Test
    public void testAcquireReleaseDowngrade() {
        lockman.acquireAndRelease(transactions[0], tables[0], LockType.X, Arrays.asList());
        lockman.acquireAndRelease(transactions[0], tables[0], LockType.S, Arrays.asList(tables[0]));
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[0]));
        assertEquals(Arrays.asList(new Pair<>(0L, LockType.S)), lockman.getLocks(tables[0]));
        assertFalse(transactions[0].getBlocked());
    }

    @Test
    public void testSimpleAcquireLock() {
        lockman.acquire(transactions[0], tables[0], LockType.S);
        lockman.acquire(transactions[1], tables[1], LockType.X);
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[0]));
        assertEquals(Arrays.asList(new Pair<>(0L, LockType.S)), lockman.getLocks(tables[0]));
        assertEquals(LockType.X, lockman.getLockType(transactions[1], tables[1]));
        assertEquals(Arrays.asList(new Pair<>(1L, LockType.X)), lockman.getLocks(tables[1]));
    }

    @Test
    public void testSimpleAcquireLockFail() {
        BaseTransaction t1 = transactions[0];

        ResourceName r1 = dbResource;

        lockman.acquire(t1, r1, LockType.X);
        try {
            lockman.acquire(t1, r1, LockType.X);
            fail();
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }
    }

    @Test
    public void testSimpleReleaseLock() {
        lockman.acquire(transactions[0], dbResource, LockType.X);
        lockman.release(transactions[0], dbResource);
        assertEquals(null, lockman.getLockType(transactions[0], dbResource));
        assertEquals(Arrays.asList(), lockman.getLocks(dbResource));
    }

    @Test
    public void testSimpleConflict() {
        lockman.acquire(transactions[0], dbResource, LockType.X);
        lockman.acquire(transactions[1], dbResource, LockType.X);
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(null, lockman.getLockType(transactions[1], dbResource));
        assertEquals(Arrays.asList(new Pair<>(0L, LockType.X)), lockman.getLocks(dbResource));
        assertFalse(transactions[0].getBlocked());
        assertTrue(transactions[1].getBlocked());

        lockman.release(transactions[0], dbResource);
        assertEquals(null, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.X, lockman.getLockType(transactions[1], dbResource));
        assertEquals(Arrays.asList(new Pair<>(1L, LockType.X)), lockman.getLocks(dbResource));
        assertFalse(transactions[0].getBlocked());
        assertFalse(transactions[1].getBlocked());
    }

    @Test
    public void testSimplePromoteLock() {
        lockman.acquire(transactions[0], dbResource, LockType.S);
        lockman.promote(transactions[0], dbResource, LockType.X);
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(Arrays.asList(new Pair<>(0L, LockType.X)), lockman.getLocks(dbResource));
    }

    @Test
    public void testSimplePromoteLockNotHeld() {
        try {
            lockman.promote(transactions[0], dbResource, LockType.X);
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }
    }

    @Test
    public void testSimplePromoteLockAlreadyHeld() {
        lockman.acquire(transactions[0], dbResource, LockType.X);
        try {
            lockman.promote(transactions[0], dbResource, LockType.X);
            fail();
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }
    }

    @Test
    public void testFIFOQueueLocks() {
        lockman.acquire(transactions[0], dbResource, LockType.X);
        lockman.acquire(transactions[1], dbResource, LockType.X);
        lockman.acquire(transactions[2], dbResource, LockType.X);

        assertTrue(holds(lockman, transactions[0], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[1], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[2], dbResource, LockType.X));

        lockman.release(transactions[0], dbResource);

        assertFalse(holds(lockman, transactions[0], dbResource, LockType.X));
        assertTrue(holds(lockman, transactions[1], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[2], dbResource, LockType.X));

        lockman.release(transactions[1], dbResource);

        assertFalse(holds(lockman, transactions[0], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[1], dbResource, LockType.X));
        assertTrue(holds(lockman, transactions[2], dbResource, LockType.X));
    }

    @Test
    public void testStatusUpdates() {
        BaseTransaction t1 = transactions[0];
        BaseTransaction t2 = transactions[1];

        ResourceName r1 = dbResource;

        lockman.acquire(t1, r1, LockType.X);
        lockman.acquire(t2, r1, LockType.X);

        assertTrue(holds(lockman, t1, r1, LockType.X));
        assertFalse(holds(lockman, t2, r1, LockType.X));
        assertFalse(t1.getBlocked());
        assertTrue(t2.getBlocked());

        lockman.release(t1, r1);

        assertFalse(holds(lockman, t1, r1, LockType.X));
        assertTrue(holds(lockman, t2, r1, LockType.X));
        assertFalse(t1.getBlocked());
        assertFalse(t2.getBlocked());
    }

    @Test
    public void testTableEventualUpgrade() {
        BaseTransaction t1 = transactions[0];
        BaseTransaction t2 = transactions[1];

        ResourceName r1 = dbResource;

        lockman.acquire(t1, r1, LockType.S);
        lockman.acquire(t2, r1, LockType.S);

        assertTrue(holds(lockman, t1, r1, LockType.S));
        assertTrue(holds(lockman, t2, r1, LockType.S));

        lockman.promote(t1, r1, LockType.X);

        assertTrue(holds(lockman, t1, r1, LockType.S));
        assertFalse(holds(lockman, t1, r1, LockType.X));
        assertTrue(holds(lockman, t2, r1, LockType.S));

        lockman.release(t2, r1);

        assertTrue(holds(lockman, t1, r1, LockType.X));
        assertFalse(holds(lockman, t2, r1, LockType.S));

        lockman.release(t1, r1);

        assertFalse(holds(lockman, t1, r1, LockType.X));
        assertFalse(holds(lockman, t2, r1, LockType.S));
    }

    @Test
    public void testIntentBlockedAcquire() {
        BaseTransaction t1 = transactions[0];
        BaseTransaction t2 = transactions[1];

        ResourceName r0 = dbResource;

        lockman.acquire(t1, r0, LockType.S);
        lockman.acquire(t2, r0, LockType.IX);

        assertTrue(holds(lockman, t1, r0, LockType.S));
        assertFalse(holds(lockman, t2, r0, LockType.IX));

        lockman.release(t1, r0);

        assertFalse(holds(lockman, t1, r0, LockType.S));
        assertTrue(holds(lockman, t2, r0, LockType.IX));
    }

    @Test
    public void testBlockedTransactionRelease() {
        BaseTransaction t1 = transactions[0];
        BaseTransaction t2 = transactions[1];

        ResourceName r1 = dbResource;
        ResourceName r2 = new ResourceName("database2");

        lockman.acquire(t1, r1, LockType.X);
        lockman.acquire(t2, r2, LockType.S);
        lockman.acquire(t2, r1, LockType.S);

        assertTrue(t2.getBlocked());

        try {
            lockman.release(t2, r1);
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }
    }

    @Test
    public void testReleaseUnheldLock() {
        BaseTransaction t1 = transactions[0];
        try {
            lockman.release(t1, dbResource);
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }
    }

}

