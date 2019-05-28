package edu.berkeley.cs186.database.concurrency;

import java.util.Arrays;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.LoggingLockManager;
import edu.berkeley.cs186.database.common.Pair;

import org.junit.*;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

public class TestLockContext {
    LoggingLockManager lockManager;

    LockContext dbLockContext;
    LockContext tableLockContext;
    LockContext pageLockContext;

    BaseTransaction[] transactions;

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.seconds(1));

    @Before
    public void setUp() {
        lockManager = new LoggingLockManager();

        dbLockContext = new LockContext(lockManager, null, "database");
        tableLockContext = dbLockContext.childContext("table");
        pageLockContext = tableLockContext.childContext("page");

        transactions = new BaseTransaction[8];
        for (int i = 0; i < transactions.length; i++) {
            transactions[i] = new DummyTransaction(lockManager, i);
        }
    }

    @Test
    public void testSimpleAcquireFail() {
        dbLockContext.acquire(transactions[0], LockType.IS);
        try {
            tableLockContext.acquire(transactions[0], LockType.X);
            fail();
        } catch (InvalidLockException e) {
            // do nothing
        }
    }

    @Test
    public void testSimpleAcquirePass() {
        dbLockContext.acquire(transactions[0], LockType.IS);
        tableLockContext.acquire(transactions[0], LockType.S);
        Assert.assertEquals(Arrays.asList(new Pair<>(dbLockContext.getResourceName(), LockType.IS),
                                          new Pair<>(tableLockContext.getResourceName(), LockType.S)), lockManager.getLocks(transactions[0]));
    }

    @Test
    public void testTreeAcquirePass() {
        dbLockContext.acquire(transactions[0], LockType.IX);
        tableLockContext.acquire(transactions[0], LockType.IS);
        pageLockContext.acquire(transactions[0], LockType.S);

        Assert.assertEquals(Arrays.asList(new Pair<>(dbLockContext.getResourceName(), LockType.IX),
                                          new Pair<>(tableLockContext.getResourceName(), LockType.IS),
                                          new Pair<>(pageLockContext.getResourceName(), LockType.S)),
                            lockManager.getLocks(transactions[0]));
    }

    @Test
    public void testSimpleReleasePass() {
        dbLockContext.acquire(transactions[0], LockType.IS);
        tableLockContext.acquire(transactions[0], LockType.S);
        tableLockContext.release(transactions[0]);

        Assert.assertEquals(Arrays.asList(new Pair<>(dbLockContext.getResourceName(), LockType.IS)),
                            lockManager.getLocks(transactions[0]));
    }

    @Test
    public void testSimpleReleaseFail() {
        dbLockContext.acquire(transactions[0], LockType.IS);
        tableLockContext.acquire(transactions[0], LockType.S);
        try {
            dbLockContext.release(transactions[0]);
            fail();
        } catch (InvalidLockException e) {
            // do nothing
        }
    }

    @Test
    public void testSharedPage() {
        BaseTransaction t1 = transactions[1];
        BaseTransaction t2 = transactions[2];

        LockContext r0 = tableLockContext;
        LockContext r1 = pageLockContext;

        dbLockContext.acquire(t1, LockType.IS);
        dbLockContext.acquire(t2, LockType.IS);
        r0.acquire(t1, LockType.IS);
        r0.acquire(t2, LockType.IS);
        r1.acquire(t1, LockType.S);
        r1.acquire(t2, LockType.S);

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t2, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
        assertTrue(TestLockManager.holds(lockManager, t2, r1.getResourceName(), LockType.S));
    }

    @Test
    public void testSandIS() {
        BaseTransaction t1 = transactions[1];
        BaseTransaction t2 = transactions[2];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;

        r0.acquire(t1, LockType.S);
        r0.acquire(t2, LockType.IS);
        r1.acquire(t2, LockType.S);
        r0.release(t1);

        assertTrue(TestLockManager.holds(lockManager, t2, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t2, r1.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.S));
    }

    @Test
    public void testSharedIntentConflict() {
        BaseTransaction t1 = transactions[1];
        BaseTransaction t2 = transactions[2];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;

        r0.acquire(t1, LockType.IS);
        r0.acquire(t2, LockType.IX);
        r1.acquire(t1, LockType.S);
        r1.acquire(t2, LockType.X);

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t2, r0.getResourceName(), LockType.IX));
        assertTrue(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t2, r1.getResourceName(), LockType.X));
    }

    @Test
    public void testSharedIntentConflictRelease() {
        BaseTransaction t1 = transactions[1];
        BaseTransaction t2 = transactions[2];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;

        r0.acquire(t1, LockType.IS);
        r0.acquire(t2, LockType.IX);
        r1.acquire(t1, LockType.S);
        r1.acquire(t2, LockType.X);
        r1.release(t1);

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t2, r0.getResourceName(), LockType.IX));
        assertFalse(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
        assertTrue(TestLockManager.holds(lockManager, t2, r1.getResourceName(), LockType.X));
    }

    @Test
    public void testSimplePromote() {
        BaseTransaction t1 = transactions[1];
        dbLockContext.acquire(t1, LockType.S);
        dbLockContext.promote(t1, LockType.X);
        assertTrue(TestLockManager.holds(lockManager, t1, dbLockContext.getResourceName(), LockType.X));
    }

    @Test
    public void testEscalateFail() {
        BaseTransaction t1 = transactions[1];

        LockContext r0 = dbLockContext;

        r0.acquire(t1, LockType.IS);
        try {
            r0.escalate(t1);
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }
    }

    @Test
    public void testEscalateS() {
        BaseTransaction t1 = transactions[1];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;

        r0.acquire(t1, LockType.IS);
        r1.acquire(t1, LockType.S);
        r0.escalate(t1);

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
    }

    @Test
    public void testEscalateMultipleS() {
        BaseTransaction t1 = transactions[1];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;
        LockContext r2 = dbLockContext.childContext("table2");
        LockContext r3 = dbLockContext.childContext("table3");

        r0.capacity(4);

        r0.acquire(t1, LockType.IS);
        r1.acquire(t1, LockType.S);
        r2.acquire(t1, LockType.IS);
        r3.acquire(t1, LockType.S);

        assertEquals(3.0 / 4, r0.saturation(t1), 1e-6);
        r0.escalate(t1);
        assertEquals(0.0, r0.saturation(t1), 1e-6);

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t1, r2.getResourceName(), LockType.IS));
        assertFalse(TestLockManager.holds(lockManager, t1, r3.getResourceName(), LockType.S));
    }

    @Test
    public void testGetLockType() {
        BaseTransaction t1 = transactions[1];
        BaseTransaction t2 = transactions[2];
        BaseTransaction t3 = transactions[3];

        dbLockContext.acquire(t1, LockType.S);
        dbLockContext.acquire(t2, LockType.IS);
        dbLockContext.acquire(t3, LockType.IS);

        tableLockContext.acquire(t2, LockType.S);
        tableLockContext.acquire(t3, LockType.IS);

        pageLockContext.acquire(t3, LockType.S);

        assertEquals(LockType.S, pageLockContext.getGlobalLockType(t1));
        assertEquals(LockType.S, pageLockContext.getGlobalLockType(t2));
        assertEquals(LockType.S, pageLockContext.getGlobalLockType(t3));
        assertEquals(null, pageLockContext.getLocalLockType(t1));
        assertEquals(null, pageLockContext.getLocalLockType(t2));
        assertEquals(LockType.S, pageLockContext.getLocalLockType(t3));
    }

    @Test
    public void testReadonly() {
        dbLockContext.disableChildLocks();
        LockContext tableContext = dbLockContext.childContext("table2");
        BaseTransaction t1 = transactions[1];
        dbLockContext.acquire(t1, LockType.IX);
        try {
            tableContext.acquire(t1, LockType.IX);
            fail();
        } catch (UnsupportedOperationException e) {
            // do nothing
        }
        try {
            tableContext.release(t1);
            fail();
        } catch (UnsupportedOperationException e) {
            // do nothing
        }
        try {
            tableContext.promote(t1, LockType.IX);
            fail();
        } catch (UnsupportedOperationException e) {
            // do nothing
        }
        try {
            tableContext.escalate(t1);
            fail();
        } catch (UnsupportedOperationException e) {
            // do nothing
        }
    }

    @Test
    public void testSaturation() {
        LockContext tableContext = dbLockContext.childContext("table2");
        BaseTransaction t1 = transactions[1];
        dbLockContext.capacity(10);
        dbLockContext.acquire(t1, LockType.IX);
        tableContext.acquire(t1, LockType.IS);
        assertEquals(0.1, dbLockContext.saturation(t1), 1E-6);
        tableContext.promote(t1, LockType.IX);
        assertEquals(0.1, dbLockContext.saturation(t1), 1E-6);
        tableContext.release(t1);
        assertEquals(0.0, dbLockContext.saturation(t1), 1E-6);
        tableContext.acquire(t1, LockType.IS);
        dbLockContext.escalate(t1);
        assertEquals(0.0, dbLockContext.saturation(t1), 1E-6);
    }
}
