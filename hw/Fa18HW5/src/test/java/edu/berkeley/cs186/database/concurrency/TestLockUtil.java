package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.LoggingLockManager;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.*;

public class TestLockUtil {
    LoggingLockManager lockManager;
    BaseTransaction[] transactions;
    LockContext dbContext;
    LockContext tableContext;
    LockContext[] pageContexts;

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.seconds(1));

    @Before
    public void setUp() {
        lockManager = new LoggingLockManager();
        transactions = new BaseTransaction[8];
        dbContext = lockManager.databaseContext();
        tableContext = dbContext.childContext("table1");
        pageContexts = new LockContext[8];
        for (int i = 0; i < transactions.length; ++i) {
            transactions[i] = new DummyTransaction(lockManager, i);
            pageContexts[i] = tableContext.childContext((long) i);
        }
    }

    @Test
    public void testRequestNullTransaction() {
        lockManager.startLog();
        LockUtil.requestLocks(null, pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(), lockManager.log);
    }

    @Test
    public void testSimpleAcquire() {
        lockManager.startLog();
        LockUtil.requestLocks(transactions[0], pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                         "acquire 0 database IS",
                         "acquire 0 database/table1 IS",
                         "acquire 0 database/table1/4 S"
                     ), lockManager.log);
    }

    @Test
    public void testSimplePromote() {
        LockUtil.requestLocks(transactions[0], pageContexts[4], LockType.S);
        lockManager.startLog();
        LockUtil.requestLocks(transactions[0], pageContexts[4], LockType.X);
        assertEquals(Arrays.asList(
                         "promote 0 database IX",
                         "promote 0 database/table1 IX",
                         "promote 0 database/table1/4 X"
                     ), lockManager.log);
    }

    @Test
    public void testSimpleEscalate() {
        LockUtil.requestLocks(transactions[0], pageContexts[4], LockType.S);
        lockManager.startLog();
        LockUtil.requestLocks(transactions[0], tableContext, LockType.S);
        assertEquals(Arrays.asList(
                         "acquire/t 0 database/table1 S",
                         "release/t 0 database/table1",
                         "release/t 0 database/table1/4"
                     ), lockManager.log);
    }

    @Test
    public void testIXBeforeIS() {
        LockUtil.requestLocks(transactions[0], pageContexts[3], LockType.X);
        lockManager.startLog();
        LockUtil.requestLocks(transactions[0], pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                         "acquire 0 database/table1/4 S"
                     ), lockManager.log);
    }

}

