package edu.berkeley.cs186.database.concurrency;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

public class TestLockType {
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis(200));

    @Test
    public void testCompatibleNL() {
        assertTrue(LockType.compatible(null, null));
        assertTrue(LockType.compatible(null, LockType.S));
        assertTrue(LockType.compatible(null, LockType.X));
        assertTrue(LockType.compatible(null, LockType.IS));
        assertTrue(LockType.compatible(null, LockType.IX));
        assertTrue(LockType.compatible(null, LockType.SIX));
        assertTrue(LockType.compatible(LockType.S, null));
        assertTrue(LockType.compatible(LockType.X, null));
        assertTrue(LockType.compatible(LockType.IS, null));
        assertTrue(LockType.compatible(LockType.IX, null));
        assertTrue(LockType.compatible(LockType.SIX, null));
    }

    @Test
    public void testCompatibleS() {
        assertTrue(LockType.compatible(LockType.S, LockType.S));
        assertFalse(LockType.compatible(LockType.S, LockType.X));
        assertTrue(LockType.compatible(LockType.S, LockType.IS));
        assertFalse(LockType.compatible(LockType.S, LockType.IX));
        assertFalse(LockType.compatible(LockType.S, LockType.SIX));
        assertFalse(LockType.compatible(LockType.X, LockType.S));
        assertTrue(LockType.compatible(LockType.IS, LockType.S));
        assertFalse(LockType.compatible(LockType.IX, LockType.S));
        assertFalse(LockType.compatible(LockType.SIX, LockType.S));
    }

    @Test
    public void testParentReal() {
        assertEquals(null, LockType.parentLock(null));
        assertEquals(LockType.IS, LockType.parentLock(LockType.S));
        assertEquals(LockType.IX, LockType.parentLock(LockType.X));
    }

    @Test
    public void testSubstitutableReal() {
        assertTrue(LockType.substitutable(LockType.S, LockType.S));
        assertTrue(LockType.substitutable(LockType.X, LockType.S));
        assertFalse(LockType.substitutable(LockType.IS, LockType.S));
        assertFalse(LockType.substitutable(LockType.IX, LockType.S));
        assertTrue(LockType.substitutable(LockType.SIX, LockType.S));
        assertFalse(LockType.substitutable(LockType.S, LockType.X));
        assertTrue(LockType.substitutable(LockType.X, LockType.X));
        assertFalse(LockType.substitutable(LockType.IS, LockType.X));
        assertFalse(LockType.substitutable(LockType.IX, LockType.X));
        assertFalse(LockType.substitutable(LockType.SIX, LockType.X));
    }

}

