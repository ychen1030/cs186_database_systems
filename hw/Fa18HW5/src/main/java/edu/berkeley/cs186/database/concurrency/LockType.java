package edu.berkeley.cs186.database.concurrency;

import com.sun.org.apache.regexp.internal.RE;
import edu.berkeley.cs186.database.table.Record;

import java.util.Arrays;
import java.util.List;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX; // shared intention exclusive

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible. A null represents no lock.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) return true;
        else if (a == S) return b == S || b == IS;
        else if (a == IS) return !(b == X);
        else if (a == IX) return b == IS || b == IX;
        else if (a == SIX) return b == IS;
        else return false;
    }

    /**
     * This method returns the least permissive lock on the parent resource
     * that must be held for a lock of type A to be granted. A null
     * represents no lock.
     */
    public static LockType parentLock(LockType a) {
        if (a == S || a == IS) return IS;
        else if (a == X || a == IX || a == SIX) return IX;
        else return a;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do). A null represents no lock.
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null) return true;
        else if (substitute == null) return false;
        else if (required == S) return substitute == S || substitute == X || substitute == SIX;
        else if (required == X) return substitute == X;
        else if (required == IS) return true;
        else if (required == IX) return substitute == X || substitute == IX || substitute == SIX;
        else if (required == SIX) return substitute == SIX;
        else return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
};

