package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;

public class LockUtil {
    /**
     * Ensure that TRANSACTION can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType must be one of LockType.S, LockType.X, and behavior is unspecified
     * if an intent lock is passed in to this method (you can do whatever you want in this case).
     *
     * If TRANSACTION is null, this method should do nothing.
     */
    public static void requestLocks(BaseTransaction transaction, LockContext lockContext,
                                    LockType lockType) {
        if (transaction == null) return;
        if (lockType != LockType.X && lockType != LockType.S) return;

        if (!LockType.substitutable(lockContext.getGlobalLockType(transaction), lockType)) {
            List<LockContext> parents = allParents(lockContext.parent);
            LockType parentLock = LockType.parentLock(lockType);
            for (LockContext current : parents) {
                if (!LockType.substitutable(current.getLocalLockType(transaction), parentLock)) {
                    if (current.getLocalLockType(transaction) == null) current.acquire(transaction, parentLock);
                    else current.promote(transaction, parentLock);
                }
            }

            if (lockContext.numChildLocks.getOrDefault(transaction.getTransNum(), 0) != 0 &&
                    childLeast(transaction, lockContext) == lockType) {
                lockContext.escalate(transaction);
            } else if (lockContext.getLocalLockType(transaction) != null &&
                    LockType.substitutable(lockType, lockContext.getLocalLockType(transaction))) {
                lockContext.promote(transaction, lockType);
            } else {
                lockContext.acquire(transaction, lockType);
            }
        }
    }

    private static List<LockContext> allParents(LockContext lockContext) {
        List<LockContext> parents = new ArrayList<>();
        while (lockContext != null) {
            parents.add(0, lockContext);
            lockContext = lockContext.parentContext();
        }
        return parents;
    }

    private static LockType childLeast(BaseTransaction transaction, LockContext lockContext) {
        LockType childLeast = null;
        for (LockContext child : lockContext.children.values()) {
            if (!LockType.substitutable(childLeast, child.getLocalLockType(transaction))) {
                childLeast = child.getLocalLockType(transaction);
            }
        }
        return childLeast;
    }

    // TODO(hw5): add helper methods as you see fit
}
