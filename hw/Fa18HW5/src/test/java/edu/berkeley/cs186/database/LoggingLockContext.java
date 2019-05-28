package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockManager;

import java.util.*;

public class LoggingLockContext extends LockContext {
    private boolean allowDisable = true;

    public LoggingLockContext(LoggingLockManager lockman, LockContext parent, Object name) {
        super(lockman, parent, name);
    }

    private LoggingLockContext(LoggingLockManager lockman, LockContext parent, Object name,
                               boolean readonly) {
        super(lockman, parent, name, readonly);
    }

    /**
     * Disables locking children. This causes all child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    @Override
    public void disableChildLocks() {
        if (this.allowDisable) {
            super.disableChildLocks();
        }
        ((LoggingLockManager) lockman).emit("disable-children " + name);
    }

    /**
     * Gets the context for the child with name NAME.
     */
    @Override
    public LockContext childContext(Object name) {
        if (!this.children.containsKey(name)) {
            this.children.put(name, new LoggingLockContext((LoggingLockManager) lockman, this, name,
                              this.childLocksDisabled || this.readonly));
        }
        return this.children.get(name);
    }

    /**
     * Sets the capacity (number of children).
     */
    @Override
    public void capacity(int capacity) {
        super.capacity(capacity);
        ((LoggingLockManager) lockman).emit("set-capacity " + name + " " + capacity);
    }

    public void allowDisableChildLocks(boolean allow) {
        this.allowDisable = allow;
    }
}

