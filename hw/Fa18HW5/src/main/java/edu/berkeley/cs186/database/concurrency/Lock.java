package edu.berkeley.cs186.database.concurrency;

class Lock {
    public ResourceName name;
    public LockType lockType;

    public Lock(ResourceName name, LockType lockType) {
        this.name = name;
        this.lockType = lockType;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Lock) {
            Lock l = (Lock) other;
            return l.name.equals(name) && lockType == l.lockType;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 37 * name.hashCode() + lockType.hashCode();
    }

    @Override
    public String toString() {
        return lockType.toString() + "(" + name.toString() + ")";
    }
}
