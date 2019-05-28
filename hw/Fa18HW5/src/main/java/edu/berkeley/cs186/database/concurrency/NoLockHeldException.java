package edu.berkeley.cs186.database.concurrency;

public class NoLockHeldException extends RuntimeException {
    private String message;

    public NoLockHeldException(String message) {
        this.message = message;
    }

    public NoLockHeldException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

