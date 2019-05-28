package edu.berkeley.cs186.database.concurrency;

public class InvalidLockException extends RuntimeException {
    private String message;

    public InvalidLockException(String message) {
        this.message = message;
    }

    public InvalidLockException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

