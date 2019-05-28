package edu.berkeley.cs186.database.concurrency;

public class DuplicateLockRequestException extends RuntimeException {
    private String message;

    public DuplicateLockRequestException(String message) {
        this.message = message;
    }

    public DuplicateLockRequestException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

