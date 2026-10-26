package ru.nsu.raft.api;

public final class NotLeaderException extends RuntimeException {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super("Submit the command to the current leader");
        this.leaderId = leaderId;
    }

    public String leaderId() {
        return leaderId;
    }
}
