package ru.nsu.raft.api;

public class NotLeaderException extends RuntimeException {
    private final String leaderId;
    public NotLeaderException(String leaderId) {
        super("Node is not the leader; retry using leaderId");
        this.leaderId = leaderId;
    }
    public String leaderId() { return leaderId; }
}
