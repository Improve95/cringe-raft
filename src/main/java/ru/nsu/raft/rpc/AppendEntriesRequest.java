package ru.nsu.raft.rpc;

public record AppendEntriesRequest(long term, String leaderId) {
    public AppendEntriesRequest {
        if (term < 0 || leaderId == null || leaderId.isBlank()) {
            throw new IllegalArgumentException("Invalid AppendEntriesRequest request");
        }
    }
}
