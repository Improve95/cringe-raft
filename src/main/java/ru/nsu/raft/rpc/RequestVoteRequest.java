package ru.nsu.raft.rpc;

public record RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    public RequestVoteRequest {
        if (term < 0
            || lastLogIndex < 0
            || lastLogTerm < 0
            || lastLogTerm > term
            || (lastLogIndex == 0 && lastLogTerm != 0)
            || candidateId == null
            || candidateId.isBlank()) {
            throw new IllegalArgumentException("Invalid RequestVote request");
        }
    }
}
