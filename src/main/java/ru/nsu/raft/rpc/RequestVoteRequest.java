package ru.nsu.raft.rpc;

public record RequestVoteRequest(long term, String candidateId) {
    public RequestVoteRequest {
        if (term < 0 || candidateId == null || candidateId.isBlank()) {
            throw new IllegalArgumentException("Invalid RequestVoteRequest request");
        }
    }
}
