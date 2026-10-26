package ru.nsu.raft.rpc;

public record RequestVoteResponse(long term, boolean voteGranted) {
}
