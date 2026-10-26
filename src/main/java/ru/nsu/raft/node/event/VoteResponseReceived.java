package ru.nsu.raft.node.event;

import ru.nsu.raft.rpc.RequestVoteResponse;

public record VoteResponseReceived(String from, RequestVoteResponse response) implements RaftEvent {
}
