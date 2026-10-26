package ru.nsu.raft.node.event;

import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

public record RequestVoteReceived(RequestVoteRequest request, CompletableFuture<RequestVoteResponse> reply) implements RaftEvent {
    @Override
    public void fail(Throwable error) {
        reply.completeExceptionally(error);
    }
}
