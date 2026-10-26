package ru.nsu.raft.node.event;

import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;

import java.util.concurrent.CompletableFuture;

public record AppendEntriesReceived(
    AppendEntriesRequest request,
    CompletableFuture<AppendEntriesResponse> reply
) implements RaftEvent {

    @Override
    public void fail(Throwable error) {
        reply.completeExceptionally(error);
    }
}
