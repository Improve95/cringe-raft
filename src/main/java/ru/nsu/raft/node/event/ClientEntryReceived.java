package ru.nsu.raft.node.event;

import ru.nsu.raft.api.EntryRequest;
import ru.nsu.raft.api.EntryResponse;

import java.util.concurrent.CompletableFuture;

public record ClientEntryReceived(EntryRequest request,
                                  CompletableFuture<EntryResponse> reply) implements RaftEvent {
    @Override
    public void fail(Throwable error) {
        reply.completeExceptionally(error);
    }
}
