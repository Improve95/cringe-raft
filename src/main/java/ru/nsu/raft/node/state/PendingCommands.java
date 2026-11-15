package ru.nsu.raft.node.state;

import ru.nsu.raft.api.EntryResponse;
import ru.nsu.raft.api.NotLeaderException;
import ru.nsu.raft.node.logger.LogEntry;
import ru.nsu.raft.node.logger.RaftLog;

import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

final class PendingCommands {
    private final TreeMap<Long, CompletableFuture<EntryResponse>> replies = new TreeMap<>();

    void add(LogEntry entry, CompletableFuture<EntryResponse> reply) {
        replies.put(entry.index(), reply);
    }

    void completeCommitted(RaftLog log) {
        var committed = replies.headMap(log.commitIndex(), true);
        committed.forEach((index, reply) -> reply.complete(new EntryResponse(index, log.termAt(index))));
        committed.clear();
    }

    void removeExpired() {
        replies.values().removeIf(CompletableFuture::isDone);
    }

    void failAll() {
        replies.values().forEach(reply -> reply.completeExceptionally(new NotLeaderException(null)));
        replies.clear();
    }
}
