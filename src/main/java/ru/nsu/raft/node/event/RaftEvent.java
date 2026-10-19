package ru.nsu.raft.node.event;

import java.util.concurrent.CompletableFuture;
import ru.nsu.raft.rpc.*;
import ru.nsu.raft.api.AddEntryResponse;

public sealed interface RaftEvent {
    record NodeStarted() implements RaftEvent {}
    record ElectionTimeout(long epoch) implements RaftEvent {}
    record HeartbeatTick() implements RaftEvent {}
    record RequestVoteReceived(RequestVoteRequest req,
            CompletableFuture<RequestVoteResponse> reply) implements RaftEvent {}
    record AppendEntriesReceived(AppendEntriesRequest req,
            CompletableFuture<AppendEntriesResponse> reply) implements RaftEvent {}
    record VoteResponseReceived(String from, RequestVoteResponse resp) implements RaftEvent {}
    // Контекст запроса позволяет отличать задержанные ответы репликации (§5.3).
    record AppendEntriesResponseReceived(String from, long sequence, AppendEntriesRequest request,
            AppendEntriesResponse resp) implements RaftEvent {}
    record AddEntryReceived(String command,
            CompletableFuture<AddEntryResponse> reply) implements RaftEvent {}
}
