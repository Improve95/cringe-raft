package ru.nsu.raft.node.event;

import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;

public record AppendEntriesResponseReceived(
    String from,
    long requestId,
    AppendEntriesRequest request,
    AppendEntriesResponse response
) implements RaftEvent {
}
