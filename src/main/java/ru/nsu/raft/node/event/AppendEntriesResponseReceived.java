package ru.nsu.raft.node.event;

import ru.nsu.raft.rpc.AppendEntriesResponse;

public record AppendEntriesResponseReceived(String from, AppendEntriesResponse response) implements RaftEvent {
}
