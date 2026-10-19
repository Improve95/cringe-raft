package ru.nsu.raft.rpc;

import java.util.List;
import ru.nsu.raft.node.LogEntry;

public record AppendEntriesRequest(long term, String leaderId, long prevLogIndex,
        long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
    public AppendEntriesRequest {
        if (term < 0 || leaderId == null || leaderId.isBlank() || prevLogIndex < 0
                || prevLogTerm < 0 || prevLogTerm > term || leaderCommit < 0 || entries == null
                || (prevLogIndex == 0 && prevLogTerm != 0)) {
            throw new IllegalArgumentException("Invalid AppendEntries request");
        }
        entries = List.copyOf(entries);
    }
}
