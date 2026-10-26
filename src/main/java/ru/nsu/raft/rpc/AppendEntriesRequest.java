package ru.nsu.raft.rpc;

import ru.nsu.raft.node.logger.LogEntry;

import java.util.List;

public record AppendEntriesRequest(long term, String leaderId, long prevLogIndex,
                                   long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
    public AppendEntriesRequest {
        if (term < 0 || prevLogIndex < 0 || prevLogTerm < 0 || prevLogTerm > term
            || (prevLogIndex == 0 && prevLogTerm != 0) || leaderCommit < 0
            || leaderId == null || leaderId.isBlank() || entries == null) {
            throw new IllegalArgumentException("Invalid AppendEntries request");
        }
        entries = List.copyOf(entries);
        validateEntries(prevLogIndex, prevLogTerm, term, entries);
    }

    private static void validateEntries(
        long previousIndex, long previousTerm,
        long term, List<LogEntry> entries
    ) {
        for (LogEntry entry : entries) {
            if (entry.index() != ++previousIndex || entry.term() < previousTerm || entry.term() > term) {
                throw new IllegalArgumentException("Entries must have consecutive indexes and ordered terms");
            }
            previousTerm = entry.term();
        }
    }

    public long lastIndex() {
        return prevLogIndex + entries.size();
    }
}
