package ru.nsu.raft.node;

import java.util.List;

public record LogSnapshot(List<LogEntry> entries, long commitIndex) {
    public LogSnapshot { entries = List.copyOf(entries); }
}
