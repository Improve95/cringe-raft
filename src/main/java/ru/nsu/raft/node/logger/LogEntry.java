package ru.nsu.raft.node.logger;

public record LogEntry(long index, long term, String command) {
    public LogEntry {
        if (index < 1 || term < 0) {
            throw new IllegalArgumentException("Log entry index must be positive and term nonnegative");
        }
    }
}
