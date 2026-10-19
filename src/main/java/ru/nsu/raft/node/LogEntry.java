package ru.nsu.raft.node;

// null-команда — служебная запись лидера для фиксации записей предыдущих термов (§5.4.2).
public record LogEntry(long index, long term, String command) {}
