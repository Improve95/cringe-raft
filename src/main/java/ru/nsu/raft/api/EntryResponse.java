package ru.nsu.raft.api;

public record EntryResponse(long index, long term) {
}
