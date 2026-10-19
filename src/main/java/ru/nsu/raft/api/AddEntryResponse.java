package ru.nsu.raft.api;

public record AddEntryResponse(long index, long term) {}
