package ru.nsu.raft.rpc;

public record AppendEntriesResponse(long term, boolean success) {}
