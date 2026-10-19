package ru.nsu.raft.node;

public record NodeSnapshot(NodeStatus status, LogSnapshot log) {}
