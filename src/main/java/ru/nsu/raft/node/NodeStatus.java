package ru.nsu.raft.node;

public record NodeStatus(String nodeId, NodeRole role, long term, String leaderId) {}
