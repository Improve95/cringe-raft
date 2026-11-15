package ru.nsu.raft.node;

import ru.nsu.raft.node.logger.LogSnapshot;

public record NodeSnapshot(NodeStatus status, LogSnapshot log) {
}
