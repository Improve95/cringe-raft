package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.RaftEvent;

@FunctionalInterface
public interface RaftEventHandler<E extends RaftEvent> {
    void handle(RaftNode node, E event);
}
