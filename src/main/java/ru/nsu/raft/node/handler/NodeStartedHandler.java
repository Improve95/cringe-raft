package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.NodeStarted;

public final class NodeStartedHandler implements RaftEventHandler<NodeStarted> {
    @Override
    public void handle(RaftNode node, NodeStarted event) {
        node.state().onEnter();
    }
}
