package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.ElectionTimeout;

public final class ElectionTimeoutHandler implements RaftEventHandler<ElectionTimeout> {
    @Override
    public void handle(RaftNode node, ElectionTimeout event) {
        if (node.electionTimer().isCurrent(event.epoch())) {
            node.state().onElectionTimeout();
        }
    }
}
