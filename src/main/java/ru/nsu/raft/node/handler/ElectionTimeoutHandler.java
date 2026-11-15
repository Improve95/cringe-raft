package ru.nsu.raft.node.handler;

import lombok.extern.slf4j.Slf4j;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.ElectionTimeout;

@Slf4j
public final class ElectionTimeoutHandler implements RaftEventHandler<ElectionTimeout> {
    @Override
    public void handle(RaftNode node, ElectionTimeout event) {
        if (node.electionTimer().isCurrent(event.epoch())) {
            node.state().onElectionTimeout();
        }
    }
}
