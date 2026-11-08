package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.HeartbeatTick;

public final class HeartbeatTickHandler implements RaftEventHandler<HeartbeatTick> {
    @Override
    public void handle(RaftNode node, HeartbeatTick event) {
        node.state().onHeartbeatTick();
    }
}
