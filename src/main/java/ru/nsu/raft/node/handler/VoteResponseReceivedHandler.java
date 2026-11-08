package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.VoteResponseReceived;

public final class VoteResponseReceivedHandler implements RaftEventHandler<VoteResponseReceived> {
    @Override
    public void handle(RaftNode node, VoteResponseReceived event) {
        node.state().onVoteResponse(event.from(), event.response());
    }
}
