package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.RequestVoteReceived;

public final class RequestVoteReceivedHandler implements RaftEventHandler<RequestVoteReceived> {
    @Override
    public void handle(RaftNode node, RequestVoteReceived event) {
        event.reply().complete(node.state().onRequestVote(event.request()));
    }
}
