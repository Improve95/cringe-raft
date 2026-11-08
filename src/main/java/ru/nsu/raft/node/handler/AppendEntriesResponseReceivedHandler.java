package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;

public final class AppendEntriesResponseReceivedHandler implements RaftEventHandler<AppendEntriesResponseReceived> {
    @Override
    public void handle(RaftNode node, AppendEntriesResponseReceived event) {
        node.state().onAppendEntriesResponse(event);
    }
}
