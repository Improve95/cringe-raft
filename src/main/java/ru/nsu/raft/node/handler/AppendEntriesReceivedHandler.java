package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.AppendEntriesReceived;

public final class AppendEntriesReceivedHandler implements RaftEventHandler<AppendEntriesReceived> {
    @Override
    public void handle(RaftNode node, AppendEntriesReceived event) {
        event.reply().complete(node.state().onAppendEntries(event.request()));
    }
}
