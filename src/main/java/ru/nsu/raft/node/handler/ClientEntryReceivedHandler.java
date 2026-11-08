package ru.nsu.raft.node.handler;

import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.ClientEntryReceived;

public final class ClientEntryReceivedHandler implements RaftEventHandler<ClientEntryReceived> {
    @Override
    public void handle(RaftNode node, ClientEntryReceived event) {
        node.state().onClientEntry(event);
    }
}
