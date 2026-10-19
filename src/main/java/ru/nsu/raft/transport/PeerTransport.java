package ru.nsu.raft.transport;

import ru.nsu.raft.config.ClusterProperties.Peer;
import ru.nsu.raft.rpc.*;

// Подмена транспорта позволяет проверять протокол без сети и случайных задержек.
public interface PeerTransport {
    void requestVote(Peer peer, RequestVoteRequest request);
    void appendEntries(Peer peer, long sequence, AppendEntriesRequest request);
}
