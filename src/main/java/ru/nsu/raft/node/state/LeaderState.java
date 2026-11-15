package ru.nsu.raft.node.state;

import ru.nsu.raft.config.ClusterProperties.Peer;
import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.node.event.ClientEntryReceived;
import ru.nsu.raft.rpc.AppendEntriesRequest;

import java.util.HashMap;
import java.util.Map;

public final class LeaderState extends AbstractRaftState {
    private final Map<String, PeerReplication> replication = new HashMap<>();
    private final PendingCommands pending = new PendingCommands();

    public LeaderState(RaftNode node) {
        super(node);
    }

    @Override
    public NodeRole role() {
        return NodeRole.LEADER;
    }

    @Override
    public void onEnter() {
        node.setLeader(node.properties().nodeId());
        node.electionTimer().cancel();
        node.properties().peers().forEach(peer ->
            replication.put(peer.id(), new PeerReplication(node.log().lastIndex() + 1)));
        // Raft §5.4.2: запись текущего терма позволяет зафиксировать унаследованный префикс.
        node.log().append(node.term(), null);
        advanceCommitIndex();
        node.heartbeatScheduler().start();
        broadcast();
    }

    @Override
    public void onExit() {
        node.heartbeatScheduler().stop();
        pending.failAll();
    }

    @Override
    public void onHeartbeatTick() {
        pending.removeExpired();
        broadcast();
    }

    @Override
    public void onClientEntry(ClientEntryReceived event) {
        if (event.reply().isDone()) {
            return;
        }
        var entry = node.log().append(node.term(), event.request().command());
        pending.add(entry, event.reply());
        pending.removeExpired();
        advanceCommitIndex();
        broadcast();
    }

    @Override
    protected void handleAppendEntriesResponse(AppendEntriesResponseReceived event) {
        PeerReplication progress = replication.get(event.from());
        if (progress == null || event.request().term() != node.term() || !progress.acknowledge(event)) {
            return;
        }
        boolean committed = advanceCommitIndex();
        if (committed) {
            broadcast();
        } else if (!event.response().success() || progress.nextIndex() <= node.log().lastIndex()) {
            node.properties().peers().stream().filter(peer -> peer.id().equals(event.from()))
                .findFirst().ifPresent(this::sendAppendEntries);
        }
    }

    private void broadcast() {
        node.properties().peers().forEach(this::sendAppendEntries);
    }

    private void sendAppendEntries(Peer peer) {
        PeerReplication progress = replication.get(peer.id());
        long previousIndex = progress.nextIndex() - 1;
        var request = new AppendEntriesRequest(
            node.term(), node.properties().nodeId(),
            previousIndex, node.log().termAt(previousIndex), node.log().entriesFrom(progress.nextIndex()),
            node.log().commitIndex());
        node.peerClient().appendEntries(peer, progress.nextRequestId(), request);
    }

    private boolean advanceCommitIndex() {
        for (long index = node.log().lastIndex(); index > node.log().commitIndex(); index--) {
            if (node.log().termAt(index) == node.term() && replicatedOnMajority(index)) {
                node.log().commitThrough(index);
                pending.completeCommitted(node.log());
                return true;
            }
        }
        return false;
    }

    private boolean replicatedOnMajority(long index) {
        long replicas = 1 + replication.values().stream().filter(peer -> peer.matchIndex() >= index).count();
        return replicas >= node.properties().quorum();
    }
}
