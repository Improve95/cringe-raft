package ru.nsu.raft.node.state;

import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.rpc.AppendEntriesRequest;

public final class LeaderState extends AbstractRaftState {
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
        node.heartbeatScheduler().start();
        broadcastHeartbeat();
    }

    @Override
    public void onExit() {
        node.heartbeatScheduler().stop();
    }

    @Override
    public void onHeartbeatTick() {
        broadcastHeartbeat();
    }

    private void broadcastHeartbeat() {
        var request = new AppendEntriesRequest(node.term(), node.properties().nodeId());
        node.properties().peers().forEach(peer -> node.peerClient().appendEntries(peer, request));
    }
}
