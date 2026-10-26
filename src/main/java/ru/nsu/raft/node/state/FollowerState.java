package ru.nsu.raft.node.state;

import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;

public final class FollowerState extends AbstractRaftState {
    public FollowerState(RaftNode node) {
        super(node);
    }

    @Override
    public NodeRole role() {
        return NodeRole.FOLLOWER;
    }

    @Override
    public void onEnter() {
        node.electionTimer().reset();
    }

    @Override
    public void onElectionTimeout() {
        node.becomeCandidate();
    }

    @Override
    protected AppendEntriesResponse acceptAppendEntries(AppendEntriesRequest request) {
        node.setLeader(request.leaderId());
        node.electionTimer().reset();
        boolean success = node.log().appendFrom(request.prevLogIndex(), request.prevLogTerm(), request.entries());
        if (success) {
            // Figure 2: нельзя фиксировать непроверенный хвост за последней записью этого RPC.
            node.log().commitThrough(Math.min(request.leaderCommit(), request.lastIndex()));
        }
        log.trace(
            "[{}] term={} AppendEntries from {} success={}",
            node.properties().nodeId(), node.term(), request.leaderId(), success);
        return new AppendEntriesResponse(node.term(), success);
    }
}
