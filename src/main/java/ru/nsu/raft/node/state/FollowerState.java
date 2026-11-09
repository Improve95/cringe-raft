package ru.nsu.raft.node.state;

import lombok.extern.slf4j.Slf4j;
import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;

@Slf4j
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
        log.trace("[{}] term={} heartbeat from {}", node.properties().nodeId(), node.term(), request.leaderId());
        return new AppendEntriesResponse(node.term(), true);
    }
}
