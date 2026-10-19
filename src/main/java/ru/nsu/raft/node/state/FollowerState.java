package ru.nsu.raft.node.state;

import ru.nsu.raft.node.*;
import ru.nsu.raft.rpc.*;

public class FollowerState extends AbstractRaftState {
    public FollowerState(RaftNode node) { super(node); }
    @Override public NodeRole role() { return NodeRole.FOLLOWER; }
    @Override public void onEnter() { node.resetElectionTimer(); }
    @Override public void onElectionTimeout() { node.becomeCandidate(); }

    @Override
    public AppendEntriesResponse onAppendEntries(AppendEntriesRequest request) {
        observeTerm(request.term());
        if (request.term() < node.term()) return new AppendEntriesResponse(node.term(), false, 0);
        node.setLeader(request.leaderId());
        node.resetElectionTimer();
        boolean accepted = node.log().accept(request);
        long matched = accepted ? request.prevLogIndex() + request.entries().size() : 0;
        log.debug("[{}] term={} append from {} accepted={}",
                node.selfId(), node.term(), request.leaderId(), accepted);
        return new AppendEntriesResponse(node.term(), accepted, matched);
    }
}
