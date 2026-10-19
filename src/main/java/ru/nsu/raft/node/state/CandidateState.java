package ru.nsu.raft.node.state;

import java.util.HashSet;
import java.util.Set;
import ru.nsu.raft.node.*;
import ru.nsu.raft.rpc.*;

public class CandidateState extends AbstractRaftState {
    private final Set<String> votes = new HashSet<>();
    public CandidateState(RaftNode node) { super(node); }
    @Override public NodeRole role() { return NodeRole.CANDIDATE; }
    @Override
    public void onEnter() {
        node.incrementTerm();
        node.recordVote(node.selfId());
        node.setLeader(null);
        votes.add(node.selfId());
        node.resetElectionTimer();
        log.info("[{}] term={} election started", node.selfId(), node.term());
        node.broadcast(new RequestVoteRequest(node.term(), node.selfId(),
                node.log().lastIndex(), node.log().lastTerm()));
        checkQuorum();
    }
    @Override public void onElectionTimeout() { node.becomeCandidate(); }
    @Override
    public void onVoteResponse(String from, RequestVoteResponse response) {
        if (observeTerm(response.term()) || response.term() != node.term()) return;
        if (response.voteGranted() && node.isPeer(from) && votes.add(from)) {
            log.info("[{}] term={} vote received from {}", node.selfId(), node.term(), from);
            checkQuorum();
        }
    }
    private void checkQuorum() {
        if (votes.size() >= node.quorum()) {
            log.info("[{}] term={} election won", node.selfId(), node.term());
            node.becomeLeader();
        }
    }
}
