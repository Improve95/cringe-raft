package ru.nsu.raft.node.state;

import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;

import java.util.HashSet;
import java.util.Set;

public final class CandidateState extends AbstractRaftState {
    private final Set<String> votes = new HashSet<>();

    public CandidateState(RaftNode node) {
        super(node);
    }

    @Override
    public NodeRole role() {
        return NodeRole.CANDIDATE;
    }

    @Override
    public void onEnter() {
        node.incrementTerm();
        node.recordVote(node.properties().nodeId());
        votes.add(node.properties().nodeId());
        node.electionTimer().reset();
        log.info("[{}] term={} election started", node.properties().nodeId(), node.term());
        var request = new RequestVoteRequest(
            node.term(), node.properties().nodeId(),
            node.log().lastIndex(), node.log().lastTerm());
        node.properties().peers().forEach(peer -> node.peerClient().requestVote(peer, request));
        checkQuorum();
    }

    @Override
    public void onElectionTimeout() {
        node.becomeCandidate();
    }

    @Override
    protected void handleVoteResponse(String from, RequestVoteResponse response) {
        if (response.voteGranted() && node.properties().hasPeer(from) && votes.add(from)) {
            log.info("[{}] term={} received vote from {}", node.properties().nodeId(), node.term(), from);
            checkQuorum();
        }
    }

    private void checkQuorum() {
        if (votes.size() >= node.properties().quorum()) {
            log.info("[{}] term={} election won", node.properties().nodeId(), node.term());
            node.becomeLeader();
        }
    }
}
