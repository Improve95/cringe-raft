package ru.nsu.raft.node.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.nsu.raft.api.NotLeaderException;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.node.event.ClientEntryReceived;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;

public abstract class AbstractRaftState implements RaftState {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final RaftNode node;

    protected AbstractRaftState(RaftNode node) {
        this.node = node;
    }

    @Override
    public final RequestVoteResponse onRequestVote(RequestVoteRequest request) {
        observeTerm(request.term());
        boolean grant = request.term() == node.term() && node.canVoteFor(request.candidateId())
                        && node.log().isCandidateUpToDate(request.lastLogIndex(), request.lastLogTerm());
        if (grant) {
            node.recordVote(request.candidateId());
            node.electionTimer().reset();
        }
        return new RequestVoteResponse(node.term(), grant);
    }

    @Override
    public final AppendEntriesResponse onAppendEntries(AppendEntriesRequest request) {
        if (observeTerm(request.term())) {
            return node.state().onAppendEntries(request);
        }
        if (request.term() < node.term()) {
            return new AppendEntriesResponse(node.term(), false);
        }
        return acceptAppendEntries(request);
    }

    @Override
    public final void onVoteResponse(String from, RequestVoteResponse response) {
        if (!observeTerm(response.term()) && response.term() == node.term()) {
            handleVoteResponse(from, response);
        }
    }

    @Override
    public final void onAppendEntriesResponse(AppendEntriesResponseReceived event) {
        if (!observeTerm(event.response().term()) && event.response().term() == node.term()) {
            handleAppendEntriesResponse(event);
        }
    }

    @Override
    public void onClientEntry(ClientEntryReceived event) {
        event.fail(new NotLeaderException(node.leaderId()));
    }

    protected AppendEntriesResponse acceptAppendEntries(AppendEntriesRequest request) {
        node.becomeFollower(node.term());
        return node.state().onAppendEntries(request);
    }

    protected void handleVoteResponse(String from, RequestVoteResponse response) {
    }

    protected void handleAppendEntriesResponse(AppendEntriesResponseReceived event) {
    }

    @Override
    public void onEnter() {
    }

    @Override
    public void onExit() {
    }

    @Override
    public void onElectionTimeout() {
    }

    @Override
    public void onHeartbeatTick() {
    }

    private boolean observeTerm(long term) {
        // Raft Figure 2: больший терм в любом RPC или ответе всегда делает узел фолловером.
        if (term <= node.term()) {
            return false;
        }
        log.info("[{}] term={} observed higher term={}", node.properties().nodeId(), node.term(), term);
        node.becomeFollower(term);
        return true;
    }
}
