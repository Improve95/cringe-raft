package ru.nsu.raft.node.state;

import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.nsu.raft.api.*;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.RaftEvent.AppendEntriesResponseReceived;
import ru.nsu.raft.rpc.*;

public abstract class AbstractRaftState implements RaftState {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final RaftNode node;
    protected AbstractRaftState(RaftNode node) { this.node = node; }

    // Единое правило для всех запросов и ответов: больший терм означает отход (§5.1).
    protected boolean observeTerm(long term) {
        if (term <= node.term()) return false;
        log.info("[{}] higher term {} -> {}", node.selfId(), node.term(), term);
        node.becomeFollower(term);
        return true;
    }
    @Override public void onEnter() {}
    @Override public void onExit() {}
    @Override public void onElectionTimeout() {}
    @Override public void onHeartbeatTick() {}
    @Override public void onVoteResponse(String from, RequestVoteResponse response) { observeTerm(response.term()); }
    @Override public void onAppendEntriesResponse(AppendEntriesResponseReceived event) { observeTerm(event.resp().term()); }

    @Override
    public RequestVoteResponse onRequestVote(RequestVoteRequest request) {
        observeTerm(request.term());
        boolean granted = request.term() == node.term() && node.canVoteFor(request.candidateId())
                && node.log().isUpToDate(request.lastLogIndex(), request.lastLogTerm());
        if (granted) {
            node.recordVote(request.candidateId());
            node.resetElectionTimer();
            log.info("[{}] term={} vote granted to {}", node.selfId(), node.term(), request.candidateId());
        }
        return new RequestVoteResponse(node.term(), granted);
    }
    @Override
    public AppendEntriesResponse onAppendEntries(AppendEntriesRequest request) {
        boolean steppedDown = observeTerm(request.term());
        if (request.term() < node.term()) return new AppendEntriesResponse(node.term(), false, 0);
        if (!steppedDown) node.becomeFollower(request.term());
        return node.state().onAppendEntries(request);
    }
    @Override
    public void onAddEntry(String command, CompletableFuture<AddEntryResponse> reply) {
        reply.completeExceptionally(new NotLeaderException(node.leaderId()));
    }
}
