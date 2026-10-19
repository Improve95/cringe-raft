package ru.nsu.raft.node.state;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import ru.nsu.raft.api.*;
import ru.nsu.raft.node.*;
import ru.nsu.raft.node.event.RaftEvent.AppendEntriesResponseReceived;
import ru.nsu.raft.rpc.AppendEntriesRequest;

public class LeaderState extends AbstractRaftState {
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<String, Long> lastResponse = new HashMap<>();
    private final Map<Long, CompletableFuture<AddEntryResponse>> pending = new HashMap<>();
    private long sequence;

    public LeaderState(RaftNode node) { super(node); }
    @Override public NodeRole role() { return NodeRole.LEADER; }
    @Override
    public void onEnter() {
        node.setLeader(node.selfId());
        node.cancelElectionTimer();
        for (var peer : node.peers()) {
            nextIndex.put(peer.id(), node.log().lastIndex() + 1);
            matchIndex.put(peer.id(), 0L);
        }
        node.log().append(node.term(), null);
        node.startHeartbeat();
        advanceCommit();
        replicate();
    }
    @Override
    public void onExit() {
        node.stopHeartbeat();
        pending.values().forEach(reply -> reply.completeExceptionally(new NotLeaderException(null)));
        pending.clear();
    }
    @Override public void onHeartbeatTick() { replicate(); }
    @Override
    public void onAddEntry(String command, CompletableFuture<AddEntryResponse> reply) {
        var entry = node.log().append(node.term(), command);
        pending.put(entry.index(), reply);
        advanceCommit();
        replicate();
    }
    @Override
    public void onAppendEntriesResponse(AppendEntriesResponseReceived event) {
        if (observeTerm(event.resp().term())) return;
        String peer = event.from();
        if (!node.isPeer(peer) || event.resp().term() != node.term()
                || event.request().term() != node.term()
                || event.sequence() <= lastResponse.getOrDefault(peer, 0L)) return;
        lastResponse.put(peer, event.sequence());
        if (event.resp().success()) acceptResponse(peer, event);
        else backtrack(peer, event.request());
    }
    private void acceptResponse(String peer, AppendEntriesResponseReceived event) {
        long matched = event.request().prevLogIndex() + event.request().entries().size();
        if (event.resp().matchIndex() != matched) return;
        matchIndex.merge(peer, matched, Math::max);
        nextIndex.put(peer, matchIndex.get(peer) + 1);
        long previousCommit = node.log().commitIndex();
        advanceCommit();
        if (previousCommit != node.log().commitIndex()) replicate();
    }
    private void backtrack(String peer, AppendEntriesRequest request) {
        long next = nextIndex.get(peer);
        // Старый отказ не должен отменять уже подтверждённый прогресс пира.
        if (request.prevLogIndex() + 1 != next || next <= matchIndex.get(peer) + 1) return;
        nextIndex.put(peer, Math.max(matchIndex.get(peer) + 1, next - 1));
        replicateTo(peer);
    }
    private void advanceCommit() {
        for (long index = node.log().lastIndex(); index > node.log().commitIndex(); index--) {
            long candidate = index;
            long replicated = 1 + matchIndex.values().stream().filter(value -> value >= candidate).count();
            // Подсчётом большинства фиксируются только записи текущего терма (§5.4.2).
            if (node.log().termAt(index) == node.term() && replicated >= node.quorum()) {
                node.log().commit(index);
                break;
            }
        }
        var iterator = pending.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getValue().isDone() || entry.getKey() <= node.log().commitIndex()) {
                entry.getValue().complete(new AddEntryResponse(entry.getKey(), node.term()));
                iterator.remove();
            }
        }
    }
    private void replicate() { node.peers().forEach(peer -> replicateTo(peer.id())); }
    private void replicateTo(String peerId) {
        long previous = nextIndex.get(peerId) - 1;
        var request = new AppendEntriesRequest(node.term(), node.selfId(), previous,
                node.log().termAt(previous), node.log().after(previous), node.log().commitIndex());
        node.sendAppend(peerId, ++sequence, request);
    }
}
