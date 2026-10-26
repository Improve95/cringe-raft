package ru.nsu.raft.support;

import ru.nsu.raft.api.EntryRequest;
import ru.nsu.raft.api.EntryResponse;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.config.ClusterProperties.Peer;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.AppendEntriesReceived;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.node.event.ClientEntryReceived;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.node.event.NodeStarted;
import ru.nsu.raft.node.event.RequestVoteReceived;
import ru.nsu.raft.node.event.VoteResponseReceived;
import ru.nsu.raft.node.logger.RaftLog;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.EventQueue;
import ru.nsu.raft.runtime.HeartbeatScheduler;
import ru.nsu.raft.transport.PeerClient;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public final class NodeFixture {
    public final RaftLog log = new RaftLog();
    public final PeerClient client = mock(PeerClient.class);
    public final HeartbeatScheduler heartbeat = mock(HeartbeatScheduler.class);
    public final ElectionTimer timer;
    public final RaftNode node;
    public final List<SentAppend> appends = new ArrayList<>();

    public NodeFixture(String... peerIds) {
        List<Peer> peers = Arrays.stream(peerIds)
            .map(id -> new Peer(id, URI.create("http://localhost:9999"))).toList();
        var properties = new ClusterProperties(
            "a", peers, Duration.ofMillis(300),
            Duration.ofMillis(600), Duration.ofMillis(50), Duration.ofSeconds(1));
        timer = new ElectionTimer(new EventQueue(), mock(ScheduledExecutorService.class), properties);
        doAnswer(call -> {
            appends.add(new SentAppend(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
            return null;
        }).when(client).appendEntries(any(), anyLong(), any());
        node = new RaftNode(properties, log, timer, heartbeat, client);
        node.handle(new NodeStarted());
    }

    public void timeout() {
        node.handle(new ElectionTimeout(timer.epoch()));
    }

    public void vote(String from, long term, boolean granted) {
        node.handle(new VoteResponseReceived(from, new RequestVoteResponse(term, granted)));
    }

    public RequestVoteResponse requestVote(long term, String candidate, long lastIndex, long lastTerm) {
        var reply = new CompletableFuture<RequestVoteResponse>();
        node.handle(new RequestVoteReceived(new RequestVoteRequest(term, candidate, lastIndex, lastTerm), reply));
        return reply.join();
    }

    public AppendEntriesResponse append(AppendEntriesRequest request) {
        var reply = new CompletableFuture<AppendEntriesResponse>();
        node.handle(new AppendEntriesReceived(request, reply));
        return reply.join();
    }

    public CompletableFuture<EntryResponse> submit(String command) {
        var reply = new CompletableFuture<EntryResponse>();
        node.handle(new ClientEntryReceived(new EntryRequest(command), reply));
        return reply;
    }

    public SentAppend lastAppend(String peerId) {
        return appends.stream().filter(sent -> sent.peer().id().equals(peerId)).toList().getLast();
    }

    public void acknowledge(SentAppend sent, long term, boolean success) {
        node.handle(new AppendEntriesResponseReceived(
            sent.peer().id(), sent.requestId(), sent.request(),
            new AppendEntriesResponse(term, success)));
    }

    public record SentAppend(Peer peer, long requestId, AppendEntriesRequest request) {
    }
}
