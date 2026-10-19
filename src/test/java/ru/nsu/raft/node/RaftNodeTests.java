package ru.nsu.raft.node;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import ru.nsu.raft.api.*;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.config.ClusterProperties.Peer;
import ru.nsu.raft.node.event.RaftEvent.*;
import ru.nsu.raft.rpc.*;
import ru.nsu.raft.runtime.*;
import ru.nsu.raft.transport.PeerTransport;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

class RaftNodeTests {
    private static final class Cluster implements PeerTransport {
        final Map<String, RaftNode> nodes = new LinkedHashMap<>();
        final Map<String, ElectionTimer> timers = new HashMap<>();
        final Set<String> isolated = new HashSet<>();
        final Deque<Runnable> network = new ArrayDeque<>();
        Cluster(int count) {
            for (int i = 1; i <= count; i++) {
                String id = "n" + i;
                List<Peer> peers = new ArrayList<>();
                for (int j = 1; j <= count; j++) {
                    if (j != i) peers.add(new Peer("n" + j, URI.create("http://localhost:" + (8000 + j))));
                }
                var properties = new ClusterProperties(id, peers, Duration.ofMillis(150),
                        Duration.ofMillis(300), Duration.ofMillis(50), Duration.ofSeconds(1));
                var timer = mock(ElectionTimer.class);
                when(timer.isCurrent(anyLong())).thenReturn(true);
                timers.put(id, timer);
                nodes.put(id, new RaftNode(properties, timer, mock(HeartbeatScheduler.class), this));
            }
            nodes.values().forEach(node -> node.handle(new NodeStarted()));
        }
        @Override public void requestVote(Peer peer, RequestVoteRequest request) {
            network.add(() -> {
                if (disconnected(request.candidateId(), peer.id())) return;
                var reply = new CompletableFuture<RequestVoteResponse>();
                nodes.get(peer.id()).handle(new RequestVoteReceived(request, reply));
                network.add(() -> {
                    if (!disconnected(peer.id(), request.candidateId())) {
                        nodes.get(request.candidateId()).handle(new VoteResponseReceived(peer.id(), reply.join()));
                    }
                });
            });
        }
        @Override public void appendEntries(Peer peer, long sequence, AppendEntriesRequest request) {
            network.add(() -> {
                if (disconnected(request.leaderId(), peer.id())) return;
                var reply = new CompletableFuture<AppendEntriesResponse>();
                nodes.get(peer.id()).handle(new AppendEntriesReceived(request, reply));
                network.add(() -> {
                    if (!disconnected(peer.id(), request.leaderId())) {
                        nodes.get(request.leaderId()).handle(new AppendEntriesResponseReceived(
                                peer.id(), sequence, request, reply.join()));
                    }
                });
            });
        }
        boolean disconnected(String from, String to) { return isolated.contains(from) || isolated.contains(to); }
        void drain() {
            int remaining = 1000;
            while (!network.isEmpty()) {
                assertTrue(remaining-- > 0, "Replication must converge");
                network.removeFirst().run();
            }
        }
        RaftNode elect(String id) {
            var node = nodes.get(id);
            node.handle(new ElectionTimeout(1));
            drain();
            assertEquals(NodeRole.LEADER, node.snapshot().status().role());
            return node;
        }
        CompletableFuture<AddEntryResponse> add(RaftNode node, String command) {
            var reply = new CompletableFuture<AddEntryResponse>();
            node.handle(new AddEntryReceived(command, reply));
            return reply;
        }
    }
    @Test void startsAsFollowerAtTermZero() {
        var node = new Cluster(3).nodes.get("n1");
        assertEquals(new NodeStatus("n1", NodeRole.FOLLOWER, 0, null), node.snapshot().status());
    }
    @Test void singleNodeElectsAndCommitsImmediately() {
        var cluster = new Cluster(1);
        var leader = cluster.elect("n1");
        var reply = cluster.add(leader, "hello");
        assertEquals(2, reply.join().index());
        assertEquals(2, leader.snapshot().log().commitIndex());
    }
    @Test void votesOnlyOncePerTermAndRejectsOlderTerm() {
        var node = new Cluster(3).nodes.get("n1");
        assertTrue(vote(node, 2, "n2", 0, 0).voteGranted());
        assertFalse(vote(node, 2, "n3", 0, 0).voteGranted());
        assertTrue(vote(node, 2, "n2", 0, 0).voteGranted());
        assertFalse(vote(node, 1, "n2", 0, 0).voteGranted());
        assertTrue(vote(node, 3, "n3", 0, 0).voteGranted());
    }
    @Test void rejectsCandidateWithOutdatedLog() {
        var node = new Cluster(3).nodes.get("n1");
        node.log().append(2, "committed elsewhere");
        node.log().append(2, "second entry");
        assertFalse(vote(node, 3, "n2", 100, 1).voteGranted());
        assertFalse(vote(node, 3, "n2", 1, 2).voteGranted());
        assertTrue(vote(node, 3, "n3", 2, 2).voteGranted());
    }
    @Test void splitVoteRestartsAndOldOrDuplicateVotesDoNotCount() {
        var cluster = new Cluster(5);
        var node = cluster.nodes.get("n1");
        node.handle(new ElectionTimeout(1));
        node.handle(new ElectionTimeout(2));
        assertEquals(2, node.term());
        node.handle(new VoteResponseReceived("n2", new RequestVoteResponse(1, true)));
        node.handle(new VoteResponseReceived("n2", new RequestVoteResponse(2, true)));
        node.handle(new VoteResponseReceived("n2", new RequestVoteResponse(2, true)));
        node.handle(new VoteResponseReceived("unknown", new RequestVoteResponse(2, true)));
        assertEquals(NodeRole.CANDIDATE, node.snapshot().status().role());
        node.handle(new VoteResponseReceived("n3", new RequestVoteResponse(2, true)));
        assertEquals(NodeRole.LEADER, node.snapshot().status().role());
    }
    @Test void staleTimerIsIgnoredAndLeaderIgnoresElectionTimeout() {
        var cluster = new Cluster(1);
        var node = cluster.nodes.get("n1");
        when(cluster.timers.get("n1").isCurrent(99)).thenReturn(false);
        node.handle(new ElectionTimeout(99));
        assertEquals(0, node.term());
        cluster.elect("n1");
        node.handle(new ElectionTimeout(1));
        assertEquals(1, node.term());
        verify(cluster.timers.get("n1")).cancel();
    }
    @Test void candidateAcceptsLeaderInSameTermAndPreservesVote() {
        var node = new Cluster(3).nodes.get("n1");
        node.handle(new ElectionTimeout(1));
        var reply = new CompletableFuture<AppendEntriesResponse>();
        node.handle(new AppendEntriesReceived(new AppendEntriesRequest(1, "n2", 0, 0, List.of(), 0), reply));
        assertTrue(reply.join().success());
        assertEquals(NodeRole.FOLLOWER, node.snapshot().status().role());
        assertEquals("n2", node.leaderId());
        assertFalse(vote(node, 1, "n3", 0, 0).voteGranted());
    }
    @Test void leaderStepsDownOnHigherTermInEveryRpcKind() {
        for (int kind = 0; kind < 4; kind++) {
            var node = new Cluster(1).elect("n1");
            switch (kind) {
                case 0 -> vote(node, 5, "n2", 1, 1);
                case 1 -> node.handle(new VoteResponseReceived("n2", new RequestVoteResponse(5, false)));
                case 2 -> node.handle(new AppendEntriesReceived(
                        new AppendEntriesRequest(5, "n2", 0, 0, List.of(), 0), new CompletableFuture<>()));
                case 3 -> node.handle(new AppendEntriesResponseReceived("n2", 1,
                        new AppendEntriesRequest(1, "n1", 0, 0, List.of(), 0), new AppendEntriesResponse(5, false, 0)));
            }
            assertEquals(5, node.term());
            assertEquals(NodeRole.FOLLOWER, node.snapshot().status().role());
        }
    }
    @Test void commitsOnlyAfterMajorityAndReplicatesCommitIndex() {
        var cluster = new Cluster(3);
        var leader = cluster.elect("n1");
        var reply = cluster.add(leader, "hello");
        assertFalse(reply.isDone());
        cluster.drain();
        assertEquals(2, reply.join().index());
        for (var node : cluster.nodes.values()) {
            assertEquals(leader.snapshot().log(), node.snapshot().log());
        }
    }
    @Test void isolatedLeaderCannotCommitAndItsSuffixIsReplacedAfterRejoin() {
        var cluster = new Cluster(3);
        var oldLeader = cluster.elect("n1");
        cluster.isolated.add("n1");
        var abandoned = cluster.add(oldLeader, "orphan");
        cluster.drain();
        assertFalse(abandoned.isDone());
        assertEquals(1, oldLeader.log().commitIndex());
        var newLeader = cluster.elect("n2");
        var confirmed = cluster.add(newLeader, "survives");
        cluster.drain();
        assertTrue(confirmed.isDone());
        cluster.isolated.clear();
        newLeader.handle(new HeartbeatTick());
        cluster.drain();
        assertEquals(NodeRole.FOLLOWER, oldLeader.snapshot().status().role());
        assertTrue(abandoned.isCompletedExceptionally());
        assertEquals(newLeader.snapshot().log(), oldLeader.snapshot().log());
        assertFalse(oldLeader.log().snapshot().entries().stream().anyMatch(entry -> "orphan".equals(entry.command())));
    }
    @Test void followerRejectsClientWriteWithKnownLeader() {
        var cluster = new Cluster(3);
        cluster.elect("n1");
        var reply = cluster.add(cluster.nodes.get("n2"), "hello");
        var exception = assertThrows(java.util.concurrent.CompletionException.class, reply::join);
        assertEquals("n1", ((NotLeaderException) exception.getCause()).leaderId());
    }
    private static RequestVoteResponse vote(RaftNode node, long term, String id, long index, long lastTerm) {
        var reply = new CompletableFuture<RequestVoteResponse>();
        node.handle(new RequestVoteReceived(new RequestVoteRequest(term, id, index, lastTerm), reply));
        return reply.join();
    }
}
