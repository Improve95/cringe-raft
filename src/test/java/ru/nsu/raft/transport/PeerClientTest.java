package ru.nsu.raft.transport;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import ru.nsu.raft.config.ClusterProperties.Peer;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.node.event.RaftEvent;
import ru.nsu.raft.node.event.VoteResponseReceived;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.runtime.EventQueue;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class PeerClientTest {
    private final Peer peer = new Peer("b", URI.create("http://peer:8082"));

    @Test
    void performsRpcsOnVirtualThreadsAndPostsCorrelatedResponses() throws Exception {
        var builder = RestClient.builder();
        var server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://peer:8082/raft/request-vote")).andRespond(request -> {
            assertThat(Thread.currentThread().isVirtual()).isTrue();
            return withSuccess("{\"term\":2,\"voteGranted\":true}", MediaType.APPLICATION_JSON).createResponse(request);
        });
        server.expect(requestTo("http://peer:8082/raft/append-entries")).andRespond(request -> {
            assertThat(Thread.currentThread().isVirtual()).isTrue();
            return withSuccess("{\"term\":2,\"success\":true}", MediaType.APPLICATION_JSON).createResponse(request);
        });
        var queue = new EventQueue();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var client = new PeerClient(builder.build(), executor, queue);
            client.requestVote(peer, new RequestVoteRequest(2, "a", 0, 0));
            var vote = (VoteResponseReceived) nextEvent(queue);
            assertThat(vote.from()).isEqualTo("b");
            assertThat(vote.response().voteGranted()).isTrue();
            var request = new AppendEntriesRequest(2, "a", 0, 0, List.of(), 0);
            client.appendEntries(peer, 7, request);
            var append = (AppendEntriesResponseReceived) nextEvent(queue);
            assertThat(append.request()).isSameAs(request);
            assertThat(append.requestId()).isEqualTo(7);
            assertThat(append.from()).isEqualTo("b");
            assertThat(append.response().success()).isTrue();
        }
        server.verify();
    }

    @Test
    void networkFailureDoesNotCreateAResponseEvent() {
        var builder = RestClient.builder();
        var server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://peer:8082/raft/request-vote")).andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));
        var queue = mock(EventQueue.class);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var client = new PeerClient(builder.build(), executor, queue);
            client.requestVote(peer, new RequestVoteRequest(1, "a", 0, 0));
        }
        verifyNoInteractions(queue);
        server.verify();
    }

    private RaftEvent nextEvent(EventQueue queue) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return queue.take();
            } catch (InterruptedException error) {
                throw new IllegalStateException(error);
            }
        }).get(2, TimeUnit.SECONDS);
    }
}
