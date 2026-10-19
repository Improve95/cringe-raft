package ru.nsu.raft.transport;

import java.util.concurrent.*;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import ru.nsu.raft.config.ClusterProperties.Peer;
import ru.nsu.raft.node.event.RaftEvent.*;
import ru.nsu.raft.rpc.*;
import ru.nsu.raft.runtime.EventQueue;

@Component
public class PeerClient implements PeerTransport {
    private static final Logger log = LoggerFactory.getLogger(PeerClient.class);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final RestClient client;
    private final EventQueue queue;

    public PeerClient(RestClient peerRestClient, EventQueue queue) {
        this.queue = queue;
        client = peerRestClient;
    }
    @Override
    public void requestVote(Peer peer, RequestVoteRequest request) {
        send(() -> {
            var response = client.post().uri(peer.url() + "/raft/request-vote")
                    .body(request).retrieve().body(RequestVoteResponse.class);
            if (response != null) queue.post(new VoteResponseReceived(peer.id(), response));
        }, peer);
    }
    @Override
    public void appendEntries(Peer peer, long sequence, AppendEntriesRequest request) {
        send(() -> {
            var response = client.post().uri(peer.url() + "/raft/append-entries")
                    .body(request).retrieve().body(AppendEntriesResponse.class);
            if (response != null) queue.post(new AppendEntriesResponseReceived(peer.id(), sequence, request, response));
        }, peer);
    }
    private void send(Runnable action, Peer peer) {
        executor.submit(() -> {
            try { action.run(); }
            catch (Exception exception) { log.debug("RPC to {} failed: {}", peer.id(), exception.toString()); }
        });
    }
    @PreDestroy
    public void close() {
        executor.shutdownNow();
    }
}
