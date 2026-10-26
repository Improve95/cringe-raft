package ru.nsu.raft.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import ru.nsu.raft.config.ClusterProperties.Peer;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.node.event.VoteResponseReceived;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;
import ru.nsu.raft.runtime.EventQueue;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class PeerClient {
    private static final Logger log = LoggerFactory.getLogger(PeerClient.class);
    private final RestClient client;
    private final ExecutorService executor;
    private final EventQueue queue;

    public PeerClient(RestClient client, ExecutorService executor, EventQueue queue) {
        this.client = client;
        this.executor = executor;
        this.queue = queue;
    }

    public void requestVote(Peer peer, RequestVoteRequest request) {
        send(
            peer, "/raft/request-vote", request, RequestVoteResponse.class,
            response -> queue.post(new VoteResponseReceived(peer.id(), response)));
    }

    public void appendEntries(Peer peer, long requestId, AppendEntriesRequest request) {
        send(
            peer, "/raft/append-entries", request, AppendEntriesResponse.class,
            response -> queue.post(new AppendEntriesResponseReceived(peer.id(), requestId, request, response)));
    }

    private <T> void send(Peer peer, String path, Object request, Class<T> responseType, Consumer<T> onResponse) {
        executor.execute(() -> {
            try {
                T response = client.post().uri(peer.url().resolve(path))
                    .contentType(MediaType.APPLICATION_JSON).body(request)
                    .retrieve().body(responseType);
                if (response != null) {
                    onResponse.accept(response);
                }
            } catch (RuntimeException error) {
                log.debug("RPC {} to {} failed: {}", path, peer.id(), error.getMessage());
            }
        });
    }
}
