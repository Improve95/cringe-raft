package ru.nsu.raft.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.AppendEntriesReceived;
import ru.nsu.raft.node.event.RequestVoteReceived;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;
import ru.nsu.raft.runtime.EventQueue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/raft")
public class RaftRpcController {
    private final EventQueue queue;
    private final long timeoutMillis;

    public RaftRpcController(EventQueue queue, ClusterProperties properties) {
        this.queue = queue;
        timeoutMillis = properties.rpcTimeout().toMillis();
    }

    @PostMapping("/request-vote")
    @Operation(summary = "Запросить голос на выборах лидера")
    public CompletableFuture<RequestVoteResponse> requestVote(@RequestBody RequestVoteRequest request) {
        var reply = new CompletableFuture<RequestVoteResponse>().orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        queue.post(new RequestVoteReceived(request, reply));
        return reply;
    }

    @PostMapping("/append-entries")
    @Operation(summary = "Передать записи лога или heartbeat")
    public CompletableFuture<AppendEntriesResponse> appendEntries(@RequestBody AppendEntriesRequest request) {
        var reply = new CompletableFuture<AppendEntriesResponse>().orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        queue.post(new AppendEntriesReceived(request, reply));
        return reply;
    }
}
